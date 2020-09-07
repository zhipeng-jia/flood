use std::collections::VecDeque;
use std::fmt::{self, Write};
use std::iter;
use std::sync::{atomic, Arc, Condvar, Mutex};
use std::thread;

use bytes::{BufMut, Bytes, BytesMut};
use log::*;
use quick_js::{self, JsValue};

static JS_LIB_CODE: &'static str = include_str!("lib.js");

#[derive(Debug)]
pub enum Error {
    JsExecError(quick_js::ExecutionError),
    InvalidScript(String),
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::JsExecError(js_err) => write!(f, "JsExecutionError: {}", js_err),
            Error::InvalidScript(msg) => write!(f, "Invalid user script: {}", msg),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub struct Generator {
    host: String,
    num_threads: usize,
    max_qsize: usize,
    thread_control: Arc<atomic::AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
    queue_mu: Arc<Mutex<VecDeque<Bytes>>>,
    queue_cond: Arc<Condvar>,
    active_thread: Arc<atomic::AtomicUsize>,
    js_context: quick_js::Context,
}

macro_rules! expect_js_str {
    ($value:expr, $msg:expr) => {
        match $value {
            JsValue::String(s) => s,
            _ => {
                return Err(Error::InvalidScript($msg.to_string()));
            }
        }
    };
}

macro_rules! expect_js_obj {
    ($value:expr, $msg:expr) => {
        match $value {
            JsValue::Object(obj) => obj,
            _ => {
                return Err(Error::InvalidScript($msg.to_string()));
            }
        }
    };
}

impl Drop for Generator {
    fn drop(&mut self) {
        self.thread_control.store(false, atomic::Ordering::SeqCst);
        while let Some(thread) = self.threads.pop() {
            thread.join().unwrap();
        }
    }
}

impl Generator {
    pub fn new(host: &str, num_threads: usize, max_qsize: usize) -> Generator {
        let js_context = quick_js::Context::new().unwrap();
        js_context.eval(JS_LIB_CODE).unwrap();
        Self {
            host: String::from(host),
            num_threads: num_threads,
            max_qsize: max_qsize,
            thread_control: Arc::new(atomic::AtomicBool::new(false)),
            threads: Vec::<thread::JoinHandle<()>>::with_capacity(num_threads),
            queue_mu: Arc::new(Mutex::new(VecDeque::<Bytes>::with_capacity(max_qsize))),
            queue_cond: Arc::new(Condvar::new()),
            active_thread: Arc::new(atomic::AtomicUsize::new(0)),
            js_context: js_context,
        }
    }

    fn test_user_script(&self, user_script: &str) -> Result<()> {
        if let Err(js_err) = self.js_context.eval(user_script) {
            return Err(Error::JsExecError(js_err));
        }
        if let Err(err) = Generator::new_request("test.com", &self.js_context) {
            return Err(err);
        }
        Ok(())
    }

    pub fn load_user_script(&mut self, user_script: &str) -> Result<()> {
        self.test_user_script(user_script)?;
        self.thread_control.store(true, atomic::Ordering::SeqCst);
        for i in 0..self.num_threads {
            let control = self.thread_control.clone();
            let queue_mu = self.queue_mu.clone();
            let queue_cond = self.queue_cond.clone();
            let max_qsize = self.max_qsize;
            let active_thread = self.active_thread.clone();
            let user_script = String::from(user_script);
            let host = self.host.clone();
            let thread = thread::spawn(move || {
                active_thread.fetch_add(1, atomic::Ordering::SeqCst);
                info!("{}-th JS thread starts", i);
                let js_context = quick_js::Context::new().unwrap();
                js_context.eval(JS_LIB_CODE).unwrap();
                js_context.eval(&user_script).unwrap();
                while control.load(atomic::Ordering::SeqCst) {
                    let data = Generator::new_request(&host, &js_context).unwrap();
                    let mut queue = queue_mu.lock().unwrap();
                    while (*queue).len() >= max_qsize {
                        active_thread.fetch_sub(1, atomic::Ordering::SeqCst);
                        queue = queue_cond.wait(queue).unwrap();
                        active_thread.fetch_add(1, atomic::Ordering::SeqCst);
                    }
                    assert!((*queue).len() < max_qsize);
                    (*queue).push_back(data);
                }
            });
            self.threads.push(thread);
        }
        Ok(())
    }

    fn new_request(host: &str, js_context: &quick_js::Context) -> Result<Bytes> {
        let empty_args = iter::empty::<JsValue>();
        let request = match js_context.call_function("newRequest", empty_args) {
            Ok(value) => expect_js_obj!(value, "newRequest must return an object"),
            Err(js_err) => {
                return Err(Error::JsExecError(js_err));
            }
        };
        for key in ["method", "path", "headers", "body"].iter() {
            if !request.contains_key(*key) {
                return Err(Error::InvalidScript(format!(
                    "Returned object must contain `{}`",
                    key
                )));
            }
        }
        let mut data = BytesMut::with_capacity(256);
        write!(
            &mut data,
            "{} {} HTTP/1.1\r\n",
            expect_js_str!(request.get("method").unwrap(), "`method` must be a string"),
            expect_js_str!(request.get("path").unwrap(), "`path` must be a string")
        )
        .unwrap();
        write!(&mut data, "Host: {}\r\n", host).unwrap();
        write!(&mut data, "Connection: keep-alive\r\n").unwrap();

        let mut has_accept = false;
        let mut has_user_agent = false;
        let mut has_content_type = false;
        let headers = expect_js_obj!(
            request.get("headers").unwrap(),
            "`headers` must be an object"
        );
        for (key, value) in headers.iter() {
            if key == "Host" || key == "Connection" || key == "Content-Length" {
                continue;
            }
            if key == "Accept" {
                has_accept = true;
            }
            if key == "User-Agent" {
                has_user_agent = true;
            }
            if key == "Content-Type" {
                has_content_type = true;
            }
            let value_str = expect_js_str!(value, "header value must be a string");
            write!(&mut data, "{}: {}\r\n", key, value_str).unwrap();
        }

        if !has_accept {
            write!(&mut data, "Accept: */*\r\n").unwrap();
        }
        if !has_user_agent {
            write!(&mut data, "User-Agent: flood\r\n").unwrap();
        }
        if !has_content_type {
            write!(&mut data, "Content-Type: text/plain\r\n").unwrap();
        }

        let body = expect_js_str!(request.get("body").unwrap(), "`body` must be a string");
        write!(&mut data, "Content-Length: {}\r\n\r\n", body.len()).unwrap();
        data.put_slice(body.as_bytes());

        Ok(data.freeze())
    }

    pub fn get(&mut self) -> Bytes {
        {
            let mut queue = self.queue_mu.lock().unwrap();
            if let Some(data) = (*queue).pop_front() {
                if (*queue).len() < self.max_qsize / 2
                    || self.active_thread.load(atomic::Ordering::SeqCst) == 0
                {
                    self.queue_cond.notify_one();
                }
                return data;
            } else {
                self.queue_cond.notify_all();
            }
        }
        warn!("JS threads failed to generate enough request data");
        Generator::new_request(&self.host, &self.js_context).unwrap()
    }
}
