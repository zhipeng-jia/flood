use std::fmt::{self, Write};
use std::iter;

use bytes::{BufMut, Bytes, BytesMut};
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

impl Generator {
    pub fn new(host: &str) -> Generator {
        let js_context = quick_js::Context::new().unwrap();
        js_context.eval(JS_LIB_CODE).unwrap();
        Self {
            host: String::from(host),
            js_context: js_context,
        }
    }

    pub fn load_user_script(&mut self, user_script: &str) -> Result<()> {
        if let Err(js_err) = self.js_context.eval(user_script) {
            return Err(Error::JsExecError(js_err));
        }
        // Test user script
        if let Err(err) = Generator::new_request("test.com", &self.js_context) {
            return Err(err);
        }
        Ok(())
    }

    pub fn new_request(host: &str, js_context: &quick_js::Context) -> Result<Bytes> {
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
        Generator::new_request(&self.host, &self.js_context).unwrap()
    }
}
