use std::fmt::Write;
use std::iter;

use bytes::{BufMut, Bytes, BytesMut};
use quick_js::{self, JsValue};

static JS_LIB_CODE: &'static str = include_str!("lib.js");

pub struct Generator {
    host: String,
    js_context: quick_js::Context,
}

impl Generator {
    pub fn new(host: &str, user_script: &str) -> Generator {
        let js_context = quick_js::Context::new().unwrap();
        js_context.eval(JS_LIB_CODE).unwrap();
        js_context.eval(user_script).unwrap();
        Self {
            host: String::from(host),
            js_context: js_context,
        }
    }

    pub fn new_request(host: &str, js_context: &quick_js::Context) -> Bytes {
        let empty_args = iter::empty::<JsValue>();
        let request = match js_context.call_function("newRequest", empty_args).unwrap() {
            JsValue::Object(obj) => obj,
            _ => {
                panic!("newRequest must return an object");
            }
        };
        let mut data = BytesMut::with_capacity(256);
        write!(
            &mut data,
            "{} {} HTTP/1.1\r\n",
            request.get("method").unwrap().as_str().unwrap(),
            request.get("path").unwrap().as_str().unwrap()
        )
        .unwrap();
        write!(&mut data, "Host: {}\r\n", host).unwrap();
        write!(&mut data, "Connection: keep-alive\r\n").unwrap();

        let mut has_accept = false;
        let mut has_user_agent = false;
        let mut has_content_type = false;
        match request.get("headers").unwrap() {
            JsValue::Object(obj) => {
                for (key, value) in obj.iter() {
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
                    write!(&mut data, "{}: {}\r\n", key, value.as_str().unwrap()).unwrap();
                }
            }
            _ => {
                panic!("headers must return an object");
            }
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

        let body = request.get("body").unwrap().as_str().unwrap();
        write!(&mut data, "Content-Length: {}\r\n\r\n", body.len()).unwrap();
        data.put_slice(body.as_bytes());

        data.freeze()
    }

    pub fn get(&mut self) -> Bytes {
        Generator::new_request(&self.host, &self.js_context)
    }
}
