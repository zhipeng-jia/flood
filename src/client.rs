use crate::exec_info::ExecutionInfo;
use crate::generator::{Generator, Request};

use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};

use bytes::{buf::BufMut, BytesMut};
use httparse;
use libc;
use log::*;
use mio::{unix::SourceFd, Events, Interest, Poll, Registry, Token};
use rand::Rng;
use timerfd::{SetTimeFlags, TimerFd, TimerState};

#[derive(PartialEq, Clone, Copy)]
enum ConnectionState {
    Idle,
    Sending,
    Receiving,
}

struct Connection {
    state: ConnectionState,
    stream: mio::net::TcpStream,
    token: Token,
    req_start_time: Option<Instant>,
    req: Option<Request>,
    req_write_pos: usize,
    resp_buf: BytesMut,
}

enum ArrivalProcess {
    Uniform,
    Poisson,
}

pub struct Client {
    addr: SocketAddr,
    generator: Generator,
    arrival_process: ArrivalProcess,
    ev_loop: Poll,
    next_token_id: usize,
    connect_timeout: Duration,
    read_timeout: Duration,
    write_timeout: Duration,
    connections: HashMap<Token, Connection>,
    idle_connections: VecDeque<Token>,
}

impl Connection {
    pub fn new(
        addr: &SocketAddr,
        token: Token,
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> io::Result<Connection> {
        let stream = std::net::TcpStream::connect_timeout(addr, connect_timeout)?;
        stream.set_nonblocking(true)?;
        stream.set_read_timeout(Some(read_timeout))?;
        stream.set_write_timeout(Some(write_timeout))?;
        let mio_stream = mio::net::TcpStream::from_std(stream);
        Ok(Self {
            state: ConnectionState::Idle,
            stream: mio_stream,
            token: token,
            req_start_time: None,
            req: None,
            req_write_pos: 0,
            resp_buf: BytesMut::with_capacity(4096),
        })
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn register(&mut self, registry: &Registry, interests: Interest) -> io::Result<()> {
        registry.register(&mut self.stream, self.token, interests)
    }

    pub fn reregister(&mut self, registry: &Registry, interests: Interest) -> io::Result<()> {
        registry.reregister(&mut self.stream, self.token, interests)
    }

    pub fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        registry.deregister(&mut self.stream)
    }

    pub fn state_transition(&mut self, registry: Option<&mio::Registry>) -> io::Result<()> {
        match self.state {
            ConnectionState::Idle => {
                self.state = ConnectionState::Sending;
                self.req_write_pos = 0;
                self.req_start_time = Some(Instant::now());
                Ok(())
            }
            ConnectionState::Sending => {
                self.state = ConnectionState::Receiving;
                self.reregister(registry.unwrap(), Interest::READABLE)
            }
            ConnectionState::Receiving => {
                self.state = ConnectionState::Idle;
                self.req = None;
                self.resp_buf.clear();
                self.req_start_time = None;
                self.reregister(registry.unwrap(), Interest::WRITABLE)
            }
        }
    }

    pub fn do_request(
        &mut self,
        generator: &mut Generator,
        exec_info: &mut ExecutionInfo,
    ) -> io::Result<bool> {
        assert!(self.state == ConnectionState::Idle);
        self.req = Some(generator.get());
        self.state_transition(None)?;
        exec_info.new_request(self.req_start_time.unwrap());
        self.write_request(exec_info)
    }

    pub fn write_request(&mut self, exec_info: &mut ExecutionInfo) -> io::Result<bool> {
        assert!(self.state == ConnectionState::Sending);
        let data = &self.req.as_ref().unwrap().input;
        assert!(self.req_write_pos < data.len());
        loop {
            match self.stream.write(data.slice(self.req_write_pos..).as_ref()) {
                Ok(nwrite) => {
                    self.req_write_pos += nwrite;
                    exec_info.inc_bytes_send(nwrite);
                    break;
                }
                Err(err) => {
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    } else {
                        exec_info.connection_error();
                        return Err(err);
                    }
                }
            }
        }
        Ok(self.req_write_pos == data.len())
    }

    pub fn recv_response(&mut self, exec_info: &mut ExecutionInfo) -> io::Result<bool> {
        assert!(self.state == ConnectionState::Receiving);
        let mut buf = [0; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(nread) => {
                    exec_info.inc_bytes_recv(nread);
                    while self.resp_buf.remaining_mut() < nread {
                        self.resp_buf.reserve(self.resp_buf.len());
                    }
                    self.resp_buf.put_slice(&buf[0..nread]);
                    if nread < buf.len() {
                        break;
                    }
                }
                Err(err) => {
                    let errno = err.raw_os_error().expect("No errno, WTF??");
                    if errno == libc::EAGAIN {
                        break;
                    } else if errno == libc::EINTR {
                        continue;
                    } else {
                        exec_info.connection_error();
                        return Err(err);
                    }
                }
            }
        }

        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Response::new(&mut headers);
        match req.parse(&self.resp_buf[..]) {
            Ok(result) => {
                if result.is_partial() {
                    return Ok(false);
                }
            }
            Err(err) => {
                exec_info.parse_error();
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    format!("HTTP parsing failed: {}", err),
                ));
            }
        }

        let req_type = self.req.as_ref().unwrap().req_type;
        if req.code.unwrap() == 200 {
            exec_info.request_finished(req_type, self.req_start_time.unwrap(), Instant::now());
        } else {
            exec_info.request_failed(req_type, self.req_start_time.unwrap(), Instant::now());
        }
        Ok(true)
    }
}

impl Client {
    pub fn new(addr: &SocketAddr, generator: Generator) -> Client {
        Self {
            addr: addr.clone(),
            generator: generator,
            arrival_process: ArrivalProcess::Uniform,
            ev_loop: Poll::new().expect("Failed to create event loop"),
            next_token_id: 0,
            connect_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            connections: HashMap::<Token, Connection>::new(),
            idle_connections: VecDeque::<Token>::with_capacity(128),
        }
    }

    pub fn set_connect_timeout(&mut self, d: Duration) {
        self.connect_timeout = d;
    }

    pub fn set_read_timeout(&mut self, d: Duration) {
        self.read_timeout = d;
    }

    pub fn set_write_timeout(&mut self, d: Duration) {
        self.write_timeout = d;
    }

    pub fn set_arrival_process(&mut self, s: &str) {
        if s == "uniform" {
            self.arrival_process = ArrivalProcess::Uniform;
        } else if s == "poisson" {
            self.arrival_process = ArrivalProcess::Poisson;
        } else {
            panic!("Unknown arrival process: {}", s);
        }
    }

    fn next_mio_token(&mut self) -> Token {
        let token = Token(self.next_token_id);
        self.next_token_id += 1;
        token
    }

    fn create_connection(&mut self) -> std::io::Result<()> {
        let token = self.next_mio_token();
        let mut connection = Connection::new(
            &self.addr,
            token,
            self.connect_timeout,
            self.read_timeout,
            self.write_timeout,
        )?;
        connection.register(self.ev_loop.registry(), Interest::WRITABLE)?;
        self.connections.insert(token, connection);
        info!(
            "Create new connection, total number is {}",
            self.connections.len()
        );
        Ok(())
    }

    fn connection_failed(&mut self, token: Token) -> std::io::Result<()> {
        let connection = self.connections.get_mut(&token).unwrap();
        connection.deregister(self.ev_loop.registry())?;
        self.connections.remove(&token);
        self.create_connection()
    }

    pub fn run(
        &mut self,
        exec_info: &mut ExecutionInfo,
        num_connections: i32,
        qps: i32,
        warmup_duration: Duration,
        duration: Duration,
    ) -> std::io::Result<()> {
        for _ in 0..num_connections {
            self.create_connection()?;
        }

        let mut tfd = TimerFd::new()?;
        match self.arrival_process {
            ArrivalProcess::Uniform => {
                tfd.set_state(
                    TimerState::Periodic {
                        current: Duration::from_millis(100),
                        interval: Duration::from_nanos(1_000_000_000 / (qps as u64)),
                    },
                    SetTimeFlags::Default,
                );
            }
            ArrivalProcess::Poisson => {
                tfd.set_state(
                    TimerState::Oneshot(Duration::from_millis(100)),
                    SetTimeFlags::Default,
                );
            }
        }

        let raw_fd = tfd.as_raw_fd();
        let mut sfd = SourceFd(&raw_fd);
        let timer_token = self.next_mio_token();
        self.ev_loop
            .registry()
            .register(&mut sfd, timer_token, Interest::READABLE)?;

        let now = Instant::now();
        let start_time = now + warmup_duration;
        exec_info.set_initial_time(start_time);
        let finish_time = start_time + duration;

        let mut events = Events::with_capacity(1024);

        while Instant::now() <= finish_time {
            match self
                .ev_loop
                .poll(&mut events, Some(Duration::from_millis(100)))
            {
                Ok(()) => {}
                Err(err) => match err.kind() {
                    ErrorKind::Interrupted => {
                        continue;
                    }
                    ErrorKind::TimedOut => {
                        continue;
                    }
                    _ => {
                        return Err(err);
                    }
                },
            }
            for event in &events {
                let token = event.token();
                if token == timer_token {
                    let tfd_value = tfd.read();
                    if tfd_value > 1 {
                        warn!("Missing {} timer expires", tfd_value - 1);
                    }
                    match self.arrival_process {
                        ArrivalProcess::Poisson => {
                            let x: f64 = rand::thread_rng().gen_range(0.0..1.0);
                            let interval = -x.ln() * 1e9 / (qps as f64);
                            let d = Duration::from_nanos(interval as u64);
                            tfd.set_state(TimerState::Oneshot(d), SetTimeFlags::Default);
                        }
                        _ => {}
                    }
                    let mut request_done = false;
                    while let Some(conn_token) = self.idle_connections.pop_front() {
                        if let Some(connection) = self.connections.get_mut(&conn_token) {
                            assert!(connection.state() == ConnectionState::Idle);
                            match connection.do_request(&mut self.generator, exec_info) {
                                Ok(_) => {
                                    connection.state_transition(Some(self.ev_loop.registry()))?;
                                }
                                Err(err) => {
                                    error!("Connection with {:?} failed: {}", conn_token, err);
                                    self.connection_failed(conn_token)?;
                                }
                            }
                            request_done = true;
                            break;
                        }
                    }
                    if !request_done && Instant::now() > start_time {
                        error!("Cannot find an idle connection.");
                    }
                } else if self.connections.contains_key(&token) {
                    let connection = self.connections.get_mut(&token).unwrap();
                    if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                        if Instant::now() > start_time {
                            if event.is_error() {
                                error!("Connection with {:?} has error", token);
                            } else if event.is_read_closed() {
                                error!("Connection with {:?} read closed", token);
                            } else if event.is_write_closed() {
                                error!("Connection with {:?} write closed", token);
                            }
                        }
                        exec_info.connection_error();
                        self.connection_failed(token)?;
                    } else if event.is_readable() {
                        match connection.state() {
                            ConnectionState::Receiving => {
                                match connection.recv_response(exec_info) {
                                    Ok(_) => {
                                        connection
                                            .state_transition(Some(self.ev_loop.registry()))?;
                                    }
                                    Err(err) => {
                                        error!("Connection with {:?} failed: {}", token, err);
                                        self.connection_failed(token)?;
                                    }
                                }
                            }
                            _ => {
                                panic!("Invalid ConnectionState for readable event");
                            }
                        }
                    } else if event.is_writable() {
                        match connection.state() {
                            ConnectionState::Idle => {
                                self.idle_connections.push_back(token);
                            }
                            ConnectionState::Sending => match connection.write_request(exec_info) {
                                Ok(_) => {
                                    connection.state_transition(Some(self.ev_loop.registry()))?;
                                }
                                Err(err) => {
                                    error!("Connection with {:?} failed: {}", token, err);
                                    self.connection_failed(token)?;
                                }
                            },
                            _ => {
                                panic!("Invalid ConnectionState for writable event");
                            }
                        }
                    }
                } else {
                    panic!("Unknown token");
                }
            }
        }

        Ok(())
    }
}
