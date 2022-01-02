use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::time::Instant;

use hdrhistogram::Histogram;
use log::*;
use rand::Rng;
use zstd;

pub struct ExecutionInfo {
    initial_time: Instant,
    traces: Vec<(u32, u32, u32)>,
    trace_sample_ratio: f32,
    pub latency_hist: Histogram<u32>,
    pub bytes_sent: usize,
    pub bytes_recv: usize,
    pub request_total: u32,
    pub success_count: u32,    // 200
    pub failure_count: u32,    // non-200
    pub conn_error_count: u32, // other errors
    pub parse_error_count: u32,
}

impl ExecutionInfo {
    pub fn new(hist_max: u64, trace_size: usize, trace_sample_ratio: f32) -> ExecutionInfo {
        Self {
            initial_time: Instant::now(),
            traces: Vec::<(u32, u32, u32)>::with_capacity(trace_size),
            trace_sample_ratio: trace_sample_ratio,
            latency_hist: Histogram::<u32>::new_with_max(hist_max, 3).unwrap(),
            bytes_sent: 0,
            bytes_recv: 0,
            request_total: 0,
            success_count: 0,
            failure_count: 0,
            conn_error_count: 0,
            parse_error_count: 0,
        }
    }

    pub fn set_initial_time(&mut self, t: Instant) {
        self.initial_time = t;
    }

    pub fn inc_bytes_send(&mut self, delta: usize) {
        self.bytes_sent += delta;
    }

    pub fn inc_bytes_recv(&mut self, delta: usize) {
        self.bytes_recv += delta;
    }

    pub fn new_request(&mut self, start_time: Instant) {
        if start_time >= self.initial_time {
            self.request_total += 1;
        }
    }

    fn record_request(&mut self, req_type: u32, start_time: Instant, finish_time: Instant) {
        let latency: u64 = finish_time.duration_since(start_time).as_micros() as u64;
        if !self.latency_hist.record(latency).is_ok() {
            warn!("Failed to record latency: {}", latency);
        }
        if self.trace_sample_ratio > 0.0 {
            let start_timestamp = start_time.duration_since(self.initial_time).as_micros() as u32;
            let finish_timestamp = finish_time.duration_since(self.initial_time).as_micros() as u32;
            if rand::thread_rng().gen_range(0.0..1.0) < self.trace_sample_ratio {
                self.traces
                    .push((req_type, start_timestamp, finish_timestamp));
            }
        }
    }

    pub fn request_finished(&mut self, req_type: u32, start_time: Instant, finish_time: Instant) {
        if start_time < self.initial_time {
            return;
        }
        self.success_count += 1;
        self.record_request(req_type, start_time, finish_time);
    }

    pub fn request_failed(&mut self, req_type: u32, start_time: Instant, finish_time: Instant) {
        if start_time < self.initial_time {
            return;
        }
        self.failure_count += 1;
        self.record_request(req_type, start_time, finish_time);
    }

    pub fn connection_error(&mut self) {
        if Instant::now() >= self.initial_time {
            self.conn_error_count += 1;
        }
    }

    pub fn parse_error(&mut self) {
        if Instant::now() >= self.initial_time {
            self.parse_error_count += 1;
        }
    }

    pub fn save_trace(&self, save_path: &str) -> io::Result<()> {
        let f = File::create(save_path)?;
        let mut encoder = zstd::stream::Encoder::new(
            BufWriter::with_capacity(1024 * 1024 * 16, f),
            /* level= */ 0,
        )
        .unwrap();

        write!(&mut encoder, "[").unwrap();
        let mut first = true;
        for &trace in self.traces.iter() {
            if first {
                write!(
                    &mut encoder,
                    "{{\"type\":{},\"start\":{},\"finish\":{}}}",
                    trace.0, trace.1, trace.2
                )
                .unwrap();
                first = false;
            } else {
                write!(
                    &mut encoder,
                    ",{{\"type\":{},\"start\":{},\"finish\":{}}}",
                    trace.0, trace.1, trace.2
                )
                .unwrap();
            }
        }
        write!(&mut encoder, "]").unwrap();

        let mut w = encoder.finish()?;
        w.flush()
    }
}
