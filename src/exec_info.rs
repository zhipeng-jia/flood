use std::time::Instant;

use hdrhistogram::Histogram;
use log::*;

pub struct ExecutionInfo {
    initial_time: Instant,
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
    pub fn new(initial_time: Instant, hist_max: u64) -> ExecutionInfo {
        Self {
            initial_time: initial_time,
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

    pub fn request_finished(&mut self, start_time: Instant, finish_time: Instant) {
        if start_time < self.initial_time {
            return;
        }
        self.success_count += 1;
        let latency: u64 = finish_time.duration_since(start_time).as_micros() as u64;
        if !self.latency_hist.record(latency).is_ok() {
            warn!("Failed to record latency: {}", latency);
        }
    }

    pub fn request_failed(&mut self, start_time: Instant, finish_time: Instant) {
        if start_time < self.initial_time {
            return;
        }
        self.failure_count += 1;
        let latency: u64 = finish_time.duration_since(start_time).as_micros() as u64;
        if !self.latency_hist.record(latency).is_ok() {
            warn!("Failed to record latency: {}", latency);
        }
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
}
