mod client;
mod exec_info;
mod generator;

use client::Client;
use exec_info::ExecutionInfo;
use generator::Generator;

use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use env_logger::{self, Env};
use humantime;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "flood")]
struct Opt {
    /// Host address
    #[structopt(short = "h", long = "host", default_value = "127.0.0.1:8080")]
    host: String,

    /// Fraction of warm-up duration w.r.t. full duration
    #[structopt(long = "warmup-fraction", default_value = "0.2")]
    warmup_fraction: f32,

    /// Running duration
    #[structopt(short = "d", long = "duration", default_value = "30s")]
    duration: String,

    /// QPS
    #[structopt(short = "r", long = "qps", default_value = "100")]
    qps: i32,

    /// Number of connections
    #[structopt(short = "c", long = "conn", default_value = "16")]
    num_conn: i32,

    /// Connect timeout
    #[structopt(long = "connect-timeout", default_value = "100ms")]
    connect_timeout: String,

    /// Connect timeout
    #[structopt(long = "read-timeout", default_value = "100ms")]
    read_timeout: String,

    /// Connect timeout
    #[structopt(long = "write-timeout", default_value = "100ms")]
    write_timeout: String,

    /// Arrival process (uniform or poisson)
    #[structopt(long = "arrival-process", default_value = "poisson")]
    arrival_process: String,

    /// Number of JS threads
    #[structopt(short = "t", long = "js-threads", default_value = "2")]
    num_js_threads: i32,

    /// Queue size for request generation
    #[structopt(long = "request-qsize", default_value = "128")]
    request_qsize: i32,

    /// Path for saving trace file
    #[structopt(short = "f", long = "trace-save-path", default_value = "")]
    trace_save_path: String,

    /// Sampling ratio for saved trace
    #[structopt(long = "trace-sample-ratio", default_value = "1.0")]
    trace_sample_ratio: f32,

    /// JavaScript file
    #[structopt(name = "SCRIPT")]
    js_script_path: String,
}

fn format_latency(micro: u64) -> String {
    if micro < 1000 {
        format!("{:>6.2}µs", micro as f64)
    } else if micro < 1_000_000 {
        format!("{:>6.2}ms", micro as f64 / 1000.0)
    } else if micro < 1_000_000_000 {
        format!("{:>6.2}s", micro as f64 / 1000000.0)
    } else {
        format!("{:>6.2}m", micro as f64 / 60000000.0)
    }
}

fn format_bytes(bytes: f64) -> String {
    if bytes < 1024.0 {
        format!("{:>6.2}B", bytes)
    } else if bytes < 1024.0 * 1024.0 {
        format!("{:>6.2}KB", bytes / 1024.0)
    } else if bytes < 1024.0 * 1024.0 * 1024.0 {
        format!("{:>6.2}MB", bytes / 1024.0 / 1024.0)
    } else {
        format!("{:>6.2}GB", bytes / 1024.0 / 1024.0 / 1024.0)
    }
}

fn print_results(opt: &Opt, duration: Duration, exec_info: &ExecutionInfo) {
    print!(
        "Running {} test @ http://{}\n",
        humantime::format_duration(duration),
        opt.host
    );
    print!("  {} connections\n", opt.num_conn);
    let hist = &exec_info.latency_hist;
    if !hist.is_empty() {
        print!("  Latency Distribution (HdrHistogram)\n");
        for &percentile in [50.0, 75.0, 90.0, 99.0, 99.9, 99.99, 99.999, 100.0].iter() {
            print!(
                "{:>7.3}%  {}\n",
                percentile,
                format_latency(hist.value_at_percentile(percentile))
            );
        }
        print!("\n");
        print!("  Detailed Percentile spectrum:\n");
        print!("       Value   Percentile   TotalCount 1/(1-Percentile)\n");
        print!("\n");
        for iter_value in hist.iter_quantiles(1) {
            if iter_value.count_since_last_iteration() > 0 {
                print!(
                    "  {:>10.3}  {:>10.6}  {:>10}  {:>10.2}\n",
                    iter_value.value_iterated_to() as f32 / 1000.0,
                    iter_value.percentile(),
                    iter_value.count_since_last_iteration(),
                    1.0 / (1.0 - iter_value.quantile())
                );
            }
        }
        print!("----------------------------------------------------------\n");
    }
    print!("\n");
    let total_requests = exec_info.success_count + exec_info.failure_count;
    print!(
        "  {} requests in {}, {} read\n",
        total_requests,
        humantime::format_duration(duration),
        format_bytes(exec_info.bytes_recv as f64)
    );
    if exec_info.failure_count > 0 {
        print!("  Non-2xx or 3xx responses: {}\n", exec_info.failure_count);
    }
    print!(
        "Requests/sec:{:>10.2}\n",
        total_requests as f32 / duration.as_secs_f32()
    );
    print!(
        "Transfer/sec:    {}\n",
        format_bytes(exec_info.bytes_sent as f64 / duration.as_secs_f64())
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    let opt = Opt::from_args();

    let mut resolved_addrs = opt.host.to_socket_addrs()?;
    let addr: SocketAddr = resolved_addrs.next().unwrap();
    let duration = humantime::parse_duration(&opt.duration)?;
    let warmup_duration = Duration::from_secs_f32(duration.as_secs_f32() * opt.warmup_fraction);
    let script_content =
        fs::read_to_string(&opt.js_script_path).expect("Failed to read script file");

    let mut generator = Generator::new(
        &opt.host,
        opt.num_js_threads as usize,
        opt.request_qsize as usize,
    );
    generator.load_user_script(&script_content)?;
    let mut client = Client::new(&addr, generator);

    client.set_connect_timeout(humantime::parse_duration(&opt.connect_timeout)?);
    let read_timeout = humantime::parse_duration(&opt.read_timeout)?;
    client.set_read_timeout(read_timeout);
    client.set_write_timeout(humantime::parse_duration(&opt.write_timeout)?);
    client.set_arrival_process(&opt.arrival_process);

    let mut exec_info = if !opt.trace_save_path.is_empty() {
        let estimated_trace_size =
            1.1 * opt.qps as f32 * duration.as_secs_f32() * opt.trace_sample_ratio;
        ExecutionInfo::new(
            read_timeout.as_micros() as u64,
            estimated_trace_size as usize,
            opt.trace_sample_ratio,
        )
    } else {
        ExecutionInfo::new(read_timeout.as_micros() as u64, 0, 0.0)
    };
    client.run(
        &mut exec_info,
        opt.num_conn,
        opt.qps,
        warmup_duration,
        duration,
    )?;
    print_results(&opt, duration, &exec_info);

    if !opt.trace_save_path.is_empty() {
        exec_info.save_trace(&opt.trace_save_path)?;
    }

    Ok(())
}
