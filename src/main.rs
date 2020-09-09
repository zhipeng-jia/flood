mod client;
mod exec_info;
mod generator;

use client::Client;
use exec_info::ExecutionInfo;
use generator::Generator;

use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use env_logger;
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
    if hist.total_count() > 0 {
        print!("  Latency Distribution (HdrHistogram)\n");
        print!(
            " 50.000%  {}\n",
            format_latency(hist.percentile(0.5).unwrap())
        );
        print!(
            " 75.000%  {}\n",
            format_latency(hist.percentile(0.75).unwrap())
        );
        print!(
            " 90.000%  {}\n",
            format_latency(hist.percentile(0.90).unwrap())
        );
        print!(
            " 99.000%  {}\n",
            format_latency(hist.percentile(0.99).unwrap())
        );
        print!(
            " 99.900%  {}\n",
            format_latency(hist.percentile(0.999).unwrap())
        );
        print!(
            " 99.990%  {}\n",
            format_latency(hist.percentile(0.9999).unwrap())
        );
        print!(
            " 99.999%  {}\n",
            format_latency(hist.percentile(0.99999).unwrap())
        );
        print!(
            "100.000%  {}\n",
            format_latency(hist.percentile(1.0).unwrap())
        );
        print!("\n");
        print!("  Detailed Percentile spectrum:\n");
        print!("       Value   Percentile   TotalCount 1/(1-Percentile)\n");
        print!("\n");
        let mut accum_count = 0;
        let total_count = hist.total_count();
        for bucket in hist {
            let count = bucket.count();
            if count == 0 {
                continue;
            }
            accum_count += count;
            let percentile = accum_count as f32 / total_count as f32;
            print!(
                "  {:>10.3}  {:>10.6}  {:>10}  {:>10.2}\n",
                bucket.value() as f32 / 1000.0,
                percentile,
                accum_count,
                1.0 / (1.0 - percentile)
            );
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
    env_logger::init();
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
    client.set_read_timeout(humantime::parse_duration(&opt.read_timeout)?);
    client.set_write_timeout(humantime::parse_duration(&opt.write_timeout)?);
    client.set_arrival_process(&opt.arrival_process);

    let exec_info = client.run(opt.num_conn, opt.qps, warmup_duration, duration)?;
    print_results(&opt, duration, &exec_info);

    Ok(())
}
