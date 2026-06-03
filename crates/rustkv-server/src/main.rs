use std::time::Duration;

use clap::Parser;
use rustkv_core::db::DEFAULT_SHARD_COUNT;
use rustkv_server::server::{
    ServerConfig, DEFAULT_ADDR, DEFAULT_MAX_FRAME_SIZE, DEFAULT_TTL_INTERVAL_MS,
};

#[derive(Debug, Parser)]
#[command(name = "rustkv-server")]
#[command(about = "Tokio-powered rustkv server")]
struct Args {
    #[arg(long, default_value = DEFAULT_ADDR)]
    addr: String,
    #[arg(long)]
    aof: Option<String>,
    #[arg(long, default_value_t = DEFAULT_MAX_FRAME_SIZE)]
    max_frame_size: usize,
    #[arg(long, default_value_t = DEFAULT_TTL_INTERVAL_MS)]
    ttl_interval_ms: u64,
    #[arg(long, default_value_t = DEFAULT_SHARD_COUNT)]
    shards: usize,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config = ServerConfig {
        addr: args.addr,
        aof_path: args.aof,
        max_frame_size: args.max_frame_size,
        ttl_interval: Duration::from_millis(args.ttl_interval_ms),
        shard_count: args.shards,
    };

    rustkv_server::server::run(config).await
}
