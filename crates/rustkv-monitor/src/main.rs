use std::error::Error;

use clap::Parser;
use rustkv_monitor::app::EventLoop;

#[derive(Debug, Parser)]
#[command(name = "rustkv-monitor")]
#[command(about = "TUI monitor for rustkv")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    let (event_loop, rx) = EventLoop::new(args.addr);
    let event_task = tokio::spawn(event_loop.run());

    let result = rustkv_monitor::ui::run(rx).await;
    event_task.abort();

    result.map_err(Into::into)
}
