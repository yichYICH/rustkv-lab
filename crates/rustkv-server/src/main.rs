use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "rustkv-server")]
#[command(about = "Tokio-powered rustkv server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,
    #[arg(long)]
    aof: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    rustkv_server::server::run(&args.addr, args.aof).await
}
