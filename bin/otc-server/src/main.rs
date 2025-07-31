use clap::Parser;
use common::init_logger;
use otc_server::{server::run_server,  OtcServerArgs, Result};


#[tokio::main]
async fn main() -> Result<()> {
    let args = OtcServerArgs::parse();
    
    init_logger(&args.log_level).expect("Logger should initialize");
    
    run_server(args).await
}