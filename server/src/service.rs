use bitflags::_core::time::Duration;
use std::thread;
use log::{error, info};
use std::error::Error;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::{fmt, fs};

use crate::{server, DEFAULT_PORT};

use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn start_server() -> crate::Result<()> {
    // enable logging
    // see https://docs.rs/tracing for more info
    // tracing_subscriber::fmt::try_init()?;

    // let cli = Cli::from_args();
    // let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);

    // Bind a TCP listener
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", DEFAULT_PORT)).await?;

    server::run(listener, signal::ctrl_c()).await
}
