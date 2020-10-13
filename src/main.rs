use anyhow::{bail, Result};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use structopt::StructOpt;
use tokio::{
    io, join,
    net::{TcpListener, TcpStream},
};
use tracing;
use tracing_subscriber;

mod connection;
mod coordinator;
use connection::tokio_connection;
use coordinator::Coordinator;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "l", long = "listen")]
    listen: bool,
    #[structopt(min_values = 1, max_values = 2)]
    args: Vec<String>,
}

// TODO: If args are "1 2" why does it succeed in making a SocketAddr?
async fn connect(addr: &SocketAddr, coordinator: &mut Coordinator) -> Result<()> {
    let stream = TcpStream::connect(addr).await?;
    let (read, write) = io::split(stream);
    tokio::spawn(coordinator.add_connection(tokio_connection::new_tokio_connection(read, write)));
    Ok(())
}

async fn listen(addr: &SocketAddr, coordinator: &mut Coordinator) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tokio::spawn(coordinator.add_connection(tokio_connection::new_spawner_connection(listener)));
    Ok(())
}

async fn start_console(coordinator: &mut Coordinator) -> Result<()> {
    let stdout = io::stdout();
    let stdin = io::stdin();
    tokio::spawn(coordinator.add_connection(tokio_connection::new_tokio_connection(stdin, stdout)));
    Ok(())
}

fn parse_options() -> Result<(bool, SocketAddr)> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt::init();

    let (address, port): (&str, &str) = if opt.listen {
        match opt.args.as_slice() {
            [port] => ("0.0.0.0", port),
            [address, port] => (address, port),
            _ => bail!("Invalid number of arguments"),
        }
    } else {
        match opt.args.as_slice() {
            [address, port] => (address, port),
            _ => bail!("Invalid number of arguments"),
        }
    };

    let port = u16::from_str(port)?;
    let addr = (address, port).to_socket_addrs()?.next().unwrap();
    return Ok((opt.listen, addr));
}

async fn run_main() -> Result<()> {
    let (listen_mode, addr) = parse_options()?;
    let mut coordinator = Coordinator::new();

    if listen_mode {
        listen(&addr, &mut coordinator).await?;
    } else {
        connect(&addr, &mut coordinator).await?;
    }
    start_console(&mut coordinator).await?;

    let (join_result,) = join!(tokio::spawn(coordinator.run()));
    join_result?;

    Ok(())
}

#[tokio::main]
async fn main() {
    run_main().await.expect("Error in main");
}
