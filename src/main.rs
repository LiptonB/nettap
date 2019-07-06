use failure::{bail, Error};
use quicli::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

mod connection;
mod coordinator;
use connection::Connection;
use coordinator::Coordinator;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "l", long = "listen")]
    listen: bool,
    #[structopt(min_values = 1, max_values = 2)]
    args: Vec<String>,
    #[structopt(flatten)]
    verbosity: Verbosity,
}

fn setup_stream<S: AsyncRead + AsyncWrite>(socket: S, conn: Connection) {}

// TODO: If args are "1 2" why does it succeed in making a SocketAddr?
fn connect(addr: &SocketAddr, conn: Connection) {
    let stream = TcpStream::connect(addr)
        .and_then(|stream| {
            setup_stream(stream, conn);
            Ok(())
        })
        .map_err(|err| {
            println!("Connection error = {:?}", err);
        });

    tokio::run(stream);
}

fn listen(addr: &SocketAddr, conn: Connection) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    tokio::spawn(
        listener
            .incoming()
            .map_err(|e| eprintln!("failed to accept socket; error = {:?}", e))
            .for_each(move |socket| {
                let conn = conn.clone();
                setup_stream(socket, conn);
                Ok(())
            }),
    );
    Ok(())
}

fn parse_options() -> Result<(bool, SocketAddr)> {
    let opt = Opt::from_args();
    opt.verbosity.setup_env_logger(env!("CARGO_PKG_NAME"))?;

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

fn main() -> CliResult {
    let (listen_mode, addr) = parse_options()?;
    tokio::run(future::lazy(move || {
        let (coordinator, connection, add_connection) = Coordinator::new();

        if listen_mode {
            listen(&addr, connection.clone());
        } else {
            connect(&addr, connection.clone());
        }

        future::ok(())
    }));
    Ok(())
}
