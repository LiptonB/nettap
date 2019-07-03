use failure::{bail, Error};
use quicli::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::exit;
use std::str::FromStr;
use structopt::StructOpt;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

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

// TODO: If args are "1 2" why does it succeed in making a SocketAddr?
fn connect(addr: &SocketAddr) {
    let stream = TcpStream::connect(addr)
        .and_then(|stream| {
            setup_stream(stream);
            Ok(())
        })
        .map_err(|err| {
            println!("Connection error = {:?}", err);
        });

    tokio::run(stream);
}

fn setup_stream<S: AsyncRead + AsyncWrite>(socket: S) {}

fn listen(addr: &SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    tokio::spawn(
        listener
            .incoming()
            .map_err(|e| eprintln!("failed to accept socket; error = {:?}", e))
            .for_each(|socket| {
                setup_stream(socket);
                Ok(())
            }),
    );
    Ok(())
}

fn parse_options() -> Result<(bool, SocketAddr)> {
    let opt = Opt::from_args();

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

fn coordinator() -> impl Future<Item = (), Error = ()> {
    future::ok(())
}

fn main() -> CliResult {
    let (listen_mode, addr) = parse_options()?;
    tokio::run(future::lazy(move || {
        if listen_mode {
            listen(&addr);
        } else {
            connect(&addr);
        }

        coordinator()
    }));
    Ok(())
}
