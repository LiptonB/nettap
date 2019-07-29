use bytes::BytesMut;
use failure::{bail, Error};
use quicli::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use structopt::StructOpt;
use tokio::codec::{BytesCodec, FramedRead};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc;

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

fn setup_stream<S>(socket: S, data_sender: mpsc::Sender<BytesMut>)
where
    S: AsyncRead + AsyncWrite + Send,
{
    let (read, write) = socket.split();
    let stream = FramedRead::new(read, BytesCodec::new());
    tokio::spawn(stream.forward(data_sender));
}

// TODO: If args are "1 2" why does it succeed in making a SocketAddr?
fn connect(addr: &SocketAddr, data_sender: mpsc::Sender<BytesMut>) {
    let stream = TcpStream::connect(addr)
        .and_then(|stream| {
            setup_stream(stream, data_sender);
            Ok(())
        })
        .map_err(|err| {
            println!("Connection error = {:?}", err);
        });

    tokio::run(stream);
}

fn listen(addr: &SocketAddr, data_sender: mpsc::Sender<BytesMut>) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    tokio::spawn(
        listener
            .incoming()
            .map_err(|e| eprintln!("failed to accept socket; error = {:?}", e))
            .for_each(move |socket| {
                let data_sender = data_sender.clone();
                setup_stream(socket, data_sender);
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
        let (coordinator, data_sender, connection_sender) = Coordinator::new();

        if listen_mode {
            listen(&addr, data_sender);
        } else {
            connect(&addr, data_sender);
        }

        future::ok(())
    }));
    Ok(())
}
