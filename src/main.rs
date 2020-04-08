use clap_verbosity_flag::Verbosity;
use failure::{bail, Error};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use structopt::StructOpt;
use tokio::net::{TcpListener, TcpStream};

mod connection;
mod coordinator;
use connection::tokio_connection;
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

/*
fn setup_stream<S>(
    socket: S,
    data_sender: mpsc::UnboundedSender<Bytes>,
    data_receiver: mpsc::UnboundedReceiver<Bytes>,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    // TODO: Can't we make the function accept these the way they are?
    let data_sender = data_sender.sink_from_err();
    let data_receiver = data_receiver.map_err(|nothing: ()| unreachable!());
    tokio::spawn(tokio_connection(data_sender, data_receiver, socket));
}
*/

// TODO: If args are "1 2" why does it succeed in making a SocketAddr?
async fn connect(addr: &SocketAddr, coordinator: &mut Coordinator) -> Result<()> {
    let stream = TcpStream::connect(addr).await?;
    coordinator.add_connection(tokio_connection::new_tokio_connection(stream));
    Ok(())
}

async fn listen(addr: &SocketAddr, coordinator: &mut Coordinator) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    coordinator.add_connection(tokio_connection::new_spawner_connection(listener));
    Ok(())
}

/*
fn listen(addr: &SocketAddr, data_sender: mpsc::UnboundedSender<Bytes>) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    tokio::spawn(
        listener
            .incoming()
            .map_err(|e| eprintln!("failed to accept socket; error = {:?}", e))
            .for_each(move |socket| {
                let data_sender = data_sender.clone();
                let (sender, receiver) = mpsc::unbounded();
                setup_stream(socket, data_sender, receiver);
                Ok(())
            }),
    );
    Ok(())
}
*/

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

async fn run_main() -> Result<()> {
    let (listen_mode, addr) = parse_options()?;
    let mut coordinator = Coordinator::new();

    if listen_mode {
        listen(&addr, &mut coordinator).await?;
    } else {
        connect(&addr, &mut coordinator).await?;
    }

    tokio::spawn(coordinator.run());
    Ok(())
}

#[tokio::main]
async fn main() {
    run_main().await.expect("Error in main");
}
