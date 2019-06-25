use std::io::Result;
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::process::exit;
use std::str::FromStr;
use structopt::StructOpt;

// TODO: address could be a hostname as well
#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "l", long = "listen")]
    listen: bool,
    #[structopt(min_values = 1, max_values = 2)]
    args: Vec<String>,
}

fn connect<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let stream = TcpStream::connect(addr)?;
    Ok(())
}

fn listen<A: ToSocketAddrs>(addr: A) -> Result<()> {
    Ok(())
}

fn parse_options() -> Result<(bool, SocketAddr)> {
    let opt = Opt::from_args();

    let (address, port): (&str, &str) = if opt.listen {
        match opt.args.as_slice() {
            [port] => ("0.0.0.0", port),
            [address, port] => (address, port),
        }
    } else {
        match opt.args.as_slice() {
            [address, port] => (address, port),
        }
    };

    let address = IpAddr::from_str(address)?;
    let port = u16::from_str(port)?;
    return Ok((opt.listen, (address, port).into()));
}

fn main() {
    let result = match parse_options() {
        Ok((true, addr)) => listen(addr),
        Ok((false, addr)) => connect(addr),
        Err(_) => {
            println!("Usage");
            exit(1);
        }
    };

    if let Err(_) = result {
        println!("Error");
        exit(2);
    }
}
