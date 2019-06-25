use std::io::Result;
use std::net::{IpAddr, TcpStream, ToSocketAddrs};
use std::process::exit;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "l", long = "listen")]
    listen: bool,
    address: IpAddr,
    port: u16,
}

fn connect<A: ToSocketAddrs>(addr: A) -> Result<()> {
    let stream = TcpStream::connect(addr)?;
    Ok(())
}

fn listen<A: ToSocketAddrs>(addr: A) -> Result<()> {
    Ok(())
}

fn main() {
    let opt = Opt::from_args();

    let result = if opt.listen {
        listen((opt.address, opt.port))
    } else {
        connect((opt.address, opt.port))
    };

    match result {
        Err(_) => {
            println!("Error");
            exit(1);
        }
        _ => {}
    }
}
