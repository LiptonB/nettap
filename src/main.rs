use std::net::IpAddr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short = "l", long = "listen")]
    listen: bool,
    address: IpAddr,
    port: u16,
}

fn main() {
    let opt = Opt::from_args();
    println!("Hello, world! {:?}", opt);
}
