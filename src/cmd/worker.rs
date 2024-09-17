use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Connect to a coordinator at the given IP address and port
    #[clap(short, long)]
    pub join: String
}
