use aerolineas_rusticas::{errors::error_types::ErrorTypes, server::nodes::Node};
use std::env;

fn main() -> Result<(), ErrorTypes> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: cargo run --bin node -- <INTERNAL_IP_ADDRESS> <CLIENT_IP_ADDRESS>");
        std::process::exit(1);
    }

    let ip_address_internal = &args[1];
    let ip_address_client = &args[2];
    let mut node = Node::new(ip_address_internal, ip_address_client).unwrap();
    node.run()
}
