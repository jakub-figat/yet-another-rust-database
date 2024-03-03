use std::net::Ipv4Addr;
use tokio::net::TcpStream;

pub struct SessionPool {
    address: Ipv4Addr,
    sessions: Vec<TcpStream>,
}

// impl SessionPool {
//     pub fn new(address: Ipv4Addr) -> SessionPool {
//
//     }
// }
