use monoio::io::AsyncWriteRentExt;
use monoio::net::{TcpListener, TcpStream};

#[monoio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:8000").unwrap();
    while let Ok((stream, _)) = listener.accept().await {
        monoio::spawn(handle(stream));
    }
}

async fn handle(mut stream: TcpStream) {
    println!("come");
    let _ = stream.write_all("hello guy").await;
}

