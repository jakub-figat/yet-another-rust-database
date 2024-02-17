use monoio::io::AsyncWriteRentExt;
use monoio::net::{TcpListener, TcpStream};
use routing::ThreadRouter;
use std::sync::Arc;
use std::thread::available_parallelism;

pub async fn start_tcp_server() {
    let num_of_threads = available_parallelism().unwrap().get() - 1;
    let thread_router = Arc::new(ThreadRouter::new(num_of_threads));
    let tcp_stream = TcpListener::bind("0.0.0.0:29876").unwrap();

    if let Ok((stream, _)) = tcp_stream.accept().await {
        monoio::spawn(handle_connection(stream, thread_router.clone()));
    }
}

async fn handle_connection(mut tcp_stream: TcpStream, thread_router: Arc<ThreadRouter>) {
    let (result, _buffer) = tcp_stream.write_all("gitara").await;
    result.unwrap();
}
