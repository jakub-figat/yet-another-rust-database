use crate::connection::{Connection, ConnectionError, ConnectionInner};
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;

pub struct ConnectionPool {
    connections: Mutex<VecDeque<Connection>>,
    acquire_timeout: Duration,
    pub semaphore: Semaphore,
}

impl ConnectionPool {
    pub async fn new(
        addr: SocketAddrV4,
        pool_size: usize,
        timeout: Duration,
    ) -> Result<Arc<ConnectionPool>, ConnectionError> {
        let pool = Arc::new(ConnectionPool {
            connections: Mutex::new(VecDeque::with_capacity(pool_size)),
            acquire_timeout: timeout,
            semaphore: Semaphore::new(pool_size),
        });

        let mut join_set = JoinSet::new();
        for _ in 0..pool_size {
            join_set.spawn(ConnectionInner::new(addr));
        }

        let mut connections = VecDeque::with_capacity(pool_size);
        while let Some(Ok(session)) = join_set.join_next().await {
            connections.push_back(session?);
        }

        {
            let mut pool_connections = pool.connections.lock().unwrap();
            for connection in connections {
                pool_connections
                    .push_back(Connection::new_for_pool(connection, Arc::downgrade(&pool)));
            }
        }

        Ok(pool)
    }

    pub async fn acquire(&self) -> Result<Connection, String> {
        let permit = tokio::select! {
            _ = sleep(self.acquire_timeout) => Err("Connection pool acquire timeout expired".to_string()),
            permit = self.semaphore.acquire() => {
                permit.map_err(|e| e.to_string())
            }
        }?;

        let mut connections = self.connections.lock().unwrap();
        let connection = connections.pop_front().unwrap();

        permit.forget();
        Ok(connection)
    }

    pub fn put_back(self: &Arc<Self>, connection_inner: ConnectionInner) {
        let mut connections = self.connections.lock().unwrap();
        connections.push_back(Connection::new_for_pool(
            connection_inner,
            Arc::downgrade(self),
        ));
        self.semaphore.add_permits(1);
    }
}
