use crate::Session;
use std::collections::VecDeque;
use std::net::SocketAddrV4;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

pub struct SessionPool {
    addr: SocketAddrV4,
    sessions: Mutex<VecDeque<Session>>,
    semaphore: Arc<Semaphore>,
}

impl SessionPool {
    pub async fn new(addr: SocketAddrV4, pool_size: usize) -> Result<SessionPool, String> {
        let mut join_set = JoinSet::new();
        for _ in 0..pool_size {
            join_set.spawn(Session::new(addr));
        }

        let mut sessions = VecDeque::with_capacity(pool_size);
        while let Some(Ok(session)) = join_set.join_next().await {
            sessions.push_back(session?);
        }

        Ok(SessionPool {
            addr,
            sessions: Mutex::new(sessions),
            semaphore: Arc::new(Semaphore::new(pool_size)),
        })
    }

    pub async fn acquire(&self) -> Session {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        let mut sessions = self.sessions.lock().await;
        let mut session = sessions.pop_front().unwrap();
        session.semaphore_permit = Some(permit);

        session
    }

    pub async fn put_back(&self, mut session: Session) {
        let permit = session.semaphore_permit.take().unwrap();
        let mut sessions = self.sessions.lock().await;
        sessions.push_back(session);
        drop(permit);
    }
}
