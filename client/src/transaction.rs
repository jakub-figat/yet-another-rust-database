use crate::connection::{ConnectionError, ConnectionInner};
use crate::Model;
use common::value::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Transaction {
    id: u64,
    connection: Arc<Mutex<ConnectionInner>>,
    coordinator_partition: usize,
    finished: bool,
}

impl Transaction {
    pub(crate) fn new(
        id: u64,
        coordinator_partition: usize,
        connection: Arc<Mutex<ConnectionInner>>,
    ) -> Transaction {
        Transaction {
            id,
            connection,
            coordinator_partition,
            finished: false,
        }
    }

    pub async fn get_for_update<T: Model>(
        &self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, ConnectionError> {
        let connection = self.connection.lock().await;
        connection.get(hash_key, sort_key, Some(self.id)).await
    }

    pub async fn insert<T: Model>(&self, instance: T) -> Result<(), ConnectionError> {
        let connection = self.connection.lock().await;
        connection.insert(instance, Some(self.id)).await
    }

    pub async fn delete(
        &self,
        hash_key: String,
        sort_key: Value,
        table_name: &str,
    ) -> Result<bool, ConnectionError> {
        let connection = self.connection.lock().await;
        connection
            .delete(hash_key, sort_key, table_name, Some(self.id))
            .await
    }

    pub async fn commit(&mut self) -> Result<(), ConnectionError> {
        let connection = self.connection.lock().await;
        self.finished = true;
        connection
            .commit_transaction(self.id, self.coordinator_partition)
            .await
    }

    pub async fn abort(&mut self) -> Result<(), ConnectionError> {
        let connection = self.connection.lock().await;
        self.finished = true;
        connection
            .abort_transaction(self.id, self.coordinator_partition)
            .await
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            let connection = self.connection.clone();
            let transaction_id = self.id;
            let coordinator_partition = self.coordinator_partition;
            let _ = tokio::spawn(async move {
                let connection = connection.lock().await;
                connection
                    .abort_transaction(transaction_id, coordinator_partition)
                    .await
            });
        }
    }
}

// TODO: refactor protobuf code?
