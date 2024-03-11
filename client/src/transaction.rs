use crate::connection::ConnectionError;
use crate::{Connection, Model};
use common::value::Value;

pub struct Transaction {
    id: u64,
    coordinator_partition: usize,
    finished: bool,
}

impl Transaction {
    pub fn new(id: u64, coordinator_partition: usize) -> Transaction {
        Transaction {
            id,
            coordinator_partition,
            finished: false,
        }
    }

    pub async fn get_for_update<T: Model>(
        &mut self,
        hash_key: String,
        sort_key: Value,
        connection: &mut Connection,
    ) -> Result<Option<T>, ConnectionError> {
        connection
            .inner
            .as_mut()
            .unwrap()
            .get(hash_key, sort_key, Some(self.id))
            .await
    }

    pub async fn insert<T: Model>(
        &mut self,
        instance: T,
        connection: &mut Connection,
    ) -> Result<(), ConnectionError> {
        connection
            .inner
            .as_mut()
            .unwrap()
            .insert(instance, Some(self.id))
            .await
    }

    pub async fn delete(
        &mut self,
        hash_key: String,
        sort_key: Value,
        table_name: &str,
        connection: &mut Connection,
    ) -> Result<bool, ConnectionError> {
        connection
            .inner
            .as_mut()
            .unwrap()
            .delete(hash_key, sort_key, table_name, Some(self.id))
            .await
    }

    pub async fn commit(&mut self, connection: &mut Connection) -> Result<(), ConnectionError> {
        self.finished = true;
        connection
            .inner
            .as_mut()
            .unwrap()
            .commit_transaction(self.id, self.coordinator_partition)
            .await
    }

    pub async fn abort(&mut self, connection: &mut Connection) -> Result<(), ConnectionError> {
        self.finished = true;
        connection
            .inner
            .as_mut()
            .unwrap()
            .abort_transaction(self.id, self.coordinator_partition)
            .await
    }
}

// TODO: figure out a way to hold reference to connection here to enable finer API and impl Drop
