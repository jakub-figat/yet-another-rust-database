use crate::handlers::HandlerError;
use futures::lock::Mutex;
use rand::{thread_rng, RngCore};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use storage::table::Table;
use storage::transaction::Transaction;

pub struct TransactionManager {
    pub transactions: HashMap<u64, Transaction>,
    coordinated_transactions: HashSet<u64>,
}

impl TransactionManager {
    pub fn new() -> TransactionManager {
        TransactionManager {
            transactions: HashMap::new(),
            coordinated_transactions: HashSet::new(),
        }
    }

    pub fn begin(&mut self) -> u64 {
        let mut rng = thread_rng();
        let transaction_id = rng.next_u64();
        self.coordinated_transactions.insert(transaction_id);
        self.transactions
            .insert(transaction_id, Transaction::new(transaction_id));
        transaction_id
    }

    pub async fn commit(
        &mut self,
        transaction_id: u64,
        tables: Arc<HashMap<String, Mutex<Table>>>,
    ) -> Result<(), HandlerError> {
        match self.coordinated_transactions.contains(&transaction_id) {
            true => {
                self.coordinated_transactions.remove(&transaction_id);

                let mut transaction = self.transactions.remove(&transaction_id).unwrap();
                if !transaction.can_commit(tables.clone()).await {
                    return Err(HandlerError::Server(format!(
                        "Transaction with id '{}' failed to commit and got aborted",
                        transaction_id
                    )));
                }

                transaction.commit(tables.clone()).await;

                Ok(())
            }
            false => Err(HandlerError::Client(format!(
                "Cannot commit non existing transaction with id '{}'",
                transaction_id
            ))),
        }
    }

    pub fn abort(&mut self, transaction_id: u64) -> bool {
        match self.coordinated_transactions.contains(&transaction_id) {
            true => {
                self.coordinated_transactions.remove(&transaction_id);
                let _ = self.transactions.remove(&transaction_id);
                true
            }
            false => false,
        }
    }

    pub fn add(&mut self, transaction_id: u64) {
        self.transactions
            .insert(transaction_id, Transaction::new(transaction_id));
    }

    pub fn remove(&mut self, transaction_id: u64) {
        let _ = self.transactions.remove(&transaction_id);
    }
}
