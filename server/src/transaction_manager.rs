use crate::handlers::HandlerError;
use rand::{thread_rng, RngCore};
use std::collections::{HashMap, HashSet};
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

    pub fn add_coordinated(&mut self) -> u64 {
        let mut rng = thread_rng();
        let transaction_id = rng.next_u64();
        self.coordinated_transactions.insert(transaction_id);
        transaction_id
    }

    pub fn remove_coordinated(&mut self, transaction_id: u64) -> Result<(), HandlerError> {
        if self.coordinated_transactions.remove(&transaction_id) {
            return Err(HandlerError::Client(format!(
                "Cannot commit non existing transaction with id '{}'",
                transaction_id
            )));
        }
        Ok(())
    }

    pub fn add(&mut self, transaction_id: u64) {
        self.transactions
            .insert(transaction_id, Transaction::new(transaction_id));
    }

    pub fn remove(&mut self, transaction_id: u64) -> Option<Transaction> {
        self.transactions.remove(&transaction_id)
    }
}
