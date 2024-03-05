use client::pool::ConnectionPool;
use client::Model;
use common::value::Value::Varchar;
use macros::DatabaseModel;
use protos::util::{
    parse_message_field_from_value, parse_proto_from_value, parse_value_from_message_field,
    parse_value_from_proto,
};
use protos::{DeleteRequest, GetResponse, InsertRequest};
use std::net::SocketAddrV4;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    let total_num_of_objects = 100_000;
    let parallelism = 10;
    let objects_per_future = total_num_of_objects / parallelism;

    let addr = SocketAddrV4::from_str("0.0.0.0:29800").unwrap();
    let connection_pool = ConnectionPool::new(addr, 10).await.unwrap();

    let mut join_set = JoinSet::new();
    for num in 0..parallelism {
        join_set.spawn(worker(connection_pool.clone(), num, objects_per_future));
    }
    let start = Instant::now();
    while let Some(result) = join_set.join_next().await {
        result.unwrap();
    }
    println!("done, {}ms", start.elapsed().as_millis());
}

async fn worker(connection_pool: Arc<ConnectionPool>, num: usize, objects_per_future: usize) {
    let mut connection = connection_pool.acquire().await;

    let users: Vec<_> = (num * objects_per_future..(num + 1) * objects_per_future)
        .map(|key| User {
            hash_key: key.to_string(),
            sort_key: key.to_string(),
            name: "aaa".to_string(),
        })
        .collect();

    for user in users {
        connection.insert(user).await.unwrap();
    }

    for key in num * objects_per_future..(num + 1) * objects_per_future {
        connection
            .get::<User>(key.to_string(), Varchar(key.to_string(), 1))
            .await
            .unwrap()
            .unwrap();
    }
}

#[derive(DatabaseModel, Clone, Debug)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
}
