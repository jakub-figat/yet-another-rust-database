use client::{Model, Session};
use common::value::Value::Varchar;
use macros::DatabaseModel;
use protos::util::{
    parse_message_field_from_value, parse_proto_from_value, parse_value_from_message_field,
    parse_value_from_proto,
};
use protos::{DeleteRequest, GetResponse, InsertRequest};
use std::net::SocketAddrV4;
use std::str::FromStr;
use std::time::Instant;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    let mut join_set = JoinSet::new();
    let total_num_of_objects = 100_000;
    let parallelism = 20;
    let objects_per_future = total_num_of_objects / parallelism;

    for num in 0..parallelism {
        join_set.spawn(async move {
            let addr = SocketAddrV4::from_str("0.0.0.0:29800").unwrap();
            let users: Vec<_> = (num * objects_per_future..(num + 1) * objects_per_future)
                .map(|key| User {
                    hash_key: key.to_string(),
                    sort_key: key.to_string(),
                    name: "aaa".to_string(),
                })
                .collect();

            let mut session = Session::new(addr).await.unwrap();

            for user in users {
                session.insert(user).await.unwrap();
            }

            for key in num * objects_per_future..(num + 1) * objects_per_future {
                session
                    .get::<User>(key.to_string(), Varchar(key.to_string(), 1))
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
    }

    let start = Instant::now();
    while join_set.join_next().await.is_some() {}
    println!("done, {}ms", start.elapsed().as_millis());
}

#[derive(DatabaseModel, Clone, Debug)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
}
