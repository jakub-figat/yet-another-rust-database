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
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let addr = SocketAddrV4::from_str("0.0.0.0:29800").unwrap();

    let user = User {
        hash_key: "1".to_string(),
        sort_key: "2".to_string(),
        name: "3".to_string(),
        last_name: "4".to_string(),
    };

    let mut session = Session::new(addr).await.unwrap();
    session.insert(user.clone()).await.unwrap();

    loop {
        thread::sleep(Duration::from_secs(1));
        let user_from_db: User = session
            .get(user.hash_key.clone(), Varchar(user.sort_key.clone(), 1))
            .await
            .unwrap()
            .unwrap();
        println!("{:?}", user_from_db);
    }
    // TODO client conn pool, server max conns/futures
}

#[derive(DatabaseModel, Clone, Debug)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
    pub last_name: String,
}
