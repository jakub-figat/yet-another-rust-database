mod model;
mod pool;

pub use crate::model::Model;

pub fn send_insert() {
    // println!("{:?}", user.to_insert_request());
    // let mut insert_request = InsertRequest::new();
    // insert_request.hash_key = "123555aaaaa".to_string();
    //
    // let mut proto_val = ProtoValue::new();
    // proto_val.data = Some(ProtoValueData::Int32(1));
    // insert_request.sort_key = MessageField(Some(Box::new(proto_val)));
    // let mut proto_request = ProtoRequest::new();
    // proto_request.data = Some(ProtoRequestData::Insert(insert_request));
    // let request_bytes = proto_request.write_to_bytes().unwrap();
    //
    // let mut socket = TcpStream::connect("0.0.0.0:29876").unwrap();
    // socket.write_all(&request_bytes).unwrap();
    //
    // let mut buf = Vec::with_capacity(1024);
    // socket.read(&mut buf).unwrap();
    //
    // let proto_response = ProtoResponse::parse_from_bytes(&buf).unwrap();
    // println!("{}", proto_response);
}
