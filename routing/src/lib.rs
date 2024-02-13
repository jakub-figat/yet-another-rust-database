use futures::channel::mpsc;

#[derive(Debug)]
pub enum Command {
    Get(String, u64),
    Insert(String, u64),
    Delete(String, u64),
    // GetBetween(String, String, u64, u64),
}

impl Command {
    pub fn primary_key(&self) -> String {
        match self {
            Command::Get(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
            Command::Insert(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
            Command::Delete(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Get(Option<(String, u64)>),
    Write(Result<(), String>),
    Delete(Option<(String, u64)>),
}

fn get_primary_key(hash_key: &str, sort_key: &u64) -> String {
    let sort_key_string = sort_key.to_string();
    let mut primary_key = String::with_capacity(hash_key.len() + sort_key_string.len() + 1);

    primary_key.push_str(&hash_key);
    primary_key.push_str(":");
    primary_key.push_str(&sort_key_string);
    primary_key
}

pub type CommandSender = mpsc::Sender<Command>;
pub type CommandReceiver = mpsc::Receiver<Command>;

pub type ResponseSender = mpsc::Sender<Response>;
pub type ResponseReceiver = mpsc::Receiver<Response>;
