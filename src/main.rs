use anyhow::{Result};
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use structopt::StructOpt;
use thiserror::Error;

fn main() {
    let opt = Opt::from_args();
    //println!("{:?}", opt.command);
    let value = match opt.command {
        Command::Get { key } => get(key),
        Command::Set { key, value } => set(key, value),
        Command::FlushAll => flush_all(),
        Command::Del { key } => delete(key),
        Command::Exists { key } => exists(key),
        Command::Rename { key, newkey } => rename(key, newkey),
        Command::Append { key, value } => append(key, value),
        Command::Keys { pattern } => get_keys(pattern),
    };
    match value {
        Ok(value) => println!("{:?}", value),
        Err(value) => eprintln!("{:?}", value)
    }
}

fn get_keys(pattern: String) -> Result<String, KVError> {
    let map = load_keys()?;
    let regex = Regex::new(&pattern).unwrap();
    let keys = map
        .keys()
        .filter(|&x| regex.is_match(x))
        .map(|k| &**k)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("Keys : {}", keys))
}

fn append(key: String, value: String) -> Result<String, KVError> {
    let mut map = load_keys()?;
    let result = map.get_mut(&key);
    match result {
        Some(v) => {
            *v = format!("{}{}", v, value);
            write_keys(map)?;
            Ok("Success".to_string())
        }
        None => Err(KVError::KeyNotFound(key)),
    }
}

fn rename(key: String, new_key: String) -> Result<String, KVError> {
    let mut map = load_keys()?;
    let value = map.remove(&key);
    match value {
        Some(v) => {
            //TODO: Not sure if result is correct approach.
            let result = format!("Renamed key {} with new key {}", &key, &new_key);
            map.insert(new_key, v);
            write_keys(map)?;
            Ok(result)
        }
        None => Err(KVError::KeyNotFound(key)),
    }
}

fn exists(key: String) -> Result<String, KVError> {
    let map = load_keys()?;

    if map.contains_key(&key) {
        Ok("Exists".to_string())
    } else {
        Err(KVError::KeyNotFound(key))
    }
}

fn delete(key: String) -> Result<String, KVError> {
    let mut map = load_keys()?;
    let value = map.remove(&key);
    match value {
        Some(value) => {
            write_keys(map)?;
            Ok(format!("Deleted key {} with value {}", &key, &value))
        }
        None => Err(KVError::KeyNotFound(key)),
    }
}

fn flush_all() -> Result<String, KVError> {
    std::fs::write("kv.db", "{}".to_string())?;
    Ok("Successfully flushed everything!".to_string())
}

fn get(key: String) -> Result<String, KVError> {
    let map = load_keys()?;
    let value = map.get(&key);
    //value.ok_or(("Key not found!"))
    match value {
        Some(value) => Ok(value.to_string()),
        None => Err(KVError::KeyNotFound(key)),
    }
}

fn set(key: String, value: String) -> Result<String, KVError> {
    let mut map = load_keys()?;
    map.insert(key, value);
    write_keys(map)?;
    Ok("Success".to_string())
}

fn write_keys(map: HashMap<String, String>) -> Result<(), KVError> {
    let json_string = serde_json::to_string(&map)?;
    std::fs::write("kv.db", json_string)?;
    Ok(())
}

fn load_keys() -> Result<HashMap<String, String>, KVError> {
    let mut file = match std::fs::File::open("kv.db") {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => std::fs::File::create("kv.db")
            .map_err(|source| KVError::WriteError {
                source: source,
                file: "kv.db".to_string(),
            })?,
        Err(e) => return Err(KVError::GenericError(e)),
    };
    let mut contents = String::new();

    file.read_to_string(&mut contents)
        .map_err(|source| KVError::ReadError { source })?;

    if contents.is_empty() {
        contents.push_str("{}");
    }

    let json: Value = serde_json::from_str(&contents)?;
    match json {
        Value::Object(map) => {
            let mut db = HashMap::new();
            for (k, v) in map {
                match v {
                    Value::String(string) => db.insert(k, string),
                    _ => return Err(KVError::UnableToMap),
                };
            }
            Ok(db)
        }
        _ => return Err(KVError::CorruptDatabase),
    }
}

#[derive(Debug, Error)]
enum KVError {
    #[error("Unable to map the db")]
    UnableToMap,

    #[error("Corrupt database")]
    CorruptDatabase,

    #[error("Failed to create file {file}")]
    WriteError {
        source: std::io::Error,
        file: String,
    },

    #[error("Failed to read the content")]
    ReadError { source: std::io::Error },

    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Failed to read JSON data")]
    DeserializationError(#[from] serde_json::Error),

    #[error("An error occurred {0}")]
    GenericError(std::io::Error),

    #[error("Key not found {0}")]
    KeyNotFound(String)
}

#[derive(Debug, StructOpt)]
#[structopt(name = "KV", about = "A key value store")]
struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "command", about = "Command to set or get value")]
enum Command {
    Get {
        #[structopt(short = "k", long = "key")]
        key: String,
    },
    Set {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "v", long = "value")]
        value: String,
    },
    FlushAll,
    Del {
        #[structopt(short = "k", long = "key")]
        key: String,
    },
    Exists {
        #[structopt(short = "k", long = "key")]
        key: String,
    },
    Rename {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "n", long = "newkey")]
        newkey: String,
    },
    Append {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "v", long = "value")]
        value: String,
    },
    Keys {
        #[structopt(short = "p", long = "pattern")]
        pattern: String,
    },
}
