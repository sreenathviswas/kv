use anyhow::Result;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::Formatter;
use structopt::StructOpt;
use thiserror::Error;

fn main() -> Result<()> {
    let opt = Opt::from_args();
    //println!("{:?}", opt.command);
    let value = match opt.command {
        Command::Get { key, serializer } => {
            let store = KV::new(serializer);
            store.get(key)?
        }
        Command::Set { key, value, serializer } => {
            let store = KV::new(serializer);
            store.set(key, value)?;
            "OK".into()
        }
        Command::Clear { serializer } => {
            let store = KV::new(serializer);
            store.clear()?;
            "OK".into()
        }
        Command::Del { key, serializer } => {
            let store = KV::new(serializer);
            store.delete(key)?;
            "OK".into()
        }
        Command::Exists { key, serializer } => {
            let store = KV::new(serializer);
            if store.exists(key)? {
                "OK".into()
            } else {
                "Not exists".into()
            }
        }
        Command::Rename { key, newkey, serializer} => {
            let store = KV::new(serializer);
            store.rename(key, newkey)?;
            "OK".into()
        }
        Command::Append { key, value, serializer } => {
            let store = KV::new(serializer);
            store.append(key, value)?;
            "OK".into()
        }
        Command::Keys { pattern, serializer } => {
            let store = KV::new(serializer);
            let keys = store.get_keys(pattern)?;
            format!("Keys : {}", keys.join(", "))
        }
    };
    println!("{:?}", value);
    Ok(())
}

#[derive(Debug, Error)]
pub enum KVError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error("Failed to read JSON data")]
    DeserializationError(#[from] serde_json::Error),

    #[error("An error occurred {0}")]
    GenericError(std::io::Error),

    #[error("Key not found {0}")]
    KeyNotFound(String),

    #[error("Failed to read BSON data")]
    DeserializeError(#[from] bson::de::Error),

    #[error("Failed to write BSON data")]
    SerializeError(#[from] bson::ser::Error),
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

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Set {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "v", long = "value")]
        value: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Clear{
        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Del {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Exists {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Rename {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "n", long = "newkey")]
        newkey: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Append {
        #[structopt(short = "k", long = "key")]
        key: String,

        #[structopt(short = "v", long = "value")]
        value: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
    Keys {
        #[structopt(short = "p", long = "pattern")]
        pattern: String,

        #[structopt(short = "s", long = "serializer", default_value = "Bson")]
        serializer: Box<dyn BackendStorage>,
    },
}

pub trait BackendStorage {
    fn load_keys(&self) -> Result<HashMap<String, String>, KVError>;
    fn write_keys(&self, map: HashMap<String, String>) -> Result<(), KVError>;
    fn clear(&self) -> Result<(), KVError>;
}

impl std::str::FromStr for Box<dyn BackendStorage> {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "Json" => Ok(Box::new(JsonBackendStorage)),
            "Bson" => Ok(Box::new(BsonBackendStorage)),
            _ => Err("Serializer must be either Json or Bson".to_string()),
        }
    }
}

impl std::fmt::Debug for Box<dyn BackendStorage> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            // .field("Serializer", match &self {
            //     JsonBackendStorage => "Json".to_string(),
            //     BsonBackendStorage => "Bson".to_string()
            // })
            .finish()
    }
}
pub struct JsonBackendStorage;

impl BackendStorage for JsonBackendStorage {
    fn load_keys(&self) -> Result<HashMap<String, String>, KVError> {
        let file = match std::fs::File::open("kv.db") {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(KVError::GenericError(e)),
        };
        let reader = std::io::BufReader::new(file);
        let map = serde_json::from_reader(reader)?;
        Ok(map)
    }

    fn write_keys(&self, map: HashMap<String, String>) -> Result<(), KVError> {
        let json_string = serde_json::to_string(&map)?;
        std::fs::write("kv.db", json_string)?;
        Ok(())
    }

    fn clear(&self) -> Result<(), KVError> {
        std::fs::write("kv.db", "{}".to_string())?;
        Ok(())
    }
}

pub struct BsonBackendStorage;

impl BackendStorage for BsonBackendStorage {
    fn load_keys(&self) -> Result<HashMap<String, String>, KVError> {
        let file = match std::fs::File::open("kv.bson") {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(KVError::GenericError(e)),
        };
        let mut reader = std::io::BufReader::new(file);
        let document = bson::document::Document::from_reader(&mut reader)?;
        let map = bson::from_bson(document.into())?;
        Ok(map)
    }

    fn write_keys(&self, map: HashMap<String, String>) -> Result<(), KVError> {
        let bson = bson::to_document(&map)?;
        let file = std::fs::File::create("kv.bson")?;
        let mut buffer = std::io::BufWriter::new(file);
        bson.to_writer(&mut buffer)?;
        Ok(())
    }

    fn clear(&self) -> Result<(), KVError> {
        std::fs::remove_file("kv.bson")?;
        Ok(())
    }
}

pub struct KV {
    pub storage: Box<dyn BackendStorage>,
}

impl KV {
    fn new(storage: Box<dyn BackendStorage>) -> Self {
        Self { storage }
    }

    fn get_keys(&self, pattern: String) -> Result<Vec<String>, KVError> {
        let map = self.storage.load_keys()?;
        let regex = Regex::new(&pattern).unwrap();
        let keys = map
            .keys()
            .filter(|&x| regex.is_match(x))
            .map(|k| &**k)
            .map(String::from)
            .collect::<Vec<_>>();
        Ok(keys)
    }

    fn append(&self, key: String, value: String) -> Result<(), KVError> {
        let mut map = self.storage.load_keys()?;
        let result = map.get_mut(&key);
        match result {
            Some(v) => {
                *v = format!("{}{}", v, value);
                self.storage.write_keys(map)?;
                Ok(())
            }
            None => Err(KVError::KeyNotFound(key)),
        }
    }

    fn rename(&self, key: String, new_key: String) -> Result<(), KVError> {
        let mut map = self.storage.load_keys()?;
        let value = map.remove(&key);
        match value {
            Some(v) => {
                map.insert(new_key, v);
                self.storage.write_keys(map)?;
                Ok(())
            }
            None => Err(KVError::KeyNotFound(key)),
        }
    }

    fn exists(&self, key: String) -> Result<bool, KVError> {
        let map = self.storage.load_keys()?;
        Ok(map.contains_key(&key))
    }

    fn delete(&self, key: String) -> Result<(), KVError> {
        let mut map = self.storage.load_keys()?;
        let value = map.remove(&key);
        match value {
            Some(_value) => {
                self.storage.write_keys(map)?;
                Ok(())
            }
            None => Err(KVError::KeyNotFound(key)),
        }
    }

    fn clear(&self) -> Result<(), KVError> {
        self.storage.clear()
    }

    fn get(&self, key: String) -> Result<String, KVError> {
        let map = self.storage.load_keys()?;
        let value = map.get(&key);
        //value.ok_or(Err(KVError::KeyNotFound(key)))
        match value {
            Some(value) => Ok(value.into()),
            None => Err(KVError::KeyNotFound(key)),
        }
    }

    fn set(&self, key: String, value: String) -> Result<(), KVError> {
        let mut map = self.storage.load_keys()?;
        map.insert(key, value);
        self.storage.write_keys(map)?;
        Ok(())
    }

    //#[test]
    // fn get_keys_returns_keys(){
    //     flush_all().unwrap();
    //     set("Abc".to_string(), "Abc".to_string()).unwrap();
    //     set("Abi".to_string(), "Abi".to_string()).unwrap();
    //     set("Xyz".to_string(), "Xyz".to_string()).unwrap();

    //     let keys = get_keys("Abc".to_string());

    //     match keys {
    //         Ok(value) => assert!(value.len() > 0),
    //         Err(_e) => panic!("get keys test failed")
    //     }
    // }

    // #[test]
    // fn append_returns_error_on_key_not_found(){
    //     let keys = append("123".to_string(), "append".to_string());

    //     match keys {
    //         Ok(_value) => panic!("Should not reach here"),
    //         Err(_e) => assert!(true)
    //     }
    // }
}
