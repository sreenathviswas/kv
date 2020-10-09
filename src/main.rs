use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;
use structopt::StructOpt;

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
    println!("{:?}", value.unwrap());
}

fn get_keys(pattern: String) -> std::io::Result<String> {
    let map: HashMap<String, String> = load_keys()?;
    let regex = Regex::new(&pattern).unwrap();
    let keys = map
        .keys()
        .filter(|&x| regex.is_match(x))
        .map(|k| &**k)
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("Keys : {}", keys))
}

fn append(key: String, value: String) -> std::io::Result<String> {
    let mut map: HashMap<String, String> = load_keys()?;
    let result = map.get_mut(&key);
    match result {
        Some(v) => {
            *v = format!("{}{}", v, value);
            write_keys(map)?;
            Ok("Success".to_string())
        }
        None => Ok("Key not found".to_string()),
    }
}

fn rename(key: String, new_key: String) -> std::io::Result<String> {
    let mut map: HashMap<String, String> = load_keys()?;
    let value = map.remove(&key);
    match value {
        Some(v) => {
            //TODO: Not sure if result is correct approach.
            let result = format!("Renamed key {} with new key {}", &key, &new_key);
            map.insert(new_key, v.to_string());
            write_keys(map)?;
            Ok(result)
        }
        None => Ok("Key not found".to_string()),
    }
}

fn exists(key: String) -> std::io::Result<String> {
    let map: HashMap<String, String> = load_keys()?;

    let exists = map.keys().filter(|&x| x == &key).collect::<Vec<_>>().len();

    if exists > 0 {
        Ok("Exists".to_string())
    } else {
        Ok("Key not found".to_string())
    }
}

fn delete(key: String) -> std::io::Result<String> {
    let mut map: HashMap<String, String> = load_keys()?;
    let value = map.remove(&key);
    match value {
        Some(value) => {
            write_keys(map)?;
            Ok(format!("Deleted key {} with value {}", &key, &value))
        }
        None => Ok("Key not found".to_string()),
    }
}

fn flush_all() -> std::io::Result<String> {
    std::fs::write("kv.db", "{}".to_string())?;
    Ok("Successfully flushed everything!".to_string())
}

fn get(key: String) -> std::io::Result<String> {
    let map: HashMap<String, String> = load_keys()?;
    let value = map.get(&key);
    //value.ok_or(("Key not found!"))
    match value {
        Some(value) => Ok(value.to_string()),
        None => Ok("Key not found".to_string()),
    }
}

fn set(key: String, value: String) -> std::io::Result<String> {
    let mut map = load_keys()?;
    map.insert(key, value);
    write_keys(map)?;
    Ok("Success".to_string())
}

fn write_keys(map: HashMap<String, String>) -> std::io::Result<()> {
    let json_string = serde_json::to_string(&map)?;
    std::fs::write("kv.db", json_string)?;
    Ok(())
}

fn load_keys() -> std::io::Result<HashMap<String, String>> {
    let mut file = match std::fs::File::open("kv.db") {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => std::fs::File::create("kv.db")?,
        Err(e) => return Err(e),
    };
    let mut contents = String::new();

    file.read_to_string(&mut contents)?;

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
                    _ => panic!("Unable to map the db"),
                };
            }
            Ok(db)
        }
        _ => panic!("Corrupt database"),
    }
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
