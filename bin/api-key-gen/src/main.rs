use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2, Params, Version,
};
use clap::{Parser, Subcommand};
use blockchain_utils::init_logger;
use otc_models::PublicApiKeyRecord;
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};
use snafu::prelude::*;
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("IO error: {}", source))]
    Io { source: std::io::Error },

    #[snafu(display("JSON error: {}", source))]
    Json { source: serde_json::Error },

    #[snafu(display("Password hashing error: {}", message))]
    PasswordHash { message: String },

    #[snafu(display("Invalid input: {}", message))]
    InvalidInput { message: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[command(name = "api-key-manager")]
#[command(about = "Manage API keys for market makers")]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a new API key
    Generate {
        /// Market maker name 
        #[arg(long)]
        tag: String,
    },
}

fn generate_api_secret() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

fn hash_api_key(api_key: &str) -> Result<String> {
    // OWASP recommended settings: m=19456 (19 MiB), t=2, p=1
    let params = Params::new(19456, 2, 1, None)
        .map_err(|e| Error::PasswordHash {
            message: e.to_string(),
        })?;

    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, Version::V0x13, params);
    let salt = SaltString::generate(&mut OsRng);

    let hash = argon2
        .hash_password(api_key.as_bytes(), &salt)
        .map_err(|e| Error::PasswordHash {
            message: e.to_string(),
        })?;

    Ok(hash.to_string())
}



fn generate_command(tag: String) -> Result<()> {

    // Generate new API key
    let id = Uuid::new_v4();
    let api_secret = generate_api_secret();
    let hash = hash_api_key(&api_secret)?;

    let key_record = PublicApiKeyRecord {
        id,
        tag: tag.clone(),
        hash: hash.clone(),
    };


    println!("Market Maker: {tag}");
    println!("ID: {id}");
    println!("\nAPI Secret (save this, it won't be shown again):");
    println!("{api_secret}\n");
    println!("{key_record:#?}");

    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    match args.command {
        Command::Generate { tag } => generate_command(tag),
    }
}