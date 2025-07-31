use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2, Params, Version,
};
use clap::{Parser, Subcommand};
use common::init_logger;
use dialoguer::Input;
use otc_models::ApiKey;
use rand::{distributions::Alphanumeric, rngs::OsRng, Rng};
use snafu::prelude::*;
use std::fs;
use std::path::PathBuf;
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
        /// Path to the API keys JSON file
        #[arg(long, default_value = "bin/otc-server/prod_whitelisted_market_makers.json")]
        output: PathBuf,
        
        /// Market maker name (if not provided, will prompt interactively)
        #[arg(long)]
        market_maker: Option<String>,
    },
    /// List all API keys
    List {
        /// Path to the API keys JSON file
        #[arg(long, default_value = "bin/otc-server/prod_whitelisted_market_makers.json")]
        input: PathBuf,
    },
}

fn generate_api_key() -> String {
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

fn load_api_keys(path: &PathBuf) -> Result<Vec<ApiKey>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(path).context(IoSnafu)?;
    serde_json::from_str(&content).context(JsonSnafu)
}

fn save_api_keys(path: &PathBuf, api_keys: &[ApiKey]) -> Result<()> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context(IoSnafu)?;
    }

    let json = serde_json::to_string_pretty(api_keys).context(JsonSnafu)?;
    fs::write(path, json).context(IoSnafu)?;
    Ok(())
}

fn generate_command(output: PathBuf, market_maker: Option<String>) -> Result<()> {
    // Get market maker name either from args or prompt
    let market_maker = match market_maker {
        Some(name) => name,
        None => Input::new()
            .with_prompt("Market maker name")
            .validate_with(|input: &String| -> std::result::Result<(), &str> {
                if input.trim().is_empty() {
                    Err("Market maker name cannot be empty")
                } else {
                    Ok(())
                }
            })
            .interact_text()
            .map_err(|e| Error::InvalidInput {
                message: e.to_string(),
            })?,
    };

    // Load existing API keys
    let mut api_keys = load_api_keys(&output)?;

    // Check if market maker already exists
    if api_keys.iter().any(|k| k.market_maker == market_maker) {
        return Err(Error::InvalidInput {
            message: format!("API key for market maker '{market_maker}' already exists"),
        });
    }

    // Generate new API key
    let id = Uuid::new_v4();
    let api_key = generate_api_key();
    let hash = hash_api_key(&api_key)?;

    let new_key = ApiKey {
        id,
        market_maker: market_maker.clone(),
        hash,
    };

    // Add to list and save
    api_keys.push(new_key.clone());
    save_api_keys(&output, &api_keys)?;

    println!("\n✅ API key generated successfully!");
    println!("\n📋 API Key Details:");
    println!("Market Maker: {market_maker}");
    println!("Key ID: {id}");
    println!("\n🔑 API Key (save this, it won't be shown again):");
    println!("{api_key}");
    println!("\n📁 Saved to: {}", output.display());

    Ok(())
}

fn list_command(input: PathBuf) -> Result<()> {
    let api_keys = load_api_keys(&input)?;

    if api_keys.is_empty() {
        println!("No API keys found in {}", input.display());
        return Ok(());
    }

    println!("API Keys in {}:", input.display());
    println!("{:<40} {:<30}", "ID", "Market Maker");
    println!("{}", "-".repeat(70));

    for key in api_keys {
        println!("{:<40} {:<30}", key.id, key.market_maker);
    }

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    init_logger(&args.log_level).expect("Logger should initialize");

    match args.command {
        Command::Generate { output, market_maker } => generate_command(output, market_maker),
        Command::List { input } => list_command(input),
    }
}