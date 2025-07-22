use config::{Config, File};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::path::Path;
use zeroize::Zeroize;

#[derive(Debug, Snafu)]
pub enum SettingsError {
    #[snafu(display("Failed to load config: {}", source))]
    Load { source: config::ConfigError },
    
    #[snafu(display("Failed to create config file: {}", source))]
    Create { source: std::io::Error },
}

type Result<T> = std::result::Result<T, SettingsError>;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub master_key: SecretString,
}

#[derive(Serialize)]
struct DefaultSettings {
    master_key: String,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let config_path = "otc-server.toml";
        
        // Check for environment variable first (useful for tests)
        if let Ok(master_key) = std::env::var("OTC_MASTER_KEY") {
            return Ok(Settings {
                master_key: SecretString::from(master_key),
            });
        }
        
        // Create default config if it doesn't exist
        if !Path::new(config_path).exists() {
            Self::create_default_config(config_path)?;
        }
        
        let settings = Config::builder()
            .add_source(File::with_name(config_path))
            .build()
            .context(LoadSnafu)?;
        
        settings.try_deserialize().context(LoadSnafu)
    }
    
    fn create_default_config(path: &str) -> Result<()> {
        use std::fs;
        
        // Generate a random 64-byte master key
        let mut key_bytes = [0u8; 64];
        getrandom::getrandom(&mut key_bytes).expect("Failed to generate random bytes");
        let master_key = hex::encode(&key_bytes);
        key_bytes.zeroize();
        
        let default = DefaultSettings { master_key };
        
        let toml = toml::to_string_pretty(&default)
            .expect("Failed to serialize default config");
        
        fs::write(path, toml).context(CreateSnafu)?;
        
        tracing::info!("Created default config file at {}", path);
        Ok(())
    }
    
    pub fn master_key_bytes(&self) -> Vec<u8> {
        hex::decode(self.master_key.expose_secret())
            .expect("Master key should be valid hex")
    }
}