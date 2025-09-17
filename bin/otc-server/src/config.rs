use secrecy::{ExposeSecret, SecretBox, SecretSlice, SecretString};
use snafu::{ResultExt, Snafu};
use std::{fs, path::Path};
use zeroize::Zeroize;

#[derive(Debug, Snafu)]
pub enum SettingsError {
    #[snafu(display("Failed to create config file: {}", source))]
    Create { source: std::io::Error },

    #[snafu(display("Failed to load config file: {}", source))]
    Load { source: std::io::Error },

    #[snafu(display("Failed to generate random bytes: {}", source))]
    Random { source: getrandom::Error },

    #[snafu(display("Failed to decode hex: {}", source))]
    HexDecode { source: alloy::hex::FromHexError },

    #[snafu(display("Invalid master key length"))]
    KeyLength {},
}

type Result<T> = std::result::Result<T, SettingsError>;

#[derive(Debug, Clone)]
pub struct MasterKey([u8; 64]);

#[derive(Debug)]
pub struct Settings {
    pub master_key: SecretBox<MasterKey>,
}

impl Zeroize for MasterKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl Settings {
    pub fn load(config_dir: &str) -> Result<Self> {
        let config_path = format!("{config_dir}/otc-server-master-key.hex");

        // Create default config if it doesn't exist
        let master_key = if !Path::new(&config_path).exists() {
            Self::create_default_config(&config_path)?
        } else {
            alloy::hex::decode(fs::read(config_path).context(LoadSnafu)?)
                .context(HexDecodeSnafu)?
                .try_into()
                .map_err(|_| SettingsError::KeyLength {})?
        };

        Ok(Settings {
            master_key: SecretBox::new(Box::new(MasterKey(master_key))),
        })
    }

    fn create_default_config(path: &str) -> Result<[u8; 64]> {
        // Generate a random 64-byte master key
        let mut key_bytes = [0u8; 64];
        getrandom::getrandom(&mut key_bytes).context(RandomSnafu)?;
        let master_key = alloy::hex::encode(key_bytes);
        key_bytes.zeroize();

        fs::write(path, master_key).context(CreateSnafu)?;

        tracing::info!("Created new master key");
        Ok(key_bytes)
    }

    #[must_use]
    pub fn master_key_bytes(&self) -> [u8; 64] {
        self.master_key.expose_secret().0
    }
}
