use secrecy::{ExposeSecret, SecretBox};
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
        // Keep an explicit copy for file serialization so we can still return
        // the generated key bytes to the running process.
        let mut key_bytes_for_file = key_bytes;
        let master_key = alloy::hex::encode(key_bytes_for_file);
        key_bytes_for_file.zeroize();

        fs::write(path, master_key).context(CreateSnafu)?;

        tracing::info!("Created new master key");
        Ok(key_bytes)
    }

    #[must_use]
    pub fn master_key_bytes(&self) -> [u8; 64] {
        self.master_key.expose_secret().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after unix epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("failed to create temporary directory");
        dir
    }

    #[test]
    fn load_creates_master_key_file_and_uses_same_key_in_memory() {
        let config_dir = unique_temp_dir("otc-server-config");
        let settings =
            Settings::load(config_dir.to_str().expect("utf8 path")).expect("load succeeds");

        let key_path = config_dir.join("otc-server-master-key.hex");
        let file_bytes = fs::read(&key_path).expect("master key file should exist");
        let from_file: [u8; 64] = alloy::hex::decode(file_bytes)
            .expect("hex should decode")
            .try_into()
            .expect("decoded key length should be 64 bytes");

        let from_settings = settings.master_key_bytes();
        assert_ne!(
            from_settings, [0u8; 64],
            "generated master key must not be all zeros"
        );
        assert_eq!(
            from_settings, from_file,
            "in-memory and persisted keys must match"
        );

        fs::remove_dir_all(&config_dir).expect("cleanup temp dir");
    }

    #[test]
    fn load_existing_master_key_uses_persisted_value() {
        let config_dir = unique_temp_dir("otc-server-config-existing");
        let key_path = config_dir.join("otc-server-master-key.hex");

        let expected = [0xABu8; 64];
        fs::write(&key_path, alloy::hex::encode(expected)).expect("write test key");

        let settings =
            Settings::load(config_dir.to_str().expect("utf8 path")).expect("load succeeds");
        assert_eq!(settings.master_key_bytes(), expected);

        fs::remove_dir_all(&config_dir).expect("cleanup temp dir");
    }
}
