use bitcoin::{key::{CompressedPublicKey, Secp256k1}, Address, PrivateKey};
use blockchain_utils::P2WPKHBitcoinWallet;

fn main() {
    // Extract WIF key from descriptor format
    let wif = "cUGWCiMZN5iRpQgYGU5DRFCed9nzLk1qj6MuJotnSXw9gkyw5huj";

    let private_key = PrivateKey::from_wif(wif).unwrap();
    println!("private key: {}", private_key);

    let secp = Secp256k1::new();
    let compressed_pk = CompressedPublicKey::from_private_key(&secp, &private_key).unwrap();
    let address = Address::p2wpkh(&compressed_pk, bitcoin::Network::Regtest);
    println!("address: {}", address);

    let rand_bytes = rand::random::<[u8; 32]>();
    let wallet = P2WPKHBitcoinWallet::from_secret_bytes(&rand_bytes, bitcoin::Network::Regtest);
    println!("rand regtest bitcoin wallet");
    println!("descriptor: {}", wallet.descriptor());
    println!("address: {}", wallet.address);
}
