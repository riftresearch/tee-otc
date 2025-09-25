use blockchain_utils::P2WPKHBitcoinWallet;

fn main() {
    let rand_bytes = rand::random::<[u8; 32]>();
    let wallet = P2WPKHBitcoinWallet::from_secret_bytes(&rand_bytes, bitcoin::Network::Regtest);
    println!("rand regtest bitcoin wallet");
    println!("descriptor: {}", wallet.descriptor());
    println!("address: {}", wallet.address);
}
