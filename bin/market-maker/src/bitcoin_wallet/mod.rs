use std::{collections::BTreeSet, future::Future, io::Write, pin::Pin};

use bdk_esplora::{esplora_client, EsploraAsyncExt};
use bdk_wallet::rusqlite::Connection;
use bdk_wallet::{
    bitcoin::{Amount, Network},
    AsyncWalletPersister, ChangeSet, KeychainKind, SignOptions, Wallet,
};
use snafu::{ResultExt, Whatever};

const SEND_AMOUNT: Amount = Amount::from_sat(5000);
const STOP_GAP: usize = 5;
const PARALLEL_REQUESTS: usize = 5;

const DB_PATH: &str = "bdk-example-esplora-async.sqlite";
const NETWORK: Network = Network::Signet;
const EXTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/0/*)";
const INTERNAL_DESC: &str = "wpkh(tprv8ZgxMBicQKsPdy6LMhUtFHAgpocR8GC6QmwMSFpZs7h6Eziw3SpThFfczTDh5rW2krkqffa11UpX3XkeTTB2FvzZKWXqPY54Y6Rq4AQ5R8L/84'/1'/0'/1/*)";
const ESPLORA_URL: &str = "http://signet.bitcoindevkit.net";
// TODO: Build struct now that demo code is compiling

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    let mut conn = Connection::open(DB_PATH).whatever_context("open_db")?;

    let wallet_opt = Wallet::load()
        .descriptor(KeychainKind::External, Some(EXTERNAL_DESC))
        .descriptor(KeychainKind::Internal, Some(INTERNAL_DESC))
        .extract_keys()
        .check_network(NETWORK)
        .load_wallet(&mut conn)
        .whatever_context("load_wallet")?;
    let mut wallet = match wallet_opt {
        Some(wallet) => wallet,
        None => Wallet::create(EXTERNAL_DESC, INTERNAL_DESC)
            .network(NETWORK)
            .create_wallet(&mut conn)
            .whatever_context("create_wallet")?,
    };

    let address = wallet.next_unused_address(KeychainKind::External);
    wallet.persist(&mut conn).whatever_context("persist")?;
    println!("Next unused address: ({}) {}", address.index, address);

    let balance = wallet.balance();
    println!("Wallet balance before syncing: {}", balance.total());

    print!("Syncing...");
    let client = esplora_client::Builder::new(ESPLORA_URL)
        .build_async()
        .whatever_context("build_async")?;

    let request = wallet.start_full_scan().inspect({
        let mut stdout = std::io::stdout();
        let mut once = BTreeSet::<KeychainKind>::new();
        move |keychain, spk_i, _| {
            if once.insert(keychain) {
                print!("\nScanning keychain [{:?}]", keychain);
            }
            print!(" {:<3}", spk_i);
            stdout.flush().expect("must flush")
        }
    });

    let update = client
        .full_scan(request, STOP_GAP, PARALLEL_REQUESTS)
        .await
        .whatever_context("full_scan")?;

    wallet
        .apply_update(update)
        .whatever_context("apply_update")?;
    wallet.persist(&mut conn).whatever_context("persist")?;
    println!();

    let balance = wallet.balance();
    println!("Wallet balance after syncing: {}", balance.total());

    if balance.total() < SEND_AMOUNT {
        println!(
            "Please send at least {} to the receiving address",
            SEND_AMOUNT
        );
        std::process::exit(0);
    }

    let mut tx_builder = wallet.build_tx();
    tx_builder.add_recipient(address.script_pubkey(), SEND_AMOUNT);

    let mut psbt = tx_builder.finish().whatever_context("finish")?;
    let finalized = wallet
        .sign(&mut psbt, SignOptions::default())
        .whatever_context("sign")?;
    assert!(finalized);

    let tx = psbt.extract_tx().whatever_context("extract_tx")?;
    client.broadcast(&tx).await.whatever_context("broadcast")?;
    println!("Tx broadcasted! Txid: {}", tx.compute_txid());

    Ok(())
}
