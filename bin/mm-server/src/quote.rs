use std::time::Duration;

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
use alloy::{
    primitives::{address, Address, U160},
    providers::ProviderBuilder,
    sol,
};

use std::collections::HashMap;

use tokio::time::timeout;
use tracing::{info, error};



#[derive(Debug, Clone, Copy)]
struct ChainAddresses {
    weth_address: Address,
    weth_cbbtc_pool_address: Address,
}

lazy_static::lazy_static! {
    static ref CHAIN_ADDRESSES: HashMap<u64, ChainAddresses> = {
        let mut m = HashMap::new();
        m.insert(1, ChainAddresses {
            weth_address: address!("0xC02aaA39b223FE8D0A0E5C4F27eAD9083C756Cc2"),
            weth_cbbtc_pool_address: address!("0x15aA01580ae866f9FF4DBe45E06e307941d90C7b"),
        });
        m.insert(8453, ChainAddresses {
            weth_address: address!("0x4200000000000000000000000000000000000006"),
            weth_cbbtc_pool_address: address!("0x7AeA2E8A3843516afa07293a10Ac8E49906dabD1"),
        });
        m
    };
}

fn get_weth_address(chain_id: u64) -> Option<Address> {
    CHAIN_ADDRESSES
        .get(&chain_id)
        .map(|addrs| addrs.weth_address)
}

fn get_weth_cbbtc_pool_address(chain_id: u64) -> Option<Address> {
    CHAIN_ADDRESSES
        .get(&chain_id)
        .map(|addrs| addrs.weth_cbbtc_pool_address)
}

sol!(
    #[sol(rpc)]
    IUniswapV3PoolState,
    r#"[{
        "inputs": [],
        "name": "slot0",
        "outputs": [
        {
            "internalType": "uint160",
            "name": "sqrtPriceX96",
            "type": "uint160"
        },
        {
            "internalType": "int24",
            "name": "tick",
            "type": "int24"
        },
        {
            "internalType": "uint16",
            "name": "observationIndex",
            "type": "uint16"
        },
        {
            "internalType": "uint16",
            "name": "observationCardinality",
            "type": "uint16"
        },
        {
            "internalType": "uint16",
            "name": "observationCardinalityNext",
            "type": "uint16"
        },
        {
            "internalType": "uint8",
            "name": "feeProtocol",
            "type": "uint8"
        },
        {
            "internalType": "bool",
            "name": "unlocked",
            "type": "bool"
        }
        ],
        "stateMutability": "view",
        "type": "function"
    }]"#
);

sol!(
    #[sol(rpc)]
    IUniswapV3PoolImmutables,
    r#"[
        {
            "inputs": [],
            "name": "token0",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [],
            "name": "token1",
            "outputs": [{"internalType": "address", "name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function"
        }
    ]"#
);



#[derive(Debug, Clone, Copy)]
pub struct ConversionRates {
    pub cbbtc_per_eth: f64,
    pub eth_per_cbbtc: f64,
}



pub async fn fetch_weth_cbbtc_conversion_rates(chain_id: u64) -> anyhow::Result<ConversionRates> {
    let rpcs = match chain_id {
        1 => vec![
            "https://eth.llamarpc.com".to_string(),
            "https://eth-mainnet.public.blastapi.io".to_string(),
            "https://eth.blockpi.network/v1/rpc/public".to_string(),
            "https://1rpc.io/eth".to_string(),
        ],
        8453 => vec![
            "https://base.llamarpc.com".to_string(),
            "https://base-mainnet.public.blastapi.io".to_string(),
            "https://base.blockpi.network/v1/rpc/public".to_string(),
            "https://1rpc.io/base".to_string(),
        ],
        _ => return Err(anyhow::anyhow!("No RPCs for chain_id: {}", chain_id)),
    };

    let mut last_error = None;
    for rpc_url in rpcs {
        info!("Trying RPC: {}", rpc_url);
        match try_fetch_from_rpc(&rpc_url, chain_id).await {
            Ok(rates) => return Ok(rates),
            Err(e) => {
                error!("RPC {} failed: {}", rpc_url, e);
                last_error = Some(e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("All RPCs failed")))
}

async fn try_fetch_from_rpc(rpc_url: &str, chain_id: u64) -> anyhow::Result<ConversionRates> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    let pool_address = get_weth_cbbtc_pool_address(chain_id).ok_or_else(|| anyhow::anyhow!("Pool not configured"))?;

    let pool = IUniswapV3PoolState::new(pool_address, &provider);
    let pool_immut = IUniswapV3PoolImmutables::new(pool_address, &provider);

    let (slot0_data, token0_addr, token1_addr) = timeout(RPC_TIMEOUT, async {
        let slot0 = pool.slot0().call().await?;
        let token0 = pool_immut.token0().call().await?;
        let token1 = pool_immut.token1().call().await?;
        Ok::<_, anyhow::Error>((slot0, token0, token1))
    }).await??;

    info!("Fetched data: slot0 sqrtPriceX96 = {}, token0 = {:?}, token1 = {:?}", slot0_data.sqrtPriceX96, token0_addr, token1_addr);

    let p_sqrt = q64_96_to_float(slot0_data.sqrtPriceX96);
    let price_t1_in_t0_unadjusted = p_sqrt * p_sqrt;

    let weth_address = get_weth_address(chain_id).ok_or_else(|| {
        anyhow::anyhow!("WETH address not configured for chain_id: {}", chain_id)
    })?;

    let (cbbtc_per_eth, eth_per_cbbtc) = if token0_addr == weth_address {
        let cbbtc_per_eth = price_t1_in_t0_unadjusted * 10f64.powi(10); // 18 - 8 = 10
        let eth_per_cbbtc = 1.0 / cbbtc_per_eth;
        info!("Computed rates: cbbtc_per_eth = {:.8}, eth_per_cbbtc = {:.8}", cbbtc_per_eth, eth_per_cbbtc);
        (cbbtc_per_eth, eth_per_cbbtc)
    } else if token1_addr == weth_address {
        let eth_per_cbbtc = price_t1_in_t0_unadjusted / 10f64.powi(10);
        let cbbtc_per_eth = 1.0 / eth_per_cbbtc;
        info!("Computed rates: cbbtc_per_eth = {:.8}, eth_per_cbbtc = {:.8}", cbbtc_per_eth, eth_per_cbbtc);
        (cbbtc_per_eth, eth_per_cbbtc)
    } else {
        return Err(anyhow::anyhow!("Invalid token configuration"));
    };

    Ok(ConversionRates { cbbtc_per_eth, eth_per_cbbtc })
}

pub fn q64_96_to_float(num: U160) -> f64 {
    let limbs = num.into_limbs();
    let lo = limbs[0] as f64; // bits 0..64
    let mid = limbs[1] as f64 * 2f64.powi(64); // bits 64..128
    let hi = limbs[2] as f64 * 2f64.powi(128); // bits 128..160 (top 32 bits used)

    let full = lo + mid + hi;
    full / 2f64.powi(96)
}


