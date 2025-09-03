use alloy::{hex, primitives::FixedBytes, sol};

pub const EIP7702_DELEGATOR_BYTECODE: &str = include_str!("deployed-bytecode.hex");

pub const EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS: &str = "0x63c0c19a282a1B52b07dD5a65b58948A07DAE32B";

pub enum ModeCode {
    Batch,
    Single,
}

impl ModeCode {
    /// This code is derived from https://github.com/erc7579/erc7579-implementation/blob/16138d1afd4e9711f6c1425133538837bd7787b5/src/lib/ModeLib.sol#L124-L130
    /// running the above methods returns the following bytes32 values
    pub fn as_fixed_bytes32(self) -> FixedBytes<32> {
        match self {
            ModeCode::Batch => FixedBytes::from_slice(&hex!(
                "0100000000000000000000000000000000000000000000000000000000000000"
            )),
            ModeCode::Single => FixedBytes::from_slice(&hex!(
                "0000000000000000000000000000000000000000000000000000000000000000"
            )),
        }
    }
}

sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    EIP7702Delegator,"src/abi.json",
}
