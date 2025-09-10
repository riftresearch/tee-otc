use alloy::{
    dyn_abi::DynSolValue,
    primitives::{keccak256, Address, U256},
    providers::DynProvider,
    signers::{local::PrivateKeySigner, SignerSync},
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::EIP7702Delegator::Execution;
use otc_models::{Lot, TokenIdentifier};
use snafu::{Location, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum ReceiveAuthorizationError {
    UnsupportedToken {
        token: TokenIdentifier,
        #[snafu(implicit)]
        loc: Location,
    },
    SignatureFailed {
        source: alloy::signers::Error,
        #[snafu(implicit)]
        loc: Location,
    },
}

pub async fn create_receive_with_authorization_execution(
    lot: &Lot,
    lot_signer: &PrivateKeySigner,
    provider: &DynProvider,
    recipient: &Address,
) -> Result<Execution, ReceiveAuthorizationError> {
    let token_address = match &lot.currency.token {
        TokenIdentifier::Address(address) => address.parse::<Address>().unwrap(),
        _ => {
            return UnsupportedTokenSnafu {
                token: lot.currency.token.clone(),
            }
            .fail()
        }
    };
    // TODO: how should we implement the case when the token is not an EIP-3009 token?
    let eip_3009_token_contract = GenericEIP3009ERC20Instance::new(token_address, provider);

    // TODO: Cache these:
    let domain_separator = eip_3009_token_contract
        .DOMAIN_SEPARATOR()
        .call()
        .await
        .unwrap();

    let receive_with_authorization_typehash = eip_3009_token_contract
        .RECEIVE_WITH_AUTHORIZATION_TYPEHASH()
        .call()
        .await
        .unwrap();

    let from = lot_signer.address();
    let to = *recipient;
    let value = lot.amount;
    let valid_after = U256::ZERO;
    let valid_before = U256::MAX;
    let nonce = [0; 32].into();

    /*
     * @notice Receive a transfer with a signed authorization from the payer
     * @dev This has an additional check to ensure that the payee's address
     * matches the caller of this function to prevent front-running attacks.
     * @param from          Payer's address (Authorizer)
     * @param to            Payee's address
     * @param value         Amount to be transferred
     * @param validAfter    The time after which this is valid (unix time)
     * @param validBefore   The time before which this is valid (unix time)
     * @param nonce         Unique nonce
     * @param v             v of the signature
     * @param r             r of the signature
     * @param s             s of the signature
    function receiveWithAuthorization(
        address from,
        address to,
        uint256 value,
        uint256 validAfter,
        uint256 validBefore,
        bytes32 nonce,
        uint8 v,
        bytes32 r,
        bytes32 s
    )
    */
    let message_value = DynSolValue::Tuple(vec![
        DynSolValue::FixedBytes(receive_with_authorization_typehash, 32), // receive_with_authorization_typehash
        DynSolValue::Address(from),                                       // from
        DynSolValue::Address(to),                                         // to
        DynSolValue::Uint(value, 256),                                    // value
        DynSolValue::Uint(valid_after, 256),                              // validAfter
        DynSolValue::Uint(valid_before, 256),                             // validBefore
        DynSolValue::FixedBytes(nonce, 32), // nonce, zeroed to save on calldata
    ]);

    let encoded_message = message_value.abi_encode();
    let message_hash = keccak256(&encoded_message);
    let eip712_hash = keccak256([&[0x19, 0x01], &domain_separator[..], &message_hash[..]].concat());
    let signature = lot_signer
        .sign_hash_sync(&eip712_hash)
        .context(SignatureFailedSnafu)?;

    let calldata = eip_3009_token_contract
        .receiveWithAuthorization(
            from,
            to,
            value,
            valid_after,
            valid_before,
            nonce,
            27 + (signature.v() as u8), // Note: the Solidity ECRecover library expects v to be 27 or 28
            signature.r().into(),
            signature.s().into(),
        )
        .calldata()
        .clone();

    Ok(Execution {
        target: token_address,
        value: U256::ZERO,
        callData: calldata.clone(),
    })
}
