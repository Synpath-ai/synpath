//! Order signing for Python: `Secp256k1Signer` and the order digests.
//!
//! `synpath.trading.polymarket_signing.WalletSigner` keeps one of these next
//! to its `eth_account` account and signs orders with it. Every argument
//! arrives as the Python code holds it (amounts as decimal strings), and
//! anything the Rust encoder refuses raises `ValueError`, on which the
//! Python side signs the same order the `eth_account` way instead -- so a
//! bad order fails with exactly the error it always did.

use k256::ecdsa::SigningKey;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use synpath_core::signing::{self, opinion, polymarket, Word};

fn value_error(message: String) -> PyErr {
    PyValueError::new_err(message)
}

fn word(text: &str) -> PyResult<Word> {
    signing::uint(text).map_err(value_error)
}

fn hex0x(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[allow(clippy::too_many_arguments)]
fn polymarket_order(
    salt: &str,
    maker: &str,
    signer: &str,
    token_id: &str,
    maker_amount: &str,
    taker_amount: &str,
    side: u8,
    signature_type: u8,
    timestamp: &str,
    metadata: &str,
    builder: &str,
) -> PyResult<polymarket::Order> {
    Ok(polymarket::Order {
        salt: word(salt)?,
        maker: signing::address(maker).map_err(value_error)?,
        signer: signing::address(signer).map_err(value_error)?,
        token_id: word(token_id)?,
        maker_amount: word(maker_amount)?,
        taker_amount: word(taker_amount)?,
        side,
        signature_type,
        timestamp: word(timestamp)?,
        metadata: signing::bytes32(metadata).map_err(value_error)?,
        builder: signing::bytes32(builder).map_err(value_error)?,
    })
}

/// The Polymarket order's EIP-712 digest, `0x`-hex: the id the CLOB gives it.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(crate) fn polymarket_order_hash(
    salt: &str,
    maker: &str,
    signer: &str,
    token_id: &str,
    maker_amount: &str,
    taker_amount: &str,
    side: u8,
    signature_type: u8,
    timestamp: &str,
    metadata: &str,
    builder: &str,
    neg_risk: bool,
) -> PyResult<String> {
    let order = polymarket_order(
        salt,
        maker,
        signer,
        token_id,
        maker_amount,
        taker_amount,
        side,
        signature_type,
        timestamp,
        metadata,
        builder,
    )?;
    Ok(hex0x(&polymarket::order_hash(&order, neg_risk)))
}

/// A secp256k1 key that signs digests and venue orders. It never leaves
/// Rust memory, is not printed, and is wiped when the object is dropped.
#[pyclass(module = "synpath._core", name = "Secp256k1Signer", frozen)]
pub struct Secp256k1Signer {
    key: SigningKey,
}

#[pymethods]
impl Secp256k1Signer {
    #[new]
    fn new(private_key: &str) -> PyResult<Self> {
        Ok(Self {
            key: signing::signing_key(private_key).map_err(value_error)?,
        })
    }

    /// The key's address, checksummed.
    #[getter]
    fn address(&self) -> String {
        signing::address_of(&self.key)
    }

    /// `r || s || v` over a 32-byte digest, as `Account._sign_hash` gives it.
    fn sign_digest<'py>(&self, py: Python<'py>, digest: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
        let digest: Word = digest
            .try_into()
            .map_err(|_| value_error(format!("a digest is 32 bytes, got {}", digest.len())))?;
        let signature = signing::sign_digest(&self.key, &digest).map_err(value_error)?;
        Ok(PyBytes::new(py, &signature))
    }

    /// The Polymarket order's signature, `0x`-hex, for its wallet type.
    #[allow(clippy::too_many_arguments)]
    fn polymarket_sign_order(
        &self,
        salt: &str,
        maker: &str,
        signer: &str,
        token_id: &str,
        maker_amount: &str,
        taker_amount: &str,
        side: u8,
        signature_type: u8,
        timestamp: &str,
        metadata: &str,
        builder: &str,
        neg_risk: bool,
    ) -> PyResult<String> {
        let order = polymarket_order(
            salt,
            maker,
            signer,
            token_id,
            maker_amount,
            taker_amount,
            side,
            signature_type,
            timestamp,
            metadata,
            builder,
        )?;
        let signature = polymarket::sign(&self.key, &order, neg_risk).map_err(value_error)?;
        Ok(hex0x(&signature))
    }

    /// The Opinion order's signature, `0x`-hex, over the market's exchange.
    #[allow(clippy::too_many_arguments)]
    fn opinion_sign_order(
        &self,
        salt: &str,
        maker: &str,
        signer: &str,
        taker: &str,
        token_id: &str,
        maker_amount: &str,
        taker_amount: &str,
        expiration: &str,
        nonce: &str,
        fee_rate_bps: &str,
        side: u8,
        signature_type: u8,
        exchange: &str,
    ) -> PyResult<String> {
        let any = |text: &str| signing::address_any_case(text).map_err(value_error);
        let order = opinion::Order {
            salt: word(salt)?,
            maker: any(maker)?,
            signer: any(signer)?,
            taker: any(taker)?,
            token_id: word(token_id)?,
            maker_amount: word(maker_amount)?,
            taker_amount: word(taker_amount)?,
            expiration: word(expiration)?,
            nonce: word(nonce)?,
            fee_rate_bps: word(fee_rate_bps)?,
            side,
            signature_type,
        };
        let signature = opinion::sign(&self.key, &order, &any(exchange)?).map_err(value_error)?;
        Ok(hex0x(&signature))
    }

    fn __repr__(&self) -> String {
        format!("Secp256k1Signer(address={})", self.address())
    }
}
