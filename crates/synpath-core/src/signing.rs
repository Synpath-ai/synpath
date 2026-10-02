//! EIP-712 order hashing and secp256k1 signing for Polymarket and Opinion.
//!
//! The Rust twin of `synpath.trading.polymarket_signing` and
//! `opinion_signing`, for the two structs those venues sign on every order.
//! The typed data is fixed, so it is encoded by hand rather than through a
//! general EIP-712 encoder: the domain separator, the struct hash and the
//! `0x1901` digest, exactly as `eth_account.encode_typed_data` builds them.
//! Signatures are RFC 6979 deterministic, low-s, with `v` as 27 or 28, as
//! `eth_account` produces them; the tests pin both against the venues' own
//! signing vectors.
//!
//! Every input is checked as the Python encoder checks it, and anything it
//! would refuse is an `Err` here, so the caller can hand the same input to
//! the Python path and get its error.

use k256::ecdsa::SigningKey;
use sha3::{Digest, Keccak256};

pub type Result<T> = std::result::Result<T, String>;

pub type Word = [u8; 32];

pub fn keccak(data: &[u8]) -> Word {
    Keccak256::digest(data).into()
}

/// A `uint256` from its decimal digits.
pub fn uint(decimal: &str) -> Result<Word> {
    let digits = decimal.trim();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("not a uint256: {decimal:?}"));
    }
    let mut word = [0u8; 32];
    for digit in digits.bytes() {
        let mut carry = u32::from(digit - b'0');
        for byte in word.iter_mut().rev() {
            let value = u32::from(*byte) * 10 + carry;
            *byte = value as u8;
            carry = value >> 8;
        }
        if carry != 0 {
            return Err(format!("does not fit in uint256: {decimal}"));
        }
    }
    Ok(word)
}

pub fn uint_u64(value: u64) -> Word {
    let mut word = [0u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

/// A 20-byte address, left-padded to a word. Mixed case must be a valid
/// EIP-55 checksum, as the Python encoder requires; all lower or all upper
/// case is taken as is.
pub fn address(text: &str) -> Result<Word> {
    let hex_part = text
        .strip_prefix("0x")
        .ok_or_else(|| format!("not an address: {text:?}"))?;
    if hex_part.len() != 40 {
        return Err(format!("not an address: {text:?}"));
    }
    let raw = hex::decode(hex_part).map_err(|_| format!("not an address: {text:?}"))?;
    let has_lower = hex_part.bytes().any(|b| b.is_ascii_lowercase());
    let has_upper = hex_part.bytes().any(|b| b.is_ascii_uppercase());
    if has_lower && has_upper && checksummed(&raw) != format!("0x{hex_part}") {
        return Err(format!("not a checksummed address: {text:?}"));
    }
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(&raw);
    Ok(word)
}

/// An address in any case, with or without `0x`: what `to_checksum_address`
/// accepts before the Opinion order is encoded.
pub fn address_any_case(text: &str) -> Result<Word> {
    let hex_part = text.strip_prefix("0x").unwrap_or(text);
    if hex_part.len() != 40 {
        return Err(format!("not an address: {text:?}"));
    }
    let raw = hex::decode(hex_part).map_err(|_| format!("not an address: {text:?}"))?;
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(&raw);
    Ok(word)
}

/// The EIP-55 checksummed form of a 20-byte address.
pub fn checksummed(raw: &[u8]) -> String {
    let lower = hex::encode(raw);
    let hash = keccak(lower.as_bytes());
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in lower.chars().enumerate() {
        let nibble = (hash[i / 2] >> (if i % 2 == 0 { 4 } else { 0 })) & 0x0f;
        out.push(if c.is_ascii_alphabetic() && nibble >= 8 {
            c.to_ascii_uppercase()
        } else {
            c
        });
    }
    out
}

/// `bytes.fromhex(value.removeprefix("0x").zfill(64))`, which must be 32 bytes.
pub fn bytes32(text: &str) -> Result<Word> {
    let hex_part = text.strip_prefix("0x").unwrap_or(text);
    if hex_part.len() > 64 {
        return Err(format!("not a bytes32: {text:?}"));
    }
    let padded = format!("{hex_part:0>64}");
    let raw = hex::decode(&padded).map_err(|_| format!("not a bytes32: {text:?}"))?;
    let mut word = [0u8; 32];
    word.copy_from_slice(&raw);
    Ok(word)
}

fn hash_words(words: &[Word]) -> Word {
    let mut hasher = Keccak256::new();
    for word in words {
        hasher.update(word);
    }
    hasher.finalize().into()
}

const DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

/// The EIP-712 domain separator for `(name, version, chainId, verifyingContract)`.
pub fn domain_separator(name: &str, version: &str, chain_id: u64, contract: &Word) -> Word {
    hash_words(&[
        keccak(DOMAIN_TYPE.as_bytes()),
        keccak(name.as_bytes()),
        keccak(version.as_bytes()),
        uint_u64(chain_id),
        *contract,
    ])
}

/// `keccak256(0x1901 || domainSeparator || structHash)`.
pub fn typed_digest(separator: &Word, struct_hash: &Word) -> Word {
    let mut data = Vec::with_capacity(66);
    data.extend_from_slice(b"\x19\x01");
    data.extend_from_slice(separator);
    data.extend_from_slice(struct_hash);
    keccak(&data)
}

// ---------------------------------------------------------------------------
// Polymarket CLOB V2
// ---------------------------------------------------------------------------

pub mod polymarket {
    use super::*;

    pub const CHAIN_ID: u64 = 137;
    pub const DOMAIN_NAME: &str = "Polymarket CTF Exchange";
    pub const DOMAIN_VERSION: &str = "2";
    pub const EXCHANGE: &str = "0xE111180000d2663C0091e4f400237545B87B996B";
    pub const NEG_RISK_EXCHANGE: &str = "0xe2222d279d744050d28e00520010520000310F59";
    pub const DEPOSIT_WALLET: u8 = 3;
    pub const ORDER_TYPE: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,\
        uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,\
        uint256 timestamp,bytes32 metadata,bytes32 builder)";

    /// The order's fields as the typed data carries them.
    pub struct Order {
        pub salt: Word,
        pub maker: Word,
        pub signer: Word,
        pub token_id: Word,
        pub maker_amount: Word,
        pub taker_amount: Word,
        pub side: u8,
        pub signature_type: u8,
        pub timestamp: Word,
        pub metadata: Word,
        pub builder: Word,
    }

    pub fn exchange(neg_risk: bool) -> Word {
        address(if neg_risk {
            NEG_RISK_EXCHANGE
        } else {
            EXCHANGE
        })
        .expect("constant address")
    }

    pub fn separator(neg_risk: bool) -> Word {
        domain_separator(DOMAIN_NAME, DOMAIN_VERSION, CHAIN_ID, &exchange(neg_risk))
    }

    pub fn struct_hash(order: &Order) -> Word {
        hash_words(&[
            keccak(ORDER_TYPE.as_bytes()),
            order.salt,
            order.maker,
            order.signer,
            order.token_id,
            order.maker_amount,
            order.taker_amount,
            uint_u64(u64::from(order.side)),
            uint_u64(u64::from(order.signature_type)),
            order.timestamp,
            order.metadata,
            order.builder,
        ])
    }

    /// The order's EIP-712 digest: also the id the CLOB gives it.
    pub fn order_hash(order: &Order, neg_risk: bool) -> Word {
        typed_digest(&separator(neg_risk), &struct_hash(order))
    }

    /// The signature for the order's wallet type: the digest signed directly
    /// (EOA, proxy, Safe), or for a Deposit Wallet the ERC-7739 wrapping of
    /// Solady's `TypedDataSign` envelope.
    pub fn sign(key: &SigningKey, order: &Order, neg_risk: bool) -> Result<Vec<u8>> {
        if order.signature_type != DEPOSIT_WALLET {
            return super::sign_digest(key, &order_hash(order, neg_risk)).map(|sig| sig.to_vec());
        }
        let separator = separator(neg_risk);
        let contents = struct_hash(order);
        let envelope_type = format!(
            "TypedDataSign(Order contents,string name,string version,uint256 chainId,\
             address verifyingContract,bytes32 salt){ORDER_TYPE}"
        );
        let envelope = hash_words(&[
            keccak(envelope_type.as_bytes()),
            contents,
            keccak(b"DepositWallet"),
            keccak(b"1"),
            uint_u64(CHAIN_ID),
            order.signer,
            [0u8; 32],
        ]);
        let inner = super::sign_digest(key, &typed_digest(&separator, &envelope))?;
        let mut out = Vec::with_capacity(65 + 64 + ORDER_TYPE.len() + 2);
        out.extend_from_slice(&inner);
        out.extend_from_slice(&separator);
        out.extend_from_slice(&contents);
        out.extend_from_slice(ORDER_TYPE.as_bytes());
        out.extend_from_slice(&(ORDER_TYPE.len() as u16).to_be_bytes());
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Opinion (the V1 CTF order on BNB Chain)
// ---------------------------------------------------------------------------

pub mod opinion {
    use super::*;

    pub const CHAIN_ID: u64 = 56;
    pub const DOMAIN_NAME: &str = "OPINION CTF Exchange";
    pub const DOMAIN_VERSION: &str = "1";
    pub const ORDER_TYPE: &str = "Order(uint256 salt,address maker,address signer,address taker,\
        uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,\
        uint256 feeRateBps,uint8 side,uint8 signatureType)";

    pub struct Order {
        pub salt: Word,
        pub maker: Word,
        pub signer: Word,
        pub taker: Word,
        pub token_id: Word,
        pub maker_amount: Word,
        pub taker_amount: Word,
        pub expiration: Word,
        pub nonce: Word,
        pub fee_rate_bps: Word,
        pub side: u8,
        pub signature_type: u8,
    }

    pub fn order_hash(order: &Order, exchange: &Word) -> Word {
        let struct_hash = hash_words(&[
            keccak(ORDER_TYPE.as_bytes()),
            order.salt,
            order.maker,
            order.signer,
            order.taker,
            order.token_id,
            order.maker_amount,
            order.taker_amount,
            order.expiration,
            order.nonce,
            order.fee_rate_bps,
            uint_u64(u64::from(order.side)),
            uint_u64(u64::from(order.signature_type)),
        ]);
        typed_digest(
            &domain_separator(DOMAIN_NAME, DOMAIN_VERSION, CHAIN_ID, exchange),
            &struct_hash,
        )
    }

    pub fn sign(key: &SigningKey, order: &Order, exchange: &Word) -> Result<[u8; 65]> {
        super::sign_digest(key, &order_hash(order, exchange))
    }
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// A private key from its hex (with or without `0x`).
pub fn signing_key(hex_key: &str) -> Result<SigningKey> {
    let raw = hex::decode(hex_key.trim().trim_start_matches("0x"))
        .map_err(|_| "not a hex private key".to_string())?;
    SigningKey::from_slice(&raw).map_err(|_| "not a valid secp256k1 private key".to_string())
}

/// The key's Ethereum address, checksummed.
pub fn address_of(key: &SigningKey) -> String {
    let point = key.verifying_key().to_sec1_point(false);
    let hash = keccak(&point.as_bytes()[1..]);
    checksummed(&hash[12..])
}

/// `r || s || v` over a 32-byte digest, `v` being 27 or 28.
pub fn sign_digest(key: &SigningKey, digest: &Word) -> Result<[u8; 65]> {
    let (signature, recovery) = key.sign_prehash_recoverable(digest);
    let mut out = [0u8; 65];
    out[..64].copy_from_slice(&signature.to_bytes());
    out[64] = 27 + recovery.to_byte();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hardhat's first account: a public test key.
    const KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    fn the_key_has_its_known_address() {
        let key = signing_key(KEY).unwrap();
        assert_eq!(
            address_of(&key),
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
        );
    }

    #[test]
    fn uints_and_addresses_encode_as_words() {
        assert_eq!(uint("256").unwrap()[30..], [1, 0]);
        assert!(uint(&"9".repeat(80)).is_err());
        assert!(uint("-1").is_err());
        assert!(address("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266").is_ok());
        assert!(address("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266").is_ok());
        assert!(address("0xF39fd6e51aad88F6F4ce6aB8827279cffFb92266").is_err());
        assert_eq!(bytes32("0x01").unwrap()[31], 1);
    }

    #[test]
    fn the_signature_recovers_to_the_signer() {
        let key = signing_key(KEY).unwrap();
        let digest = keccak(b"synpath");
        let sig = sign_digest(&key, &digest).unwrap();
        let signature = k256::ecdsa::Signature::from_slice(&sig[..64]).unwrap();
        let recovery = k256::ecdsa::RecoveryId::from_byte(sig[64] - 27).unwrap();
        let recovered =
            k256::ecdsa::VerifyingKey::recover_from_prehash(&digest, &signature, recovery).unwrap();
        assert_eq!(&recovered, key.verifying_key());
        assert!(signature.normalize_s() == signature, "low s");
    }
}
