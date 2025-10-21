use sha2::{Digest, Sha256};

pub fn hash_file_name(key: &str) -> String {
    let hash = Sha256::digest(key);
    base16ct::lower::encode_string(&hash)
}
