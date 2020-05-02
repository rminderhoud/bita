use blake2::Digest;
use blake3;

use crate::HashSum;

#[derive(Debug, Clone, Copy)]
pub enum HashFunction {
    Blake2,
    Blake3,
}

impl std::fmt::Display for HashFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Blake2 => "Blake2",
                Self::Blake3 => "Blake3",
            }
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HasherBuilder {
    pub hash_length: usize,
    pub function: HashFunction,
}
impl HasherBuilder {
    pub fn build(self) -> Hasher {
        match self.function {
            HashFunction::Blake2 => Hasher::Blake2 {
                hash_length: self.hash_length,
                hasher: blake2::Blake2b::new(),
            },
            HashFunction::Blake3 => Hasher::Blake3 {
                hash_length: self.hash_length,
                hasher: blake3::Hasher::new(),
            },
        }
    }
}

pub enum Hasher {
    Blake2 {
        hash_length: usize,
        hasher: blake2::Blake2b,
    },
    Blake3 {
        hash_length: usize,
        hasher: blake3::Hasher,
    },
}

impl Hasher {
    pub fn input(&mut self, data: &[u8]) {
        match self {
            Self::Blake2 { hasher, .. } => {
                hasher.input(data);
            }
            Self::Blake3 { hasher, .. } => {
                hasher.input(data);
            }
        }
    }
    pub fn finilize(self) -> HashSum {
        match self {
            Self::Blake2 {
                hasher,
                hash_length,
            } => HashSum::from_slice(&hasher.result()[..hash_length]),
            Self::Blake3 {
                hasher,
                hash_length,
            } => {
                let mut block = vec![0; hash_length];
                let mut output = hasher.finalize_xof();
                output.fill(&mut block);
                HashSum::from_vec(block)
            }
        }
    }
    pub fn hash_sum(mut self, data: &[u8]) -> HashSum {
        self.input(data);
        self.finilize()
    }
}
