//! A fault-tolerant MapReduce (lite) system.
//!
//! Users can specify map and reduce tasks, and then distribute
//! those tasks to workers running on a MapReduce cluster. For simplicity, data
//! is kept on an S3-compatible system, unlike Hadoop or GFS.

use bytes::Bytes;
use std::hash::Hasher;

pub mod codec;
pub mod standalone;
pub mod utils;
pub mod workload;
pub mod cmd;
pub mod S3;
pub mod Encode;

/////////////////////////////////////////////////////////////////////////////
// MapReduce application types
/////////////////////////////////////////////////////////////////////////////

/// The output of an application map function.
///
/// There are 2 layers of [`anyhow::Result`]s here. The outer layer
/// accounts for errors that arise while creating the iterator.
/// The inner layer accounts for errors that occur during iteration.
///
/// This accomodates both batch (all keys emitted at once) and lazy
/// (keys only emitted when the iterator is consumed) map operations.
pub type MapOutput = anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<KeyValue>>>>;

/// A map function takes a key-value pair and auxiliary arguments.
///
/// It returns an iterator that yields new key-value pairs.
pub type MapFn = fn(kv: KeyValue, aux: Bytes) -> MapOutput;

/// A reduce function takes in a key, an iterator over values for that key,
/// and an auxiliary argument. It returns an [`anyhow::Result`]
/// containing a single output value.
pub type ReduceFn = fn(
    key: Bytes,
    values: Box<dyn Iterator<Item = Bytes> + '_>,
    aux: Bytes,
) -> anyhow::Result<Bytes>;

/// A map reduce application.
#[derive(Copy, Clone)]
pub struct Workload {
    pub map_fn: MapFn,
    pub reduce_fn: ReduceFn,
}

/////////////////////////////////////////////////////////////////////////////
// Key-value pairs
/////////////////////////////////////////////////////////////////////////////

/// A single key-value pair.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct KeyValue {
    /// The key.
    pub key: Bytes,
    /// The value.
    pub value: Bytes,
}

impl KeyValue {
    /// Construct a new key-value pair from the given key and value.
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Get the key of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn key(&self) -> Bytes {
        self.key.clone()
    }

    /// Get the value of this key-value pair.
    ///
    /// This method is cheap, since [`Bytes`] are cheaply cloneable.
    #[inline]
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    /// Consumes the key-value pair and returns the key.
    #[inline]
    pub fn into_key(self) -> Bytes {
        self.key
    }

    /// Consumes the key-value pair and returns the value.
    #[inline]
    pub fn into_value(self) -> Bytes {
        self.value
    }
}

/// Hashes an intermediate key. Compute a reduce bucket for a given key
/// by calculating `ihash(key) % n_reduce`.
pub fn ihash(key: &[u8]) -> u32 {
    let mut hasher = fnv::FnvHasher::with_key(0);
    hasher.write(key);
    let value = hasher.finish() & 0x7fffffff;
    u32::try_from(value).expect("Failed to compute ihash of value")
}
