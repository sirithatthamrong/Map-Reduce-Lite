use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use glob::glob;
use itertools::Itertools;
use standalone::Job;
use std::{
    fs::File,
    io::{Read, Write},
};

use crate::*;

// types related to this engine
type BucketIndex = u32;
type Buckets = DashMap<BucketIndex, Vec<KeyValue>>;

pub fn perform_map(
    job: &Job,
    engine: &Workload,
    serialized_args: &Bytes,
    num_reduce_worker: u32,
) -> Result<Buckets> {
    // Iterator going through all files in the input file path, precisely, input/*
    let input_files = glob(&job.input)?;
    let buckets: Buckets = Buckets::new();
    for pathspec in input_files.flatten() {
        let mut buf = Vec::new();
        {
            // a scope so that the file is closed right after reading
            let mut file = File::open(&pathspec)?;
            // Reads the input file completely and stores in buf
            file.read_to_end(&mut buf)?;
        }
        // Converts to Bytes
        let buf = Bytes::from(buf);
        let filename = pathspec.to_str().unwrap_or("unknown").to_string();
        // Stores the data read from each file as <Filename, All data in file>
        let input_kv = KeyValue {
            key: Bytes::from(filename),
            value: buf,
        };
        let map_func = engine.map_fn;
        // For each <key, value> object that has been mapped by the map function,
        // create a KeyValue object, and insert the KeyValue object into a bucket
        // according to the hashed value (mod # workers)
        for item in map_func(input_kv, serialized_args.clone())? {
            let KeyValue { key, value } = item?;
            let bucket_no = ihash(&key) % num_reduce_worker;

            #[allow(clippy::unwrap_or_default)]
            buckets
                .entry(bucket_no)
                .or_insert(Vec::new())
                .push(KeyValue { key, value });
        }
    }

    Ok(buckets)
}

pub fn perform_reduce(
    job: &Job,
    engine: &Workload,
    serialized_args: &Bytes,
    _num_reduce_worker: u32,
    buckets: Buckets,
) -> Result<()> {
    let reduce_func = engine.reduce_fn;
    let output_dir = &job.output;
    // For each bucket and its contents, compute and create the output file (according to bucket id),
    // and sort the keys in the bucket in ascending order.
    for (reduce_id, mut bkt) in buckets.into_iter() {
        let out_pathspec = format!("{}/mr-out-{}", &output_dir, reduce_id);
        let mut out_file = File::create(&out_pathspec)?;
        bkt.sort_unstable_by_key(KeyValue::key);
        // Iterate through the values associated with each key and apply reduce function and write to file.
        for (key, value_group) in &bkt.into_iter().chunk_by(KeyValue::key) {
            let iter = value_group.map(KeyValue::into_value);
            let out = reduce_func(key.clone(), Box::new(iter), serialized_args.clone())?;
            out_file.write_all(&out)?;
        }
    }
    Ok(())
}
