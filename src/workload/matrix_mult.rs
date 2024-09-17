use std::collections::HashMap;

use crate::*;
use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};

// Map function for the first stage
pub fn map_stage_one(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let content = String::from_utf8(kv.value.to_vec())?;
    let mut map_output = Vec::new();

    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 4 {
            continue;
        }
        let row: i32 = parts[0].parse()?;
        let col: i32 = parts[1].parse()?;
        let value: f64 = parts[2].parse()?;
        let matrix_name = parts[3];

        if matrix_name == "A" {
            let key = format!("{}", col);
            let value = format!("A,{},{}", row, value);
            map_output.push(KeyValue {
                key: Bytes::from(key),
                value: Bytes::from(value),
            });
        } else if matrix_name == "B" {
            let key = format!("{}", row);
            let value = format!("B,{},{}", col, value);
            map_output.push(KeyValue {
                key: Bytes::from(key),
                value: Bytes::from(value),
            });
        }
    }

    Ok(Box::new(map_output.into_iter().map(Ok)))
}

// Reduce function for the first stage
pub fn reduce_stage_one(_key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>, _aux: Bytes) -> Result<Bytes> {
    let mut map_a = HashMap::new();
    let mut map_b = HashMap::new();

    for value in values {
        let value_str = String::from_utf8(value.to_vec())?;
        let parts: Vec<&str> = value_str.split(',').collect();
        if parts.len() != 3 {
            continue;
        }
        let matrix_name = parts[0];
        let index: i32 = parts[1].parse()?;
        let value: f64 = parts[2].parse()?;

        if matrix_name == "A" {
            map_a.insert(index, value);
        } else if matrix_name == "B" {
            map_b.insert(index, value);
        }
    }

    let mut output = BytesMut::new();
    for (i, a_value) in map_a.iter() {
        for (j, b_value) in map_b.iter() {
            let product = a_value * b_value;
            output.put(format!("{} {} {} C\n", i, j, product).as_bytes());
        }
    }

    Ok(output.freeze())
}

// Map function for the second stage
pub fn map_stage_two(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let content = String::from_utf8(kv.value.to_vec())?;
    let mut map_output = Vec::new();

    for line in content.lines() {
        println!("{}", line);
        let parts: Vec<&str> = line.split_whitespace().collect();
        println!("{:?}", parts);

        if parts.len() != 4 {
            continue;
        }

        let row = parts[0].to_string();
        let col = parts[1].to_string();
        let value = parts[2].to_string();

        let key = format!("{} {}", row, col);
        let kv = KeyValue {
            key: Bytes::from(key), 
            value: Bytes::from(value),
        };
        map_output.push(kv);
    }

    Ok(Box::new(map_output.into_iter().map(Ok)))
}

// Reduce function for the second stage
pub fn reduce_stage_two(_key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>, _aux: Bytes) -> Result<Bytes> {
    let mut sum = 0.0;

    let key = String::from_utf8_lossy(&_key).to_string();

    for value in values {
        let value_str = String::from_utf8(value.to_vec())?;
        let value: f64 = value_str.parse()?;
        sum += value;
    }

    let result = format!("{key} {} C\n", sum);
    Ok(Bytes::from(result))
}