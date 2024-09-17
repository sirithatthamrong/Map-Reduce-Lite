#![allow(warnings)]
use std::fs::{self, File, OpenOptions};
use std::sync::Arc;
use parquet::arrow::ArrowWriter;
use arrow::array::{ArrayRef, Array};

use arrow::array::BinaryArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use bytes::Bytes;

use crate::cmd::coordinator::now;
use crate::KeyValue;

// fn main() {
//         let key = vec![Bytes::from("ball"), Bytes::from("bat"), Bytes::from("glove"), Bytes::from("glove")];
//         let value = vec![Bytes::from("ayo huh"), Bytes::from("ayo 121huh"), Bytes::from("ayuh"), Bytes::from("ayh")];
//         let key2 = vec![Bytes::from("O promised consort"), Bytes::from("let"), Bytes::from("us"), Bytes::from("go")];
//         let value2 = vec![Bytes::from("If you grieve"), Bytes::from("for this world"), Bytes::from("yield the path"), Bytes::from("forward to us")];
//         write_parquet("output.parquet", key, value);
//         write_parquet("output2.parquet", key2, value2);
//         let res = read_parquet("output.parquet");
//         let res2 = read_parquet("output2.parquet");
//         combine_parquets(vec!["output.parquet", "output2.parquet"], "output_combined.parquet");
//         let res3 = read_parquet("output_combined.parquet");
//         let res4 = batch_reading_parquet("output_combined.parquet", 2, 20);
//         // println!("Read from parquet: {:?}", res);
//         // println!("Read from parquet: {:?}", res2);
//         println!("Read from parquet: {:?}", res3);
//         println!("Read from parquet: {:?}", res4);
//
//
// }





// Will return from start to start + batch_size, if batch size exceeds the number of rows, it will return the rest of the rows
fn batch_reading_parquet(filename: &str, start: usize, batch_size: usize) -> (Vec<Bytes>, Vec<Bytes>){
        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mut reader = builder.build().unwrap();
        let mut ret_key = Vec::new();
        let mut ret_value = Vec::new();
        let record_batch = reader.next().unwrap().unwrap();
        let row_size = record_batch.num_rows();
        let actual_batch_size = if start + batch_size > row_size {
                row_size - start
        } else {
                batch_size
        };
        let key_arr_ref = record_batch.column(0).slice(start, actual_batch_size);
        let value_arr_ref = record_batch.column(1).slice(start, actual_batch_size);
        let key = arr_to_vec(key_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        let value = arr_to_vec(value_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        ret_key.extend(key);
        ret_value.extend(value);
        return (ret_key, ret_value);

}
pub fn write_parquet(filename:&str, key: Vec<Bytes>, value: Vec<Bytes>){
        let file = File::create(filename).unwrap();
        let key: Vec<&[u8]> = key.iter().map(|b| b.as_ref()).collect();
        let vals: Vec<&[u8]> = value.iter().map(|b| b.as_ref()).collect();
        let ids = BinaryArray::from(key);
        let vals = BinaryArray::from(vals);
        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);

        let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                        Arc::new(ids) as ArrayRef,
                        Arc::new(vals) as ArrayRef,
                ],
        ).unwrap();
        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        // println!("Schema is: {:?}", batch.schema());
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).expect("Writing batch");
        // writer must be closed to write footer
        writer.close().unwrap();
}

fn arr_to_vec(binary_array: &BinaryArray) -> Vec<Bytes> {
        let mut ret = Vec::new();
        for i in 0..binary_array.len() {
                let value = binary_array.value(i);
                let value_bytes = Bytes::copy_from_slice(value);
                ret.push(value_bytes);
        }
        return ret;
}
pub fn read_parquet(filename: &str) -> (Vec<Bytes>, Vec<Bytes>){
        println!("{}", filename);
        //Will explode if you have more than one collumn
        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        // println!("Converted arrow schema is: {}", builder.schema());
        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();
        // println!("Read {} records.", record_batch.num_rows());
        // Print out the content of the Parquet file
        let key_arr_ref = record_batch.column(0);
        let value_arr_ref = record_batch.column(1);
        let ret_key = arr_to_vec(key_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        let ret_value = arr_to_vec(value_arr_ref.as_any().downcast_ref::<BinaryArray>().unwrap());
        return (ret_key, ret_value) ;


}

pub fn combine_parquets(input_files: Vec<&str>, prefix: &str, output_file: &str) -> (){

        fs::create_dir_all(&format!(".{}", prefix)).unwrap();
        let filename = format!(".{}/{}", prefix, output_file);

        let mut file = OpenOptions::new().write(true).create(true).open(&filename).unwrap();
        // let file = File::create(output_file).unwrap();
        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);
        let batch = RecordBatch::new_empty(SchemaRef::from(schema));
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        for input in input_files{
                let (key, value) = read_parquet(input);
                let key: Vec<&[u8]> = key.iter().map(|b| b.as_ref()).collect();
                let vals: Vec<&[u8]> = value.iter().map(|b| b.as_ref()).collect();
                let ids = BinaryArray::from(key);
                let vals = BinaryArray::from(vals);
                let batch = RecordBatch::try_from_iter(vec![
                        ("id", Arc::new(ids) as ArrayRef),
                        ("val", Arc::new(vals) as ArrayRef),
                ]).unwrap();
                writer.write(&batch).expect("Writing batch");
        }

        writer.close().unwrap();
}



// When parsing big file you got some key value, then you can append to parquet
// let file = File::create("output.parquet").unwrap();
// Some loop{
        // getKeyValue
        // let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        // append_parquet
        // writer.close().unwrap();
// }
pub fn append_parquet(_file: &File, writer: &mut ArrowWriter<File>, key: Vec<Bytes>, value: Vec<Bytes>){
        let key: Vec<&[u8]> = key.iter().map(|b| b.as_ref()).collect();
        let vals: Vec<&[u8]> = value.iter().map(|b| b.as_ref()).collect();
        let ids = BinaryArray::from(key);
        let vals = BinaryArray::from(vals);
        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);
        let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                        Arc::new(ids) as ArrayRef,
                        Arc::new(vals) as ArrayRef,
                ],
        ).unwrap();
        // WriterProperties can be used to set Parquet file options
        let _props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        // println!("Schema is: {:?}", batch.schema());
        writer.write(&batch).expect("Writing batch");
        // writer must be closed to write footer

}
pub fn make_writer(file: & File) -> ArrowWriter<File>{
        let cloned_file = file.try_clone().unwrap();
        let fields = vec![
                Field::new("id", DataType::Binary, false),
                Field::new("val", DataType::Binary, false),
        ];
        let schema = Schema::new(fields);
        let batch = RecordBatch::new_empty(SchemaRef::from(schema));
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        return ArrowWriter::try_new(cloned_file, batch.schema(), Some(props)).unwrap();
}

pub fn key_value_list_to_key_listand_value_list(kv_list: Vec<KeyValue>) -> (Vec<Bytes>, Vec<Bytes>){
        let mut key_list = Vec::new();
        let mut value_list = Vec::new();
        for kv in kv_list{
                key_list.push(kv.key);
                value_list.push(kv.value);
        }
        return (key_list, value_list);
}