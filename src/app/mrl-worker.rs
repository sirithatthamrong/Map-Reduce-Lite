use itertools::Itertools;
use mrlite::*;
use bytes::Bytes;
use clap::Parser;
use cmd::worker::Args;
use tokio::time::sleep;
use tonic::Request;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use aws_sdk_s3::Client;
use S3::minio;
use standalone::Job;
use dashmap::DashMap;
use uuid::Uuid; 
use std::fs;

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::{WorkerRegistration, WorkerReport, WorkerRequest, WorkerCountRequest};
use mapreduce::coordinator_client::CoordinatorClient;
use mrlite::Encode::encode_decode;
use mrlite::Encode::encode_decode::key_value_list_to_key_listand_value_list;
use mrlite::S3::minio::upload_parts;

async fn get_number_of_workers(coordinator_client: &mut CoordinatorClient<tonic::transport::Channel>) -> Result<i32, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(WorkerCountRequest {});
    let response = coordinator_client.get_worker_count(request).await?;
    Ok(response.into_inner().count)
}

type BucketIndex = u32;
type Buckets = DashMap<BucketIndex, Vec<KeyValue>>;

async fn map(
    client: &Client,
    job: &Job,
    num_reduce_worker: u32
)-> Result<Vec<String>, anyhow::Error> {

    let buckets: Buckets = Buckets::new();

    let engine = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let s3_bucket_name = "mrl-lite";
    let s3_object_name = &job.input.clone();
    let (_, exact_name) = s3_object_name.rsplit_once('/').unwrap();
    println!("{exact_name}");
    let content = minio::get_object(&client, s3_bucket_name, s3_object_name).await?;
    let _: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    let serialized_args = Bytes::from(job.args.join(" "));
    
    let filename = s3_object_name.clone();
    // let mut buf = Vec::new();
    let buf = Bytes::from(content);
    let input_kv = KeyValue {
        key: Bytes::from(filename),
        value: buf,
    };
    // println!("{:?}", input_kv);
    let map_func = engine.map_fn;

    let mut bucket_paths: HashSet<String> = HashSet::new();
    for item in map_func(input_kv, serialized_args.clone())? {
        let KeyValue { key, value } = item?;
        let bucket_no = ihash(&key) % num_reduce_worker;
        let path = format!(".{}{}", job.output, bucket_no);
        let _ = fs::create_dir_all(path)?;
        bucket_paths.insert(format!("{}{}", job.output, bucket_no));
        #[allow(clippy::unwrap_or_default)]
        buckets
            .entry(bucket_no)
            .or_insert(Vec::new())
            .push(KeyValue { key, value });
    }
    // println!("Buckets: {:?}", buckets);
    //Now under bucket subdirectory we upload the intermediate data from this worker
   
    for (bucket_no, key_values) in buckets.into_iter() {
        // println!("Bucket [{bucket_no}]");
        let filename = exact_name;
        let object_name = format!("{}{}/{}", &job.output,bucket_no, filename);
        let (keys, values): (Vec<Bytes>, Vec<Bytes>) = key_value_list_to_key_listand_value_list(key_values.clone());
        //cheese method cuz has to be the same name
        encode_decode::write_parquet(&format!(".{}", object_name), keys, values);
        upload_parts(&client, s3_bucket_name, &object_name).await.unwrap();
        fs::remove_file(format!(".{}{}/{}", &job.output,bucket_no, filename))?;
    }

    //clean local files

    //you can merge files with this
    // merge_files_under_prefix_and_cleanup(client: &Client, bucket: &str, prefix: &str, output_file: &str)
    // the prefix should be like temp/{bucketname} output_file should be like temp/{bucketname}.parquet


    Ok(bucket_paths.into_iter().collect_vec())
}

async fn reduce(client: &Client, job: &Job) -> Result<String, anyhow::Error> {
    let engine: Workload = workload::try_named(&job.workload.clone()).expect("Error loading workload");
    let bucket_name = "mrl-lite";
    let job_input = job.input.clone();
    let mut input = job_input.chars();
    input.next();
    let object_name = input.as_str();
    let (_, exact_name) = object_name.rsplit_once('/').unwrap();
    println!("{exact_name}");

    let files_in_bucket = minio::list_files_with_prefix(&client, &bucket_name, &object_name).await.unwrap();
    println!("{:?}", files_in_bucket);
    // let combined_input_fn = format!("{}/merged", &object_name);
    // minio::merge_files_under_prefix_and_cleanup(&client, &bucket_name, &object_name, &combined_input_fn).await;

    // Fetch intermediate data from MinIO
    let mut key_value_vec: Vec<(Bytes, Bytes)> = Vec::new();
    for file in &files_in_bucket {
        let temp_filename = Uuid::new_v4().to_string();
        let _ = minio::download_file(&client, &bucket_name, &file,&temp_filename.clone()).await?;

        let (keys, values) = encode_decode::read_parquet(&temp_filename.clone());

        let mut keys_values: Vec<(Bytes, Bytes)> = keys.into_iter().zip(values.into_iter()).collect();
        fs::remove_file(temp_filename.clone()).expect("Failed to remove temp file");
        key_value_vec.append(&mut keys_values);
    }

    // Intermediate data storage
    let mut intermediate_data = HashMap::new();

    for (key, value) in key_value_vec {
        intermediate_data.entry(key).or_insert_with(Vec::new).push(value);
    }

    // Sort intermediate data by key
    let mut sorted_intermediate_data: Vec<(Bytes, Vec<Bytes>)> = intermediate_data.into_iter().collect();
    sorted_intermediate_data.sort_by(|a, b| a.0.cmp(&b.0));

    // Reduce intermediate data
    let mut output_data = Vec::new();
    //Map reduce parse addition args
    let serialized_args = Bytes::from(job.args.join(" "));
    let reduce_func = engine.reduce_fn;


    for (key, values) in sorted_intermediate_data {
        let value_iter = Box::new(values.into_iter());
        let reduced_value = reduce_func(key.clone(), value_iter, serialized_args.clone())?;
        output_data.push(KeyValue { key: key.clone(), value: reduced_value });
    }

    // Prepare and upload the final output
    let filename = format!("mr-out-{exact_name}");
    let mut content = String::new();
    let output_file = format!("{}{}", job.output, filename);

    for kv in &output_data {
        let _ = utils::string_from_bytes(kv.key.clone());
        let value = utils::string_from_bytes(kv.value.clone());
        let formatted = format!("{}", value.unwrap());
        content.push_str(formatted.as_str());
    }

    match minio::upload_string(&client, bucket_name, &output_file, &content).await {
        Ok(_) => println!("Uploaded to {}",output_file),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }
    Ok(filename.to_string())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    print!("Hello worker!\n");

    // Parse command line arguments
    let args = Args::parse();
    let ip = args.join; 
    print!("IP to join: {}", ip);

    // Connect to coordinator
    let mut client = CoordinatorClient::connect(format!("http://{}", ip)).await.unwrap();

    // Register with coordinator
    let request = Request::new(WorkerRegistration { });
    let response = client.register_worker(request).await?;
    println!("Worker registered: {:?} \n", response);

    // Get S3 args
    let s3_args = response.into_inner();
    let s3_ip = format!("http://{}", s3_args.args.get("ip").unwrap().clone());
    let s3_user = s3_args.args.get("user").unwrap().clone();
    let s3_pw = s3_args.args.get("pw").unwrap().clone();
    // println!("s3: {} {} {}\n", s3_ip, s3_user, s3_pw);

    // Initialize S3 client
    let s3_client = minio::get_min_io_client(s3_ip.clone(), s3_user.clone(), s3_pw.clone()).await?;  

    // Main loop to receive and process tasks
    loop {
        println!("Waiting for tasks...");
        match client.get_task(Request::new(WorkerRequest {})).await {
            Ok(response) => {
                let task = response.into_inner();
                println!("Received task: {:?}", task);

                let job = Job {
                    input: task.input.clone(),
                    workload: task.workload.clone(),
                    output: task.output.clone(),
                    args: task.args.clone().split_whitespace().map(String::from).collect(),
                };

                let num_workers = get_number_of_workers(&mut client).await.unwrap() as u32;
                println!("{num_workers}");
                // Process task based on its type
                let out_fn = match task.status.as_str() {
                    "Map" => {
                        match map(&s3_client, &job, num_workers).await {
                            Ok(paths) => Some(paths),
                            Err(err) => {
                                eprintln!("Error during map task: {:?}", err);
                                None
                            }
                        }
                    }
                    "Reduce" => {
                        match reduce(&s3_client, &job).await {
                            Ok(name) => Some(vec![name]),
                            Err(err) => {
                                eprintln!("Error during reduce task: {:?}", err);
                                None
                            }
                        }
                    }
                    _ => {
                        eprintln!("Invalid task status received: {}", task.status);
                        None
                    }
                };

                if out_fn.is_some() {
                    // Report task completion to coordinator
                    let report = Request::new(WorkerReport {
                        task: task.status,
                        input: task.input,
                        output: out_fn.unwrap(),
                    });
                    if let Err(err) = client.report_task(report).await {
                        eprintln!("Error reporting task completion: {:?}", err);
                    }
                }
            }
            Err(status) => {
                eprintln!("Error receiving task: {:?}", status);
                sleep(Duration::from_secs(1)).await; // Sleep before retrying
            }
        }

        // Sleep for a short period before checking for the next task
        sleep(Duration::from_secs(1)).await;
    }
}

/* 
S3 Client examples


    // Lists the buckets 
    let resp = match s3_client.list_buckets().send().await {
        Ok(resp) =>  println!("{:?}", resp),
        Err(e) => eprintln!("Failed to list buckets: {:?}", e),
    };  
    // println!("bucket accessible: {}", minio::is_bucket_accessible(s3_client.clone(), bucket_name.to_string()).await.unwrap());

    // Get object
    let bucket_name = "mrl-lite";
    let object_name = "/testcases/graph-edges/00.txt";
    match minio::get_object(&s3_client, bucket_name, object_name).await {
        Ok(content) => println!("{:?}", content),
        Err(e) => eprintln!("Failed to get object: {:?}", e),
    }

    match minio::list_files_with_prefix(&s3_client, bucket_name, "testcases").await {
        Ok(content) => println!("{:?}", content),
        Err(e) => eprintln!("Failed!!! {:?}", e),
    }

    // //Write object
    match minio::upload_string(&s3_client, bucket_name, "write_test.txt", "Mother wouldst Thou truly Lordship sanction\n in one so bereft of light?").await {
        Ok(_) => println!("Uploaded"),
        Err(e) => eprintln!("Failed to upload: {:?}", e),
    }

    //Delete object s3 doesnt return error for some reason lol
    match minio::delete_object(&s3_client, bucket_name, "write_test.txt").await {
        Ok(_) => println!("Deleted"),
        Err(e) => eprintln!("Failed to delete: {:?}", e),
    }

*/