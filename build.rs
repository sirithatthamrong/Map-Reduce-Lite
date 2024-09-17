use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // tonic_build::compile_protos("proto/mapreduce.proto").unwrap();
    let out_dir = env::var("OUT_DIR")?;
    let proto_file = "proto/mapreduce.proto";
        tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .out_dir(&out_dir)
            .compile(&[proto_file], &["proto"])?;
    Ok(())
}



