#![ allow(warnings)]
use clap::Parser;
use tonic::transport::Channel;
use tonic::Request;
use mrlite::cmd::ctl::{Args, Commands};

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_client::CoordinatorClient;
use mapreduce::{Empty, JobListRequest, JobRequest, Status as SystemStatus, Task};

fn display_jobs(jobs: Vec<Task>, show: &str) {
    let n = jobs.len();
    if n == 0 {
        println!("No jobs in {} job list", show);
        return;
    }
    let mut ctr = 0;
    for job in jobs {
        println!("[{}]\tSTATUS: [{:?}]\tIN: [{}]\tOUT: [{}]\tWORKLOAD: [{}]", ctr, job.status, job.input, job.output, job.workload);
        ctr+=1;
    }
}

fn display_system_status(sys_stat: SystemStatus) {
    let n_workers = sys_stat.worker_count;
    let mut active_count = 0;
    let mut dead_count = 0;
    println!("---------- WORKER STATUS ----------");
    for worker in sys_stat.workers {
        println!("[{}]\tState: {}", worker.address, worker.state);
        if worker.state == format!("Idle") || worker.state == format!("Busy") {
            active_count += 1;
        } else {
            dead_count += 1;
        }
    }
    // Ideally we want some health stats here like how many idle/busy workers out of n_workers
    println!("-----------------------------------");
    println!("System health:\t{}%", (active_count as f32/n_workers as f32)*100 as f32);
    println!("Active Workers:\t{active_count} / {n_workers}");
    println!("Dead Workers:\t{dead_count} / {n_workers}");
    println!("-----------------------------------");

    match sys_stat.jobs.get(0) {
        Some(job) => {
            println!("System is currently working on:");
            println!("[CURRENT JOB]\tSTATUS: [{:?}]\tIN: [{}]\tOUT: [{}]\tWORKLOAD: [{}]", job.status, job.input, job.output, job.workload);
        },
        None => {
            println!("System is currently idle -- no jobs queued");
            println!("-----------------------------------");
            return;
        }
    }
    println!("-----------------------------------");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let host = match args.host {
        Some(h) => h,
        None => format!("127.0.0.1:50051") // If the host is not specified, assume read from config file na kub (future)
    };

    let mut client = CoordinatorClient::connect(format!("http://{}", host)).await?;

    match args.command {
        Commands::Submit { input, workload, output , args} => {
            let request = Request::new(JobRequest {
                input,
                workload,
                output,
                args: "".to_string(),
            });

            let response = client.submit_job(request).await?;
            println!("Submitted job: {:?}", response);
        },
        Commands::Jobs {show} => {
            let show_req = match show {
                Some(s) => match s {
                    s if s == format!("all") || s == format!("a") => s,
                    s if s == format!("complete") || s == format!("c") => s,
                    s if s == format!("default") || s == format!("d") => s,
                    _ => format!("default")
                }
                None => format!("default")
            };
            // println!("{}", show_req);
            let response = client.list_jobs(Request::new(JobListRequest {show: show_req.clone()})).await?;
            display_jobs(response.into_inner().jobs, &show_req);
            // println!("Job list: {:?}", response); 
        },
        Commands::Status {} => {
            let response = client.system_status(Request::new(Empty {})).await?;
            // println!("System status: {:?}", response);
            
            display_system_status(response.into_inner());
            // let system_status: SystemStatus = response.into_inner();
            // println!("Worker Count: {}", system_status.worker_count);
            // for worker in system_status.workers {
            //     println!("Worker Address: {}, State: {}", worker.address, worker.state);
            // }
            // println!("Jobs: {:?}", system_status.jobs);
        },
    }

    Ok(())
}
