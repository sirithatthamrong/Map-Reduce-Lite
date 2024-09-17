use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;
use aws_sdk_s3::Client;
// use anyhow::*;
// use bytes::Bytes;
use mrlite::*;
use clap::Parser;
use cmd::coordinator::{now, Args};
// use tokio::net::TcpListener;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod mapreduce {
    tonic::include_proto!("mapreduce");
}

use mapreduce::coordinator_server::{Coordinator, CoordinatorServer};
use mapreduce::{Empty, JobList, JobListRequest, JobRequest, JobResponse, Status as SystemStatus, Task, 
    Worker, WorkerRegistration, WorkerReport, WorkerRequest, WorkerResponse, WorkerCountRequest, WorkerCountResponse};
use mrlite::S3::minio::*;

/* 
    Only one coordinator !!
*/

// Only one job should be in either of the following states: `{MapPhase, Shuffle, ShufflePhase}``; 
// all other jobs should either be `Pending` or `Completed`.
#[derive(Debug, Clone, PartialEq)]
enum JobStatus {
    Pending,
    MapPhase,
    Shuffle,
    ReducePhase,
    Completed
} 

#[derive(Debug, Clone)]
struct FileStatus {
    status: JobStatus,
    elapsed: u128,
}

#[derive(Debug)]
struct FileStatusCounter {
    pending: usize,
    mapping: usize,
    shuffle: usize,
    reducing: usize,
    complete: usize
}
// Struct for a Job, which holds the status of a job
// and the assigned `standalone::Job`.
#[derive(Clone, Debug)]
struct Job {
    id: String,
    status: JobStatus,
    job: standalone::Job,
    files: Arc<Mutex<Vec<String>>>,
    file_status: Arc<Mutex<HashMap<String, FileStatus>>>,
}

// The default state for a worker node is `Idle`, meaning no work is assigned but the worker is alive.
// 
// A worker node is `busy` if it is currently operating a task, and `dead` if it has not responded
// to a status check (heartbeat).
#[derive(Debug, Clone)]
enum WorkerState {
    Idle,
    Busy,
    Dead
}

// Struct for a worker, which holds the state of a worker
// and the IP address of the worker to send RPC for communication.
#[derive(Debug, Clone)]
struct WorkerNode {
    state: WorkerState,
    addr: SocketAddr,
    /// Time since this Worker last checked in
    elapsed: u128,
}

// Creates a new Coordinator Service with the supplied arguments
impl CoordinatorService {
    fn new(ip: impl ToString, user: impl ToString, pw: impl ToString, client: Client, timeout: u64) -> Self {
        Self {
            job_queue: Arc::new(Mutex::new(VecDeque::new())),
            completed_jobs: Arc::new(Mutex::new(VecDeque::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            os_ip: ip.to_string(),
            os_user: user.to_string(),
            os_pw: pw.to_string(),
            s3_client: client,
            timeout: Duration::from_secs(timeout).as_nanos(),
        }
    }
}

// Struct for the coordinator, which holds the job queue and the worker list.
pub struct CoordinatorService {
    job_queue: Arc<Mutex<VecDeque<Job>>>,
    completed_jobs: Arc<Mutex<VecDeque<Job>>>,
    workers: Arc<Mutex<HashMap<SocketAddr, WorkerNode>>>,
    os_ip: String,
    os_user: String,
    os_pw: String,
    s3_client: Client,
    timeout: u128,
}

fn get_next_file(files: &Vec<String>, file_status: &HashMap<String, FileStatus>, timeout: u128) -> Option<String> {
    for file in files {
        let this_file_status = file_status.get(file).unwrap();
        match this_file_status.status {
            JobStatus::Pending => {
                return Some(file.clone());
            }
            JobStatus::MapPhase => {
                let elapsed = now() - this_file_status.elapsed;
                if (elapsed) > timeout {
                    println!("Gave a straggling task -- {}", file);
                    return Some(file.clone());
                }
            }
            JobStatus::Shuffle => {
                return Some(file.clone());
            }
            JobStatus::ReducePhase => {
                let elapsed = now() - this_file_status.elapsed;
                if (elapsed) > timeout {
                    println!("Gave a straggling task -- {}", file);
                    return Some(file.clone());
                }
            }
            JobStatus::Completed => {
                return None;
            }
        }
    }
    return None;
}

fn next_state(file: &String, file_status: &HashMap<String, FileStatus>) -> Option<JobStatus> {
    let completed = file_status.get(file);
    match completed {
        Some(f_s) => {
            if f_s.status.eq(&JobStatus::MapPhase) {
                Some(JobStatus::Shuffle)
            } else if f_s.status.eq(&JobStatus::ReducePhase) {
                Some(JobStatus::Completed)
            // } else if f_s.status.eq(&JobStatus::Shuffle) {
            //     Some(JobStatus::Shuffle)
            // } else if f_s.status.eq(&JobStatus::Completed) {
            //     Some(JobStatus::Completed)
            } 
            else {
                None
            }
        }
        None => {
            None
        }
    }
}

fn get_file_index(file: &str, files: &Vec<String>) -> usize {
    files.iter().position(|f| f==file).unwrap()
}

fn contains_path(path: &str, files: &Vec<String>) -> bool {
    files.contains(&path.to_string())
}
/// Used to check the status of a job and possibly inform whether or not 
/// the job's status can be changed to the next level.
fn check_all_file_states(files: &Vec<String>, file_status: &HashMap<String, FileStatus>) -> Option<JobStatus> {
    // if all files are in shuffle phase, then we change job status to shuffle
    let n_files = files.len();
    let mut status_counter: FileStatusCounter = FileStatusCounter {
        pending: 0,
        mapping: 0,
        shuffle: 0,
        reducing: 0,
        complete: 0
    };
    for file in files {
        match file_status.get(file) {
            Some(f) => {
                match f.status {
                    JobStatus::Pending => {
                        status_counter.pending += 1;
                    }
                    JobStatus::MapPhase => {
                        status_counter.mapping += 1;
                    }
                    JobStatus::Shuffle => {
                        status_counter.shuffle += 1;
                    }
                    JobStatus::ReducePhase => {
                        status_counter.reducing += 1;

                    }
                    JobStatus::Completed => {
                        status_counter.complete += 1;
                    }
                }
            }
            None => {
                // Should not reach here
                eprintln!("File & FileStatus mismatch");
            }
        }
    }
    println!("{:?}", status_counter);
    if status_counter.complete == n_files {
        Some(JobStatus::Completed)
    } else if status_counter.shuffle == n_files {
        Some(JobStatus::Shuffle)
    } else if status_counter.pending == n_files {
        Some(JobStatus::Pending)
    } else if (status_counter.shuffle > 0 && status_counter.complete == 0) && status_counter.mapping <= n_files {
        Some(JobStatus::MapPhase)
    } else if status_counter.mapping == 0 && (status_counter.shuffle > 0 || status_counter.reducing > 0) {
        Some(JobStatus::ReducePhase)
    } else {
        // Should not be reached unless I'm tripping
        None
    }
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    // Get the number of workers in the worker list
    async fn get_worker_count(&self, _request: Request<WorkerCountRequest>) -> Result<Response<WorkerCountResponse>, Status> {
        let workers = self.workers.lock().unwrap();
        let count = workers.len() as i32;
        Ok(Response::new(WorkerCountResponse { count }))
    }
    // Register a worker with the coordinator
    // This function is called when a worker node wants to register itself with the coordinator. 
    // When a worker starts up, it should send a registration request to the coordinator to announce its availability for task assignments
    async fn register_worker(&self, request: Request<WorkerRegistration>) -> Result<Response<WorkerResponse>, Status> {
        let worker_addr = request.remote_addr().ok_or_else(|| Status::unknown("No remote address"))?;
        let worker = WorkerNode {
            state: WorkerState::Idle,
            addr: worker_addr.clone(),
            elapsed: now(),
        };
        println!("New worker joined at {:?}", worker.addr.to_string());
        let mut args: HashMap<String, String> = HashMap::new();
        args.insert("ip".into(), self.os_ip.clone());
        args.insert("user".into(), self.os_user.clone());
        args.insert("pw".into(), self.os_pw.clone());
        let mut workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        workers.insert(worker_addr, worker);

        Ok(Response::new(WorkerResponse {
            success: true,
            message: "Worker registered".into(),
            args: args,
        }))
    }

    // Get a task from the job queue
    // and return the task to the worker
    // This function is called by a worker node when it requests a task from the coordinator. 
    // Once a worker is registered and ready to perform work, it will periodically request tasks from the coordinator to execute
    async fn get_task(&self, _request: Request<WorkerRequest>) -> Result<Response<Task>, Status> {
        // Gets the next job in the job queue.
        let next_job: Option<Job> = {
            let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());
            let next_job = job_q.pop_front();
            if next_job.is_some() {
                let job = next_job.clone().unwrap();
                job_q.push_front(job);
                next_job.clone()
            } else {
                None
            }
        };
        // let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());
        let worker_addr = _request.remote_addr().ok_or_else(|| Status::unknown("No remote address"))?;
        match next_job {
            Some(job) => {
                match job.status {
                    JobStatus::Pending => {
                        // pick first file -> change job's status to MapPhase

                        // We want to pull the input files here (we assume that there have been no submitted files earlier)
                        let list_input_files = list_files_with_prefix(&self.s3_client, "mrl-lite", &job.job.input).await.unwrap();
                
                        let mut input_files: HashMap<String, FileStatus> = HashMap::new();
                        let _ = list_input_files.clone().into_iter().for_each(|f| {input_files.insert(f, FileStatus { status: JobStatus::Pending, elapsed: 0 });});
                        let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());

                        // If no input files => mark the job as completed
                        if list_input_files.len() == 0 {
                            job_q.pop_front();
                            let modified_job = Job {
                                id: job.id.clone(),
                                status: JobStatus::Completed,
                                job: job.job.clone(),
                                files: Arc::new(Mutex::new(list_input_files.clone())),
                                file_status: Arc::new(Mutex::new(input_files.clone())),
                            };
                            let mut completed_jobs = self.completed_jobs.lock().unwrap();
                            completed_jobs.push_back(modified_job);
                            return Err(Status::not_found("No files in input!"))
                        }

                        // let new_input_files = list_files_with_prefix(&self.s3_client, &format!("mrl-lite"), &job.job.input).await.unwrap();
                        // let mut input_file = job.file_status.lock().unwrap();
                        let new_status = FileStatus {
                            status: JobStatus::MapPhase,
                            elapsed: now(),
                        };
                        input_files.insert(
                            list_input_files.get(0).expect("Error: No file found in job.files").to_string().clone(),
                            new_status.clone()
                        );                        

                        let task = Task {
                            input: list_input_files.get(0).unwrap().to_string().clone(), 
                            workload: job.job.workload.clone(),
                            output: format!("/temp/{}/", job.id), //tmp file
                            args: job.job.args.join(" "),
                            status: "Map".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::MapPhase,
                            job: job.job.clone(),
                            files: Arc::new(Mutex::new(list_input_files.clone())),
                            file_status: Arc::new(Mutex::new(input_files.clone())),
                        };
                        job_q.pop_front();
                        job_q.push_front(modified_job);
                        let mut workers = self.workers.lock().unwrap();
                        workers.insert(worker_addr, WorkerNode { state: WorkerState::Busy, addr: worker_addr.clone(), elapsed: now() });
                        return Ok(Response::new(task))
                    }
                    JobStatus::MapPhase => {
                        let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());
                        let mut input_file = job.file_status.lock().unwrap();
                        let file_names = job.files.lock().unwrap();
                        // let opt_file = get_next_file(&job.files, &input_file);
                        let file = match get_next_file(&file_names, &input_file, self.timeout) {
                            Some(f) => f,
                            None => { 
                                // let modified_job = job.clone();
                                // job_q.push_front(modified_job);
                                let mut workers = self.workers.lock().unwrap();
                                workers.insert(worker_addr, WorkerNode { state: WorkerState::Idle, addr: worker_addr.clone(), elapsed: now() });
                                return Err(Status::not_found("No job available"))
                            }
                        };
                        let new_status = FileStatus {
                            status: JobStatus::MapPhase,
                            elapsed: now(),
                        };
                        input_file.insert( file.clone(), new_status.clone());

                        let task = Task {
                            input: file.clone(), 
                            workload: job.job.workload.clone(),
                            output:  format!("/temp/{}/", job.id), //tmp file
                            args: job.job.args.join(" "),
                            status: "Map".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::MapPhase,
                            job: job.job.clone(),
                            files: job.files.clone(),
                            file_status: Arc::new(Mutex::new(input_file.clone())),
                        };
                        job_q.pop_front();
                        job_q.push_front(modified_job);
                        let mut workers = self.workers.lock().unwrap();
                        workers.insert(worker_addr, WorkerNode { state: WorkerState::Busy, addr: worker_addr.clone(), elapsed: now() });
                        return Ok(Response::new(task))
                    }
                    JobStatus::Shuffle => {
                        // If the JobStatus is in shuffle, then we need to read the temp filenames in S3 again
                        let mut input_file = job.file_status.lock().unwrap();
                        let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());

                        let new_status = FileStatus {
                            status: JobStatus::ReducePhase,
                            elapsed: now(),
                        };
                        input_file.insert(
                            job.files.lock().unwrap().get(0).expect("Error: No file found in job.files").to_string().clone(),
                            new_status.clone()
                        );                        
                        let filename = job.files.lock().unwrap().get(0).unwrap().to_string().clone();

                        let task = Task {
                            input: filename.clone(), 
                            workload: job.job.workload.clone(),
                            output: format!("{}/", job.job.output.clone()),
                            args: job.job.args.join(" "),
                            status: "Reduce".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::ReducePhase,
                            job: job.job.clone(),
                            files: job.files.clone(),
                            file_status: Arc::new(Mutex::new(input_file.clone())),
                        };

                        job_q.pop_front();
                        job_q.push_front(modified_job);                        
                        let mut workers = self.workers.lock().unwrap();
                        workers.insert(worker_addr, WorkerNode { state: WorkerState::Busy, addr: worker_addr.clone(), elapsed: now() });
                        return Ok(Response::new(task))
                    }
                    JobStatus::ReducePhase => {
                        let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());

                        let mut input_file = job.file_status.lock().unwrap();
                        let file = match get_next_file(&job.files.lock().unwrap(), &input_file, self.timeout) {
                            Some(f) => f,
                            None => { 
                                // let modified_job = job.clone();
                                // job_q.push_front(modified_job);
                                let mut workers = self.workers.lock().unwrap();
                                workers.insert(worker_addr, WorkerNode { state: WorkerState::Idle, addr: worker_addr.clone(), elapsed: now() });
                                return Err(Status::not_found("No job available"))
                            }
                        };
                        let new_status = FileStatus {
                            status: JobStatus::ReducePhase,
                            elapsed: now(),
                        };
                        input_file.insert(
                            file.clone(),
                            new_status.clone()
                        );          

                        let task = Task {
                            input: file.clone(), 
                            workload: job.job.workload.clone(),
                            output: format!("{}/", job.job.output.clone()), 
                            args: job.job.args.join(" "),
                            status: "Reduce".into(),
                        };
                        let modified_job = Job {
                            id: job.id.clone(),
                            status: JobStatus::ReducePhase,
                            job: job.job.clone(),
                            files: job.files.clone(),
                            file_status: Arc::new(Mutex::new(input_file.clone())),
                        };
                        job_q.pop_front();
                        job_q.push_front(modified_job);
                        let mut workers = self.workers.lock().unwrap();
                        workers.insert(worker_addr, WorkerNode { state: WorkerState::Busy, addr: worker_addr.clone(), elapsed: now() });
                        return Ok(Response::new(task))

                    }
                    JobStatus::Completed => {
                        let mut job_q = self.job_queue.lock().unwrap_or_else(|e| e.into_inner());
                        job_q.pop_front();
                        let mut completed_jobs = self.completed_jobs.lock().unwrap();
                        let client = self.s3_client.clone();
                        let prefix = format!("temp/{}/", job.id.clone());
                        completed_jobs.push_back(job);
                        tokio::spawn(async move {
                            remove_object_with_prefix(&client, &format!("mrl-lite"), &format!("{}", prefix),  &format!("{}", prefix)).await.unwrap();
                            remove_object(&client, &format!("mrl-lite"), &format!("{}", prefix)).await.unwrap();
                        });                        

                        let mut workers = self.workers.lock().unwrap();
                        workers.insert(worker_addr, WorkerNode { state: WorkerState::Idle, addr: worker_addr.clone(), elapsed: now() });
                        return Err(Status::not_found("Job completed!"))
                    }
                }
            },
            None => {
                let mut workers = self.workers.lock().unwrap();
                workers.insert(worker_addr, WorkerNode { state: WorkerState::Idle, addr: worker_addr.clone(), elapsed: now() });
                return Err(Status::not_found("No job available"))
            }
        }
    }

    // Submit a job to the job queue
    // and return a response to the client
    async fn submit_job(&self, request: Request<JobRequest>) -> Result<Response<JobResponse>, Status> {
        let _ = match workload::try_named(&request.get_ref().workload.clone()) {
            Some(engine) => engine,
            None => {
                return Ok(Response::new(JobResponse {
                    success: false,
                    message: "Invalid workload".into(),
                })
                )
            }
        };

        let mut input_dir = request.get_ref().input.clone();
        if input_dir.ends_with('/') {
            input_dir.pop();
        }

        let mut output_dir = request.get_ref().output.clone();
        if output_dir.ends_with('/') {
            output_dir.pop();
        }

        let standalone_job = standalone::Job {
            input: input_dir.clone(),
            workload: request.get_ref().workload.clone(),
            output: output_dir.clone(),
            args: request.get_ref().args.clone().split_whitespace().map(String::from).collect() // Change this to a vector of strings
        };
        
        let job_id = format!("{:?}", now());

        let job = Job {
            id: job_id,
            status: JobStatus::Pending,
            job: standalone_job, 
            files: Arc::new(Mutex::new(Vec::new())),
            file_status: Arc::new(Mutex::new(HashMap::new())),
        };

        self.job_queue.lock().unwrap().push_back(job);

        Ok(Response::new(JobResponse {
            success: true,
            message: "Job submitted".into(),
        }))
    }   

    // List all jobs in the job queue
    // and return the list to the client
    async fn list_jobs(&self, request: Request<JobListRequest>) -> Result<Response<JobList>, Status> {
        let show = request.get_ref().show.clone();
        // Define a standalone function to convert from Job to Task
        fn job_to_task(job: Job) -> Task {
            Task {
                input: job.job.input,
                workload: job.job.workload,
                output: job.job.output,
                args: job.job.args.join(" "),
                status: format!("{:?}", job.status),
            }
        }
        let tasks = match show {
            s if s == format!("complete") => {
                let completed_jobs = self.completed_jobs.lock().unwrap();
    
                let tasks: Vec<Task> = completed_jobs.iter().map(|job| job_to_task(job.clone())).collect();
                tasks
            }
            s if s == format!("all") => {
                let completed_jobs = self.completed_jobs.lock().unwrap();
    
                let mut completed_tasks: Vec<Task> = completed_jobs.iter().map(|job| job_to_task(job.clone())).collect();
                let jobs = self.job_queue.lock().unwrap();
    
                let mut tasks: Vec<Task> = jobs.iter().map(|job| job_to_task(job.clone())).collect();
                completed_tasks.append(&mut tasks);
                completed_tasks
            }
            _ => {

                let jobs = self.job_queue.lock().unwrap();
    
                let tasks: Vec<Task> = jobs.iter().map(|job| job_to_task(job.clone())).collect();
                tasks
            }
        };
        Ok(Response::new(JobList { jobs: tasks }))
    }
    
    // Get the system status
    async fn system_status(&self, _request: Request<Empty>) -> Result<Response<SystemStatus>, Status> {
        let workers = self.workers.lock().unwrap();
        let worker_list: Vec<Worker> = workers.iter().map(|(worker_addr, worker)| {
            let state = if (now() - worker.elapsed) > self.timeout {
                WorkerState::Dead
            } else {
                worker.state.clone()
            };
            Worker {
                address: worker_addr.to_string().clone(),
                state: format!("{:?}", state),
            }
        }).collect();      
        let job_q = self.job_queue.lock().unwrap();
        let mut jobs = Vec::new();
        for job in job_q.iter() {
            let task = Task {
                input: job.job.input.clone(),
                workload: job.job.workload.clone(),
                output: job.job.output.clone(),
                args: job.job.args.join(" "),
                status: format!("{:?}", job.status),
            };
            jobs.push(task);
        }

        Ok(Response::new(SystemStatus {
            worker_count: workers.len() as i32,
            workers: worker_list,
            jobs: jobs,
        }))
    }

    /// gRPC call for workers to inform the coordinator that they have finished
    /// a task on the file given to them.
    async fn report_task(&self, request: Request<WorkerReport>) -> Result<Response<WorkerResponse>, Status> {
        let completed_file = request.get_ref().input.clone();
        let out_paths = request.get_ref().output.clone();
        let mut job_q = self.job_queue.lock().unwrap();
        // If the file status is currently in MapPhase -> change file state to Shuffle
        // If the file status is currently in ReducePhase -> change file state to Completed
        match job_q.pop_front() {
            Some(job) => {
                let in_files = job.files.lock().unwrap();
                let mut file_names = in_files.clone();
                let mut file_status = job.file_status.lock().unwrap();
                let next_file_state = next_state(&completed_file, &file_status);
                if next_file_state.is_none() {
                    let modified_job = job.clone();
                    job_q.push_front(modified_job);
                    return Err(Status::already_exists("Job already submitted"))
                }
                let next_state = next_file_state.unwrap();
                let new_status = FileStatus {
                    status: next_state.clone(),
                    elapsed: 0,
                };
                for path in out_paths {
                    file_status.insert(path.clone(), new_status.clone());
                    if !contains_path(&path, &file_names) {
                        file_names.push(path.clone());
                    }
                }
                file_status.remove(&completed_file);
                let file_index: usize = get_file_index(&completed_file, &in_files);
                file_names.remove(file_index);
                let next_job_state = check_all_file_states(&file_names, &file_status).unwrap();

                if job.status.ne(&next_job_state) {
                    let modified_job = Job {
                        id: job.id.clone(),
                        status: next_job_state,
                        job: job.job.clone(),
                        files: Arc::new(Mutex::new(file_names.clone())),
                        file_status: Arc::new(Mutex::new(file_status.clone())),
                    };
                    job_q.push_front(modified_job);
                } else {
                    let modified_job = Job {
                        id: job.id.clone(),
                        status: job.status.clone(),
                        job: job.job.clone(),
                        files: Arc::new(Mutex::new(file_names.clone())),
                        file_status: Arc::new(Mutex::new(file_status.clone())),
                    };
                    job_q.push_front(modified_job);
                }
            }
            None => {
                // Nothing in queue.. what u submitting?
                return Err(Status::not_found("No jobs in queue"))
            }
        }
        Ok(Response::new(WorkerResponse { success: true, message: "".into(), args: HashMap::new()}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello coordinator!");
    let args = Args::parse();
    let port: u128 = args.port.unwrap_or(50051);
    let os_ip: String = args.os.unwrap_or_else(|| "127.0.0.1:9000".into());
    let os_user: String = args.user.unwrap_or_else(|| "ROOTNAME".into());
    let os_pw: String = args.pw.unwrap_or_else(|| "CHANGEME123".into());
    let timeout: u64 = args.timeout.unwrap_or_else(|| 15);
    // Port to listen to
    let addr = format!("0.0.0.0:{port}").parse().unwrap();

    // If having trouble connecting to minio vvvvvvvvv
    // let s3_client = get_local_minio_client().await; 
    let s3_client = get_min_io_client(format!("http://{}",os_ip.clone()), os_user.clone(), os_pw.clone()).await.unwrap();
    let coordinator = CoordinatorService::new(os_ip.clone(), os_user.clone(), os_pw.clone(), s3_client.clone(), timeout);

    println!("Coordinator listening on {}", addr);
    // Create a bucket for the coordinator, and the subdirectores if not exist
    initialize_bucket_directories(&coordinator.s3_client).await?;

    // Start a new the gRPC server
    // and add the coordinator service to the server
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}