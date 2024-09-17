use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
    #[clap(short, long, default_value = None, short = 'J')]
    pub host: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Submit a job to the cluster
    Submit {
        /// Glob spec for the input files
        #[arg(short, long)]
        input: String,

        // Name of the workload
        #[arg(short, long)]
        workload: String,

        /// Output directory
        #[arg(short, long)]
        output: String,

        /// Auxiliary arguments to pass to the MapReduce application.
        #[clap(value_parser, last = true)]
        args: Option<Vec<String>>,
    },
    /// Lists all jobs that have been submitted to the system and their statuses
    /// 
    /// Statuses include: pending, map phase <progress>, shuffle, 
    /// shuffle phase <progress>, or completed.
    Jobs {
        /// Arg to show all only completed jobs, all jobs or only non-completed jobs.
        /// Valid arguments: complete, all, default (non-completed)
        #[arg(short, long, default_value = None)]
        show: Option<String>,
    },
    /// Displays the health status of the system, showing how many workers
    /// are registered, what the coordinator/worker(s) are up to.
    Status {
        
    }
}

pub struct Job {
    pub input: String,
    pub workload: String,
    pub output: String,
    pub args: Vec<String>,
}
