# Map-Reduce-Lite

This project was completed as a part of the ICCS492: The Guts of Modern System course by Kanladaporn Sirithatthamrong, Phavarisa Limchitti, and Tath Kanchanarin.

## Overview
Map-Reduce-Lite (MRL) is a distributed system inspired by the MapReduce paradigm, designed to run across multiple physical nodes using one coordinator and several worker nodes. Workers communicate with the coordinator via RPC over TCP, ensuring task distribution, execution, and status reporting. The system is built to handle multiple jobs, reschedule tasks in case of worker failures, and leverage S3-compatible storage (MinIO) for input, intermediate, and output data.

### Key Features
- **Coordinator-Worker Architecture**: The system consists of a single coordinator and multiple workers, where workers initiate communication with the coordinator via RPC to request tasks.
- **S3-Compatible Storage**: Input, intermediate, and output files are exchanged using MinIO (S3-compatible) to support distributed data storage.
- **Multiple Jobs Support**: Users can submit multiple jobs, and the system manages them in a queue, ensuring only one job runs at a time while others remain pending.
- **Straggler Detection and Handling**: To optimize performance, the system detects slow-running (straggler) workers and redistributes their tasks to faster workers if necessary.
- **CLI Interface (mrl-ctl)**: A command-line interface allows users to interact with the system, submit jobs, check job statuses, and view the system's health.
- **gRPC for Communication**: The project utilizes gRPC for RPC communication between the coordinator, workers, and the control CLI (mrl-ctl).

### Command-line Interface (CLI)
- `mrl-ctl submit --input <input-path> --output <output-path> --workload <workload-type>`  
  Submit a job to the system. Input and output paths are S3-compatible URLs, and the workload type specifies the task (e.g., word count).
  
- `mrl-ctl jobs`  
  View all submitted tasks and their current status (pending, running, shuffle phase, or completed).
  
- `mrl-ctl status`  
  Display the current health status of the system, including the number of active workers and their respective tasks.

### Running the Coordinator and Workers
- **Coordinator**:  
  `mrl-coordinator [--port <port>] {args}`  
  The coordinator listens for RPC requests from workers and the control CLI. It distributes tasks, monitors worker progress, and handles failures.

- **Worker**:  
  `mrl-worker --join <coordinatorAddress>`  
  Workers connect to the coordinator, register themselves, and request tasks for execution.

### Reference Implementation
We also provide a reference implementation (`standalone`) for single-threaded, standalone runs of Map-Reduce-Lite. The reference can be executed as follows:

`./standalone submit --input "testcases/books/*" --output out --workload wc`: This command runs the word count workload on the `testcases/books/*` files, outputting results to the `out` directory.
