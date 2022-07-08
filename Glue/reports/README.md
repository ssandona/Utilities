# Generate reports out of Glue Job executions

This repo contains a set of utilities tools to generate reports out of Glue Job executions.

## Prerequisites

Python3 is needed to run the included utilities.

The following Python packages are needed:
- json
- numpy
- boto3

If not installed, you can install them with:

```
pip3 install boto3
pip3 install json
pip3 install numpy
```

## Generate a File with the list of Glue Job Names

You can use the **generate_list_of_jobs.py** script to create a text file containing the list of Glue Job Names in a specific region for the current account.

You can run the script as follow

```
python3 generate_list_of_jobs.py --region REGION
```

The script generates by default the output file **job_list.txt** in the same location where the script resides. The file contains one Job Name per line.
You can customize the file output location by specifying the **--output-file FILENAME** parameter.

### Example run

Run command:

```
python3 generate_list_of_jobs.py --region eu-west-1
```

Stdout:

```
Retrieving Glue jobs ...
=========
test-job-1
test-job-2
=========
List of Glue jobs written in [job_list.txt]
```

Output file **job_list.txt**:

```
test-job-1
test-job-2
```

## Generate for a specified Job Name a JSON file containing details about the related SUCCESSFULL Runs

You can use the **generate_report_for_glue_job.py** script to create a JSON file for the specified Glue Job. For each successful JobRun an entry on the JSON file will be created.

You can run the script as:

```
python3 generate_report_for_glue_job.py --job-name JOB_NAME --region REGION --job-runs NUM_RUNS
```

The **--job-runs** parameter identifies the number of last job runs to consider. If not specified, the last 10 successful runs will be included. If the available Job Runs number is lower than the specified value only the available Job runs will be retrieved. 

The script generates the output file in **reports/<job_name>.json**. Details about the retrieved info are also printed on the stdout during script execution.

For Glue jobs configured with autoscaling the number configured as "Maximum number of workers" does not reflect the real number of allocated executors for the entire duration of the jobs. In CloudWatch we have available the 2 following metrics:

- **glue.driver.ExecutorAllocationManager.executors.numberAllExecutors** : The number of actively running job executors.
- **glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors** : The number of maximum (actively running and pending) job executors needed to satisfy the current load.

The first metric highlights the number of Executors allocated at a specific point in time and so reflect the actual resources utilization. The second metric highlights the number of Executors need estimated at a specific point in time and can be useful to evaluate the value we selected for "Maximum number of workers" (See more info in <a href="https://docs.aws.amazon.com/glue/latest/dg/monitoring-awsglue-with-cloudwatch-metrics.html" target="_blank">Monitoring AWS Glue Using Amazon CloudWatch Metrics</a> and <a href="https://docs.aws.amazon.com/glue/latest/dg/monitor-debug-capacity.html" target="_blank">Monitoring for DPU Capacity Planning</a>.

On the script for each Job Run configured with autoscaling we collect from CloudWatch values of the 2 metrics every 5 minutes for the entire duration of the execution. We then compure the p0, p25, p50, p75 and p100 percentiles for those as a summary. For each Job Run configured without autoscaling we setup the percentiles values for those 2 metrics with the value of "Requested number of workers".



### Example run

Run command:

```
python3 generate_report_for_glue_job.py --job-name test-job-1 --region eu-west-1 --job-runs 2
```

Stdout:

```
======================================
Collecting details for Job [test-job-1] (last 2 runs)
======================================
Id [jr_YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY]
	JobName [test-job-1]
	MaxCapacity [6.0]
	MaxNumberOfWorkers [3.0]
	ExecutionTime [145]
	Attempt [0]
	WorkerType [G.2X]
	JobRunState [SUCCEEDED]
	StartedOn [20220706-14:55:14]
	CompletedOn [20220706-14:57:52]
	Autoscaling[true]
	numberAllExecutors_p0 [2.0]
	numberAllExecutors_p25 [2.0]
	numberAllExecutors_p50 [2.0]
	numberAllExecutors_p75 [2.0]
	numberAllExecutors_p100 [2.0]
	numberMaxNeededExecutors_p0 [1.0]
	numberMaxNeededExecutors_p25 [1.0]
	numberMaxNeededExecutors_p50 [1.0]
	numberMaxNeededExecutors_p75 [1.0]
	numberMaxNeededExecutors_p100 [1.0]
Id [jr_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX]
	JobName [test-job-1]
	MaxCapacity [3.0]
	MaxNumberOfWorkers [3.0]
	ExecutionTime [147]
	Attempt [0]
	WorkerType [G.1X]
	JobRunState [SUCCEEDED]
	StartedOn [20220704-12:28:07]
	CompletedOn [20220704-12:30:41]
	Autoscaling [false]
	numberAllExecutors_p0 [3.0]
	numberAllExecutors_p25 [3.0]
	numberAllExecutors_p50 [3.0]
	numberAllExecutors_p75 [3.0]
	numberAllExecutors_p100 [3.0]
	numberMaxNeededExecutors_p0 [3.0]
	numberMaxNeededExecutors_p25 [3.0]
	numberMaxNeededExecutors_p50 [3.0]
	numberMaxNeededExecutors_p75 [3.0]
	numberMaxNeededExecutors_p100 [3.0]

Retrieved [2] Job Runs details

Glue Job Run details written in [reports/test-job-1.json]
````

Output file **reports/test-job-1.json**:

```
{"Id": "jr_YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY", "JobName": "test-job-1", "MaxCapacity": 6.0, "MaxNumberOfWorkers": 3.0, "ExecutionTime": 145, "Attempt": 0, "WorkerType": "G.2X", "JobRunState": "SUCCEEDED", "StartedOn": "20220706-14:55:14", "CompletedOn": "20220706-14:57:52", "Autoscaling": "true", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 1.0, "numberMaxNeededExecutors_p25": 1.0, "numberMaxNeededExecutors_p50": 1.0, "numberMaxNeededExecutors_p75": 1.0, "numberMaxNeededExecutors_p100": 1.0}
{"Id": "jr_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "JobName": "test-job-1", "MaxCapacity": 3.0, "MaxNumberOfWorkers": 3.0, "ExecutionTime": 147, "Attempt": 0, "WorkerType": "G.1X", "JobRunState": "SUCCEEDED", "StartedOn": "20220704-12:28:07", "CompletedOn": "20220704-12:30:41", "Autoscaling": "false", "numberAllExecutors_p0": 3.0, "numberAllExecutors_p25": 3.0, "numberAllExecutors_p50": 3.0, "numberAllExecutors_p75": 3.0, "numberAllExecutors_p100": 3.0, "numberMaxNeededExecutors_p0": 3.0, "numberMaxNeededExecutors_p25": 3.0, "numberMaxNeededExecutors_p50": 3.0, "numberMaxNeededExecutors_p75": 3.0, "numberMaxNeededExecutors_p100": 3.0}

```

This JSON file can be imported in QuickSight in order to see how our last **NUM_RUNS** Glue Job Executions performed over time.


## Given a list of Glue Job Names generate for each one a JSON file containing details about the related SUCCESSFULL Runs

You can use the **batch_generate_reports_for_glue_jobs.py** script to obtain the same results as the **generate_report_for_glue_job.py** script but for all the jobs specified inside an input file.

The input file used by default is **job_list.txt** in the same location where the script resides (i.e., output of **generate_list_of_jobs.py**).

You can customize the input file location by specifying the **--jobs-list-file FILENAME** parameter.

The script generates one output file per job in **reports/<job_name>.json**. 

You can run the script as:

```
python3 batch_generate_reports_for_glue_jobs.py --region REGION --job-runs NUM_RUNS
```

### Example run

Run command:

```
python3 batch_generate_reports_for_glue_jobs.py -region eu-west-1 --job-runs 2
```

Stdout:

```
======================================
Collecting details for Job [test-job-1] (last 2 runs)
======================================
Id [jr_YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY]
	JobName [test-job-1]
	MaxCapacity [6.0]
	MaxNumberOfWorkers [3.0]
	ExecutionTime [145]
	Attempt [0]
	WorkerType [G.2X]
	JobRunState [SUCCEEDED]
	StartedOn [20220706-14:55:14]
	CompletedOn [20220706-14:57:52]
	Autoscaling[true]
	numberAllExecutors_p0 [2.0]
	numberAllExecutors_p25 [2.0]
	numberAllExecutors_p50 [2.0]
	numberAllExecutors_p75 [2.0]
	numberAllExecutors_p100 [2.0]
	numberMaxNeededExecutors_p0 [1.0]
	numberMaxNeededExecutors_p25 [1.0]
	numberMaxNeededExecutors_p50 [1.0]
	numberMaxNeededExecutors_p75 [1.0]
	numberMaxNeededExecutors_p100 [1.0]
Id [jr_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX]
	JobName [test-job-1]
	MaxCapacity [3.0]
	MaxNumberOfWorkers [3.0]
	ExecutionTime [147]
	Attempt [0]
	WorkerType [G.1X]
	JobRunState [SUCCEEDED]
	StartedOn [20220704-12:28:07]
	CompletedOn [20220704-12:30:41]
	Autoscaling [false]
	numberAllExecutors_p0 [3.0]
	numberAllExecutors_p25 [3.0]
	numberAllExecutors_p50 [3.0]
	numberAllExecutors_p75 [3.0]
	numberAllExecutors_p100 [3.0]
	numberMaxNeededExecutors_p0 [3.0]
	numberMaxNeededExecutors_p25 [3.0]
	numberMaxNeededExecutors_p50 [3.0]
	numberMaxNeededExecutors_p75 [3.0]
	numberMaxNeededExecutors_p100 [3.0]

Retrieved [2] Job Runs details

Glue Job Run details written in [reports/test-job-1.json]

======================================
Collecting details for Job [test-job-2] (last 2 runs)
======================================
Id [jr_KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK]
	JobName [test-job-2]
	MaxCapacity [2.0]
	MaxNumberOfWorkers [2.0]
	ExecutionTime [46]
	Attempt [0]
	WorkerType [G.1X]
	JobRunState [SUCCEEDED]
	StartedOn [20220428-11:55:53]
	CompletedOn [20220428-11:56:45]
	Autoscaling [false]
	numberAllExecutors_p0 [2.0]
	numberAllExecutors_p25 [2.0]
	numberAllExecutors_p50 [2.0]
	numberAllExecutors_p75 [2.0]
	numberAllExecutors_p100 [2.0]
	numberMaxNeededExecutors_p0 [2.0]
	numberMaxNeededExecutors_p25 [2.0]
	numberMaxNeededExecutors_p50 [2.0]
	numberMaxNeededExecutors_p75 [2.0]
	numberMaxNeededExecutors_p100 [2.0]
Id [jr_WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW]
	JobName [test-job-2]
	MaxCapacity [2.0]
	MaxNumberOfWorkers [2.0]
	ExecutionTime [52]
	Attempt [0]
	WorkerType [G.1X]
	JobRunState [SUCCEEDED]
	StartedOn [20220428-11:51:24]
	CompletedOn [20220428-11:52:23]
	Autoscaling [false]
	numberAllExecutors_p0 [2.0]
	numberAllExecutors_p25 [2.0]
	numberAllExecutors_p50 [2.0]
	numberAllExecutors_p75 [2.0]
	numberAllExecutors_p100 [2.0]
	numberMaxNeededExecutors_p0 [2.0]
	numberMaxNeededExecutors_p25 [2.0]
	numberMaxNeededExecutors_p50 [2.0]
	numberMaxNeededExecutors_p75 [2.0]
	numberMaxNeededExecutors_p100 [2.0]

Retrieved [2] Job Runs details

Glue Job Run details written in [reports/test-job-2.json]
```

Output file **reports/test-job-1.json**:

```
{"Id": "jr_YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY", "JobName": "test-job-1", "MaxCapacity": 6.0, "MaxNumberOfWorkers": 3.0, "ExecutionTime": 145, "Attempt": 0, "WorkerType": "G.2X", "JobRunState": "SUCCEEDED", "StartedOn": "20220706-14:55:14", "CompletedOn": "20220706-14:57:52", "Autoscaling": "true", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 1.0, "numberMaxNeededExecutors_p25": 1.0, "numberMaxNeededExecutors_p50": 1.0, "numberMaxNeededExecutors_p75": 1.0, "numberMaxNeededExecutors_p100": 1.0}
{"Id": "jr_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", "JobName": "test-job-1", "MaxCapacity": 3.0, "MaxNumberOfWorkers": 3.0, "ExecutionTime": 147, "Attempt": 0, "WorkerType": "G.1X", "JobRunState": "SUCCEEDED", "StartedOn": "20220704-12:28:07", "CompletedOn": "20220704-12:30:41", "Autoscaling": "false", "numberAllExecutors_p0": 3.0, "numberAllExecutors_p25": 3.0, "numberAllExecutors_p50": 3.0, "numberAllExecutors_p75": 3.0, "numberAllExecutors_p100": 3.0, "numberMaxNeededExecutors_p0": 3.0, "numberMaxNeededExecutors_p25": 3.0, "numberMaxNeededExecutors_p50": 3.0, "numberMaxNeededExecutors_p75": 3.0, "numberMaxNeededExecutors_p100": 3.0}
```

Output file **reports/test-job-2.json**:

```
{"Id": "jr_KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK", "JobName": "test-job-2", "MaxCapacity": 2.0, "MaxNumberOfWorkers": 2.0, "ExecutionTime": 46, "Attempt": 0, "WorkerType": "G.1X", "JobRunState": "SUCCEEDED", "StartedOn": "20220428-11:55:53", "CompletedOn": "20220428-11:56:45", "Autoscaling": "false", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 2.0, "numberMaxNeededExecutors_p25": 2.0, "numberMaxNeededExecutors_p50": 2.0, "numberMaxNeededExecutors_p75": 2.0, "numberMaxNeededExecutors_p100": 2.0}
{"Id": "jr_WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW", "JobName": "test-job-2", "MaxCapacity": 2.0, "MaxNumberOfWorkers": 2.0, "ExecutionTime": 52, "Attempt": 0, "WorkerType": "G.1X", "JobRunState": "SUCCEEDED", "StartedOn": "20220428-11:51:24", "CompletedOn": "20220428-11:52:23", "Autoscaling": "false", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 2.0, "numberMaxNeededExecutors_p25": 2.0, "numberMaxNeededExecutors_p50": 2.0, "numberMaxNeededExecutors_p75": 2.0, "numberMaxNeededExecutors_p100": 2.0}
```


## Take the last job run details for each job and include that in a Summary File

You can use the **generate_summary_with_last_runs.py** to include the last run of each processed Glue Job (i.e., processed by **batch_generate_reports_for_glue_jobs.py** script) and include it in a single file.

This script will

- take the output of the **batch_generate_reports_for_glue_jobs.py** script (i.e., **reports/<job_name>.json** files)
- take the first line of each file (last successful Job Run) and include it in an output file (i.e., by default **jobs_last_run_summary.json**)

You can run the script as

```
python3 generate_summary_with_last_runs.py
```

You can customize the output file location by specifying the **--output-file FILENAME** parameter.

### Example run

Run command:

```
python3 generate_summary_with_last_runs.py
```

Stdout:

```
Include Last Run of [test-job-1] (reports/test-job-1.json)
Include Last Run of [test-job-2] (reports/test-job-2.json)
Glue Jobs summary file of last runs written in [jobs_last_run_summary.json]
```

Output file **reports/test-job-1.json**:

```
{"Id": "jr_YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY", "JobName": "test-job-1", "MaxCapacity": 6.0, "MaxNumberOfWorkers": 3.0, "ExecutionTime": 145, "Attempt": 0, "WorkerType": "G.2X", "JobRunState": "SUCCEEDED", "StartedOn": "20220706-14:55:14", "CompletedOn": "20220706-14:57:52", "Autoscaling": "true", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 1.0, "numberMaxNeededExecutors_p25": 1.0, "numberMaxNeededExecutors_p50": 1.0, "numberMaxNeededExecutors_p75": 1.0, "numberMaxNeededExecutors_p100": 1.0}
{"Id": "jr_KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK", "JobName": "test-job-2", "MaxCapacity": 2.0, "MaxNumberOfWorkers": 2.0, "ExecutionTime": 46, "Attempt": 0, "WorkerType": "G.1X", "JobRunState": "SUCCEEDED", "StartedOn": "20220428-11:55:53", "CompletedOn": "20220428-11:56:45", "Autoscaling": "false", "numberAllExecutors_p0": 2.0, "numberAllExecutors_p25": 2.0, "numberAllExecutors_p50": 2.0, "numberAllExecutors_p75": 2.0, "numberAllExecutors_p100": 2.0, "numberMaxNeededExecutors_p0": 2.0, "numberMaxNeededExecutors_p25": 2.0, "numberMaxNeededExecutors_p50": 2.0, "numberMaxNeededExecutors_p75": 2.0, "numberMaxNeededExecutors_p100": 2.0}
```

This JSON file can be imported in QuickSight in order to understand (according to our last job runs) which are our biggest jobs (i.e., order them by **ExecutionTime**, order them by **numberAllExecutors_p75** or by a combination of the 2).


