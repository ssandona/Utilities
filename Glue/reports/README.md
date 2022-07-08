# This repo contains a set of utilities tools to generate reports out of Glue Job executions

Please note the following python packages are needed:
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

## Generate for a specified Job Name a JSON file containing details about the related SUCCESSFULL Runs

You can use the **generate_report_for_glue_job.py** script to create a JSON file for the specified Glue Job. For each successful JobRun an entry on the JSON file will be created.

You can run the script as:

```
python3 generate_report_for_glue_job.py --job-name JOB_NAME --region REGION --job-runs NUM_RUNS
```

The **--job-runs** parameter identifies the number of last job runs to consider. If not specified, the last 10 successful runs will be included.

The script generates the output file in **reports/<job_name>.json**. 

## Given a list of Glue Job Names generate for each a JSON file containing details about the related SUCCESSFULL Runs

You can use the **batch_generate_reports_for_glue_jobs.py** script to obtain the same results as the **generate_report_for_glue_job.py** script but for all the jobs specified on an input file.

The input file used by default is **job_list.txt** in the same location where the script resides (i.e., output of **generate_list_of_jobs.py**).

You can customize the input file location by specifying the **--jobs-list-file FILENAME** parameter.

The script generates the one output file per job in **reports/<job_name>.json**. 

You can run the script as:

```
python3 batch_generate_reports_for_glue_jobs.py --region REGION --job-runs NUM_RUNS
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