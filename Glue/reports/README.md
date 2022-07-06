# This repo contains a set of utilities tools to generated reports out of Glue Job executions

## Generate a File with the list of Glue Job Names

You can use the **generate_list_of_jobs.py** script to create a text file containing the list of Glue Job Names in a specific region for the current account.

You can run the script as

```
python3 generate_list_of_jobs.py
```

It will generated a file called **job_list.txt** as a result in the same location where the script reside. The file contains one Job Name per line.

## Generate for each Job Name a JSON file containing details about the different Runs

You can use the **collect_jobruns_details.py** script to create a JSON file for each Glue Job highlighting details for each JobRun.

The script expect a file called **job_list.txt** in the same location as where the script reside.

You can run the script as

```
python3 collect_jobruns_details.py
```

It will create a folder called "report" in the same location as where the script reside and inside it will create one file per Job Name called **<job_name>.json**. Inside this file you have one JSON object per line highlighting the main JobRun details.

The resulting JSON files can be imported in QuickSight in order to check different job run durations.


