import sys

import boto3
import os
import json
from datetime import datetime, timedelta

import numpy as np

import argparse


#region="eu-west-1"
#job_name="CreateHudiTable"
#job_runs=10

retrieved_jobs = 0
job_runs=0

client=None
client_cloudwatch=None

path="reports"


def extract_cloudwatch_autoscaling_info(job, job_info):

	if job_info["Autoscaling"] == "false":

		p0 = job_info["MaxCapacity"]
		p25 = job_info["MaxCapacity"]
		p50 = job_info["MaxCapacity"]
		p75 = job_info["MaxCapacity"]
		p100 = job_info["MaxCapacity"]

		print("\tnumberAllExecutors_p0 [" + str(p0) + "]")
		job_info["numberAllExecutors_p0"] =  p0

		print("\tnumberAllExecutors_p25 [" + str(p25) + "]")
		job_info["numberAllExecutors_p25"] =  p25

		print("\tnumberAllExecutors_p50 [" + str(p50) + "]")
		job_info["numberAllExecutors_p50"] =  p50

		print("\tnumberAllExecutors_p75 [" + str(p75) + "]")
		job_info["numberAllExecutors_p75"] =  p75

		print("\tnumberAllExecutors_p100 [" + str(p100) + "]")
		job_info["numberAllExecutors_p100"] =  p100

		print("\tnumberMaxNeededExecutors_p0 [" + str(p0) + "]")
		job_info["numberMaxNeededExecutors_p0"] =  p0

		print("\tnumberMaxNeededExecutors_p25 [" + str(p25) + "]")
		job_info["numberMaxNeededExecutors_p25"] =  p25

		print("\tnumberMaxNeededExecutors_p50 [" + str(p50) + "]")
		job_info["numberMaxNeededExecutors_p50"] =  p50

		print("\tnumberMaxNeededExecutors_p75 [" + str(p75) + "]")
		job_info["numberMaxNeededExecutors_p75"] =  p75

		print("\tnumberMaxNeededExecutors_p100 [" + str(p100) + "]")
		job_info["numberMaxNeededExecutors_p100"] =  p100

		return job_info

	try:
		start_time = job["StartedOn"]
		end_time = job["CompletedOn"]
	
		numberAllExecutors = client_cloudwatch.get_metric_statistics(
			Namespace='Glue',
			MetricName='glue.driver.ExecutorAllocationManager.executors.numberAllExecutors',
			Dimensions=[
				{
					'Name': 'JobRunId',
					'Value': job["Id"]
				},
				{
					'Name': 'JobName',
					'Value': job["JobName"]
				},
				{
					"Name": "Type",
					"Value": "gauge"
				}
			],
			StartTime=start_time,
			EndTime=end_time,
			Period=300,
			Statistics=[
				'Maximum'
			]
		)

		metrics = np.array([])
		# Add / Append an element at the end of a numpy array
		datapoints=numberAllExecutors["Datapoints"]

		for datapoint in datapoints:
			metrics = np.append(metrics, datapoint["Maximum"])

		p0 = np.percentile(metrics, 0)
		p25 = np.percentile(metrics, 25)
		p50 = np.percentile(metrics, 50)
		p75 = np.percentile(metrics, 75)
		p100 = np.percentile(metrics, 100)

		print("\tnumberAllExecutors_p0 [" + str(p0) + "]")
		job_info["numberAllExecutors_p0"] =  p0

		print("\tnumberAllExecutors_p25 [" + str(p25) + "]")
		job_info["numberAllExecutors_p25"] =  p25

		print("\tnumberAllExecutors_p50 [" + str(p50) + "]")
		job_info["numberAllExecutors_p50"] =  p50

		print("\tnumberAllExecutors_p75 [" + str(p75) + "]")
		job_info["numberAllExecutors_p75"] =  p75

		print("\tnumberAllExecutors_p100 [" + str(p100) + "]")
		job_info["numberAllExecutors_p100"] =  p100

		numberMaxNeededExecutors = client_cloudwatch.get_metric_statistics(
			Namespace='Glue',
			MetricName='glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors',
			Dimensions=[
				{
					'Name': 'JobRunId',
					'Value': job["Id"]
				},
				{
					'Name': 'JobName',
					'Value': job["JobName"]
				},
				{
					"Name": "Type",
					"Value": "gauge"
				}
			],
			StartTime=start_time,
			EndTime=end_time,
			Period=300,
			Statistics=[
				'Maximum'
			]
		)

		metrics = np.array([])
		# Add / Append an element at the end of a numpy array
		
		datapoints=numberMaxNeededExecutors["Datapoints"]
		
		for datapoint in datapoints:
			metrics = np.append(metrics, datapoint["Maximum"])

		p0 = np.percentile(metrics, 0)
		p25 = np.percentile(metrics, 25)
		p50 = np.percentile(metrics, 50)
		p75 = np.percentile(metrics, 75)
		p100 = np.percentile(metrics, 100)

		print("\tnumberMaxNeededExecutors_p0 [" + str(p0) + "]")
		job_info["numberMaxNeededExecutors_p0"] =  p0

		print("\tnumberMaxNeededExecutors_p25 [" + str(p25) + "]")
		job_info["numberMaxNeededExecutors_p25"] =  p25

		print("\tnumberMaxNeededExecutors_p50 [" + str(p50) + "]")
		job_info["numberMaxNeededExecutors_p50"] =  p50

		print("\tnumberMaxNeededExecutors_p75 [" + str(p75) + "]")
		job_info["numberMaxNeededExecutors_p75"] =  p75

		print("\tnumberMaxNeededExecutors_p100 [" + str(p100) + "]")
		job_info["numberMaxNeededExecutors_p100"] =  p100
		
	except Exception as e:
			print(e)

	return job_info



def extract_info(jobs):
	global retrieved_jobs
	global job_runs

	for job in jobs:
		try:
			if job["JobRunState"] == "SUCCEEDED":
				job_info={}

				print("Id [" + job["Id"] + "]")
				job_info["Id"] = job["Id"]

				print("\tJobName [" + job["JobName"] + "]")
				job_info["JobName"] = job["JobName"]

				print("\tMaxCapacity [" + str(job["MaxCapacity"]) + "]")
				job_info["MaxCapacity"] = job["MaxCapacity"]

				print("\tExecutionTime [" + str(job["ExecutionTime"]) + "]")
				job_info["ExecutionTime"] = job["ExecutionTime"]

				print("\tAttempt [" + str(job["Attempt"]) + "]")
				job_info["Attempt"] = job["Attempt"]

				if 'WorkerType' in job:
					print("\tWorkerType [" + job["WorkerType"] + "]")
					job_info["WorkerType"] = job["WorkerType"]
				else:
					print("\tWorkerType [None]")
					job_info["WorkerType"] = "None"

				print("\tJobRunState [" + job["JobRunState"] + "]")
				job_info["JobRunState"] = job["JobRunState"]

				print("\tStartedOn [" + job["StartedOn"].strftime("%Y%m%d-%H:%M:%S") + "]")
				job_info["StartedOn"] = job["StartedOn"].strftime("%Y%m%d-%H:%M:%S")

				print("\tCompletedOn [" + job["CompletedOn"].strftime("%Y%m%d-%H:%M:%S") + "]")
				job_info["CompletedOn"] = job["CompletedOn"].strftime("%Y%m%d-%H:%M:%S")
	
				if 'Arguments' in job:
					arg=job['Arguments']
					if '--enable-auto-scaling' in arg:
						print("\tAutoscaling[" + arg["--enable-auto-scaling"] + "]")
						job_info["Autoscaling"] = arg["--enable-auto-scaling"]
					else:
						print("\tAutoscaling [false]")
						job_info["Autoscaling"]="false"
				else:
					print("\tAutoscaling [false]")
					job_info["Autoscaling"]="false"

				job_info=extract_cloudwatch_autoscaling_info(job, job_info)

				#print(job_info)

				file_object = open(path + "/" + job_info["JobName"] + ".json", 'a')
				job_info_Json = json.dumps(job_info)
				file_object.write(str(job_info_Json) + "\n")
				file_object.close()

				retrieved_jobs = retrieved_jobs +1

				if retrieved_jobs >= job_runs:
					break

		except Exception as e:
			print(e)




def generate_report(job_name, runs, region):

	global job_runs
	global client
	global client_cloudwatch
	global retrieved_jobs

	job_runs = runs
	retrieved_jobs = 0

	client = boto3.client('glue', region_name=region)
	client_cloudwatch = boto3.client('cloudwatch', region_name=region)
	
	try:
		os.mkdir(path)
	except:
		pass
	
	open(path + "/" + job_name + ".json", 'w').close()
	
	print("======================================")
	print("Collecting details for Job [" + job_name + "] (last " + str(job_runs) + " runs)")
	print("======================================")
	
	response = client.get_job_runs(
		JobName=job_name
	)
	
	
	extract_info(response["JobRuns"])
	
	while ('NextToken' in response):
		if retrieved_jobs >= job_runs:
			break
		response = client.get_job_runs(
			JobName=job_name,
			NextToken=response["NextToken"])
		extract_info(response["JobRuns"])
	
	print("\nRetrieved [" + str(retrieved_jobs) + "] Job Runs details\n")

	#Remove file if no Runs
	if retrieved_jobs == 0:
		os.remove(path + "/" + job_name + ".json")
	else:
		print("Glue Job Run details written in [" + path + "/" + job_name + ".json]")
		

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Get Glue Job Report')
	parser.add_argument('--job-name', dest='job_name', type=str, help='Name of Glue Job', required=True)
	parser.add_argument('--job-runs', dest='job_runs', type=int, help='Number of Job Runs to consider', default=10)
	parser.add_argument('--region', dest='region', type=str, help='AWS Region', required=True)
	
	args = parser.parse_args()

	generate_report(args.job_name, args.job_runs, args.region)