import boto3
import os
import json
from datetime import datetime, timedelta

import numpy as np


region='eu-west-1'
job_runs_list_file="jobs_last_run_summary.json"

job_runs_cloudwatch_details="report_cloudwatch.json"



def extract_info(job):
	try:
		job_json = json.loads(job)
		job_info={}
		job_info["Id"]=job_json["Id"]
		job_info["Job_Name"]=job_json["Job_Name"]

		print("Collecting details for [" + job_json["Job_Name"] + " - " + job_json["Id"] + "]")

		job_info["MaxCapacity"]=job_json["MaxCapacity"]
		job_info["StartedOn"]=job_json["StartedOn"]
		job_info["CompletedOn"]=job_json["CompletedOn"]
		job_info["Autoscaling"]=job_json["Autoscaling"]

		#If no autoscaling we setup all the percentiles as the MaxCapacity value
		if job_json["Autoscaling"] == "false":
			job_info["numberAllExecutors_p0"]=job_json["MaxCapacity"]
			job_info["numberAllExecutors_p25"]=job_json["MaxCapacity"]
			job_info["numberAllExecutors_p50"]=job_json["MaxCapacity"]
			job_info["numberAllExecutors_p75"]=job_json["MaxCapacity"]
			job_info["numberAllExecutors_p100"]=job_json["MaxCapacity"]

			job_info["numberMaxNeededExecutors_p0"]=job_json["MaxCapacity"]
			job_info["numberMaxNeededExecutors_p25"]=job_json["MaxCapacity"]
			job_info["numberMaxNeededExecutors_p50"]=job_json["MaxCapacity"]
			job_info["numberMaxNeededExecutors_p75"]=job_json["MaxCapacity"]
			job_info["numberMaxNeededExecutors_p100"]=job_json["MaxCapacity"]
		else:
			start_time = datetime.strptime(job_json["StartedOn"], "%Y%m%d%H%M%S") - timedelta(hours=2, minutes=0)
			end_time = datetime.strptime(job_json["CompletedOn"], "%Y%m%d%H%M%S") - timedelta(hours=2, minutes=0)
		
			numberAllExecutors = client.get_metric_statistics(
				Namespace='Glue',
				MetricName='glue.driver.ExecutorAllocationManager.executors.numberAllExecutors',
				Dimensions=[
					{
						'Name': 'JobRunId',
						'Value': job_info["Id"]
					},
					{
						'Name': 'JobName',
						'Value': job_info["Job_Name"]
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
	
			job_info["numberAllExecutors_p0"]=np.percentile(metrics, 0)
			job_info["numberAllExecutors_p25"]=np.percentile(metrics, 25)
			job_info["numberAllExecutors_p50"]=np.percentile(metrics, 50)
			job_info["numberAllExecutors_p75"]=np.percentile(metrics, 75)
			job_info["numberAllExecutors_p100"]=np.percentile(metrics, 100)
	
			numberMaxNeededExecutors = client.get_metric_statistics(
				Namespace='Glue',
				MetricName='glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors',
				Dimensions=[
					{
						'Name': 'JobRunId',
						'Value': job_json["Id"]
					},
					{
						'Name': 'JobName',
						'Value': job_json["Job_Name"]
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
			
			job_info["numberMaxNeededExecutors_p0"]=np.percentile(metrics, 0)
			job_info["numberMaxNeededExecutors_p25"]=np.percentile(metrics, 25)
			job_info["numberMaxNeededExecutors_p50"]=np.percentile(metrics, 50)
			job_info["numberMaxNeededExecutors_p75"]=np.percentile(metrics, 75)
			job_info["numberMaxNeededExecutors_p100"]=np.percentile(metrics, 100)

		file_object = open(job_runs_cloudwatch_details, 'a')
		job_info_Json = json.dumps(job_info)
		file_object.write(str(job_info_Json) + "\n")
		file_object.close()
	except Exception as e:
			print(e)



client = boto3.client('cloudwatch', region_name=region)

open(job_runs_cloudwatch_details, 'w').close()

with open(job_runs_list_file, 'r') as f:
	jobs = f.readlines()

for job in jobs:
	#print(job)
	extract_info(job)






'''
response = client.get_metric_statistics(
	Namespace='AWS/ElasticMapReduce',
	MetricName='ContainerAllocated',
	Dimensions=[
		{
		    'Name': 'JobFlowId',
		    'Value': 'j-3LPP67LZZO051'
		}
	],
	StartTime=datetime(2022, 6, 23),
	EndTime=datetime(2022, 6, 24),
	Period=300,
	Statistics=[
	    'Maximum'
	]
)
'''