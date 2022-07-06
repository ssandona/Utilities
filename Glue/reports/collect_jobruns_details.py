import boto3
import os
import json
from datetime import datetime


region='eu-west-1'
job_list_file="job_list.txt"

workers_DPU={
	"G.1X": 1,
	"G.2X":2
}

path = "report"

#Extract_info from JobRuns

def extract_info(jobs):
	#print("extract")
	for job in jobs:
		try:
			job_info={}
			job_info["Id"]=job["Id"]
			job_info["Job_Name"]=job["JobName"]
			job_info["MaxCapacity"]=job["MaxCapacity"]
			job_info["ExecutionTime"]=job["ExecutionTime"]
			job_info["Attempt"]=job["Attempt"]
			if 'WorkerType' in job:
				job_info["WorkerType"]=job["WorkerType"]
				job_info["MaxDPUs"]=workers_DPU[job["WorkerType"]] * job["MaxCapacity"]
			else:
				job_info["WorkerType"]="None"
				job_info["MaxDPUs"]=0
			job_info["JobRunState"]=job["JobRunState"]
			#job_info["StartedOn"]=job["StartedOn"].isoformat()
			#job_info["CompletedOn"]=job["CompletedOn"].isoformat()
			job_info["StartedOn"]=job["StartedOn"].strftime("%Y%m%d%H%M%S")
			job_info["CompletedOn"]=job["CompletedOn"].strftime("%Y%m%d%H%M%S")

			#print(job)
			if 'Arguments' in job:
				arg=job['Arguments']
				if '--enable-auto-scaling' in arg:
					job_info["Autoscaling"]=arg["--enable-auto-scaling"]
				else:
					job_info["Autoscaling"]="false"
			else:
				job_info["Autoscaling"]="false"
			#print(job_info)
			file_object = open(path + "/" + job["JobName"] + ".json", 'a')
			job_info_Json = json.dumps(job_info)
			file_object.write(str(job_info_Json) + "\n")
			file_object.close()
		except Exception as e:
			print(e)




client = boto3.client('glue', region_name=region)

try:
    os.mkdir(path)
except:
    print ("Creation of the directory %s failed - probably already existing" % path)
else:
    print ("Successfully created the directory %s " % path)

#open(job_list_file, 'w').close()

with open(job_list_file, 'r') as f:
	job_names = f.readlines()

for job_name in job_names:
	job_name=job_name.replace('\n', "")
	print("=== Job " + job_name + "===")
	open(path + "/" + job_name + ".json", 'w').close()
	response = client.get_job_runs(
		JobName=job_name
	)
	#print(response)
	extract_info(response["JobRuns"])
	
	# No paginator available for the list_jobs operation
	while ('NextToken' in response):
		print("Not done yet")
		response = client.get_job_runs(
			JobName=job_name,
			NextToken=response["NextToken"])
		extract_info(response["JobRuns"])
	
	#print("Done")	