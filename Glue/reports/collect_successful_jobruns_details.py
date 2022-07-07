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

path = "report_success"

#Extract_info from JobRuns

def extract_info(jobs):
	#print("extract")
	num_jobs=0
	for job in jobs:
		try:
			if job["JobRunState"] == "SUCCEEDED":
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
					job_info["MaxDPUs"]=0.0
				job_info["JobRunState"]=job["JobRunState"]
				#job_info["StartedOn"]=job["StartedOn"].isoformat()
				#job_info["CompletedOn"]=job["CompletedOn"].isoformat()
				job_info["StartedOn"]=job["StartedOn"].strftime("%Y%m%d%H%M%S")
				job_info["CompletedOn"]=job["CompletedOn"].strftime("%Y%m%d%H%M%S")
				job_info["StartedOn_Day"]=job["StartedOn"].strftime("%Y%m%d")
				job_info["CompletedOn_Day"]=job["CompletedOn"].strftime("%Y%m%d")
	
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
				num_jobs = num_jobs +1
		except Exception as e:
			print(e)
	return num_jobs




client = boto3.client('glue', region_name=region)

try:
    os.mkdir(path)
except:
    print ("Creation of the directory %s failed - probably already existing\n" % path)
else:
    print ("Successfully created the directory %s \n" % path)

#open(job_list_file, 'w').close()

with open(job_list_file, 'r') as f:
	job_names = f.readlines()

for job_name in job_names:
	job_name=job_name.replace('\n', "")
	print("Collecting details for [" + job_name + "]")
	open(path + "/" + job_name + ".json", 'w').close()
	response = client.get_job_runs(
		JobName=job_name
	)
	#print(response)
	retrieved_jobs = 0
	retrieved_jobs=retrieved_jobs + extract_info(response["JobRuns"])
	
	while ('NextToken' in response):
		response = client.get_job_runs(
			JobName=job_name,
			NextToken=response["NextToken"])
		retrieved_jobs=retrieved_jobs + extract_info(response["JobRuns"])
	print("Retrieved [" + str(retrieved_jobs) + "] Job Runs details\n")

	#Remove file if no Runs
	if retrieved_jobs == 0:
		os.remove(path + "/" + job_name + ".json")
	
print("Done")	