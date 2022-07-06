import boto3

region='eu-west-1'
job_list_file="job_list.txt"

def write(jobs):
	print("write")
	file_object = open(job_list_file, 'a')
	for job in jobs:
		print(job)
		file_object.write(job + "\n")
	file_object.close()


open(job_list_file, 'w').close()

client = boto3.client('glue', region_name=region)

response = client.list_jobs()
write(response["JobNames"])

# No paginator available for the list_jobs operation
while ('NextToken' in response):
	print("Not done yet")
	response = client.list_jobs(
		NextToken=response["NextToken"])
	write(response["JobNames"])

print("Done")