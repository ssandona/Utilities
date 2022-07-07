import boto3
import argparse


def write(jobs,job_list_file):
	print("=========")
	file_object = open(job_list_file, 'a')
	for job in jobs:
		print(job)
		file_object.write(job + "\n")
	file_object.close()


def generate_list(region, output_file):

	print("Retrieving Glue jobs ...")

	open(output_file, 'w').close()
	
	client = boto3.client('glue', region_name=region)
	
	response = client.list_jobs()
	write(response["JobNames"], output_file)
	
	# No paginator available for the list_jobs operation
	while ('NextToken' in response):
		response = client.list_jobs(
			NextToken=response["NextToken"])
		write(response["JobNames"], output_file)
	
	print("=========")
	print("List of Glue jobs written in [" + output_file + "]")


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Get List of Glue Jobs in the specified region')
	parser.add_argument('--region', dest='region', type=str, help='AWS Region', required=True)
	parser.add_argument('--output-file', dest='output_file', type=str, help='AWS Region', default="job_list.txt")
	
	args = parser.parse_args()

	generate_list(args.region,args.output_file)