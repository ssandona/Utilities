import argparse
import generate_report_for_glue_job as report

def generate_reports(runs, region, job_list_file):
	with open(job_list_file, 'r') as f:
		job_names = f.readlines()

	for job_name in job_names:
		job_name=job_name.replace('\n', "")

		report.generate_report(job_name, runs, region)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Get Glue Job Report')
	parser.add_argument('--job-runs', dest='job_runs', type=int, help='Number of Job Run to consider', default=10)
	parser.add_argument('--region', dest='region', type=str, help='AWS Region', required=True)
	parser.add_argument('--jobs-list-file', dest='job_list_file', type=str, help='Location of Job List File', default="job_list.txt")
	
	args = parser.parse_args()

	generate_reports(args.job_runs, args.region, args.job_list_file)