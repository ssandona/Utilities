import os


region='eu-west-1'
jobs_last_run_summary="jobs_last_run_summary.json"

path = "report"

open(jobs_last_run_summary, 'w').close()
file_object = open(jobs_last_run_summary, 'a')

for filename in os.listdir(path):
    f = os.path.join(path, filename)
    # checking if it is a file
    if os.path.isfile(f):
        with open(f, "r") as file:
        	first_line = file.readline()
        	file_object.write(first_line)

file_object.close()
        	

