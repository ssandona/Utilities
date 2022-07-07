import os
import argparse

path = "reports"

def generate_summary(output_file):
	global path

	open(output_file, 'w').close()
	file_object = open(output_file, 'a')

	for filename in os.listdir(path):
		f = os.path.join(path, filename)
		if os.path.isfile(f):
			with open(f, "r") as file:
				first_line = file.readline()
				file_object.write(first_line)

	file_object.close()
	print("Glue Jobs summary file of last runs written in ["+ output_file +"]")

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Generate summary of Glue Jobs last runs (parsing the output of the)')
	parser.add_argument('--output-file', dest='output_file', type=str, help='AWS Region', default="jobs_last_run_summary.json")
	
	args = parser.parse_args()

	generate_summary(args.output_file)

        	

