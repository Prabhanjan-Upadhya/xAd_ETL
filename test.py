#!/usr/bin/python

import os, sys, getopt, csv, errno
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
from CSVData import *

def Update_Log(file, message):

	""" Formats the log messages. Takes in <file> and <message> arguments. """

	utc_start = datetime.now(tz)
	current_time = utc_start.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
	return current_time+ " INFO Hour " + file + message

def Writer(dest_filename, result_queue, stop):

	""" Writes output to files in a thread-safe manner using the multiprocessing queue. """

	with open(dest_filename, 'a') as dest_file:
		while True:
			line = result_queue.get()
			if line == stop:
				dest_file.close()
				return
			dest_file.write(line + '\n')

def parse_row(row, filename, result_queue):

	"""
	Threads use this method to merge <row> data with the file <filename> to be merged. 
	Uses CSVData object to store and merge results from the files.
	"""

	try:
		connection_type[int(row[2])]
		device_type[int(row[3])]
	except (KeyError, IndexError) as e:
		log_queue.put( Update_Log(filename, ": Corrupted device or connection data.") )
		return
	
	CSV_Data = CSVData(row, connection_type, device_type)
	des = open("in/facts/clicks/" + filename, 'rb')
	try:
		reader_des = csv.reader(des)
		for row_des in reader_des:
			if( CSV_Data.data["transaction_id"] == row_des[1]):
				date = datetime.fromtimestamp(float(row_des[0]), tz)
				timestamp_file2 = date.strftime('%Y-%m-%dT%H:%M:%S-07:00')
				if( CSV_Data.data["iso8061_timestamp"][:13] == timestamp_file2[:13]):
					CSV_Data.UpdateClicks(row_des[2])
	finally:
		des.close()

	json_data =  CSV_Data.Make_JSON_Object()
	result_queue.put(json_data)
	
def parse_etl(file, log_queue):
	
	"""
	Spawns <n> threads to work on each row of the initial CSV file.
	Also starts a file writer process to create the a custom output file.
	Uses the multiprocessing queue <result_queue> to store the contents to be written to the output file.
	"""	

	utc_start = datetime.now(tz)
	start_time = utc_start.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
	log_queue.put(start_time+ " INFO Hour " + os.path.splitext(file)[0] + " ETL start.")
	
	result_queue = multiprocessing.Queue()
	Kill_Flag = "Kill!"

	src = open("in/facts/imps/" + file, 'rb')
	try:
		with open('in/facts/clicks/' + file) as desfile:
			desfile.close()

	except (OSError, IOError) as e:
		log_queue.put( Update_Log(file, ": Cannot find file or directory.") )
			
	else:
		try:
			reader_src = csv.reader(src)
			writer_process = multiprocessing.Process(target=Writer, args=('out/'+ os.path.splitext(file)[0] + '.json', result_queue, Kill_Flag))
			writer_process.start()
			pool = ThreadPool(number_of_threads)
			results = pool.map( partial(parse_row, filename=file, result_queue=result_queue), reader_src)

			pool.close()
			pool.join()
		finally:
			src.close()
			result_queue.put(Kill_Flag)
			writer_process.join()
	
		utc_end = datetime.now(tz)
		end_time = utc_end.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
	
		log_queue.put(end_time + " INFO Hour " + os.path.splitext(file)[0] + " ETL completed, elapsed time: " + str( (utc_end - utc_start).seconds ) + "s.") 


def main(argv):

	"""
	Driver function to do ETL operations on the files.
	Creates log and output directories.
	Spawns <n_threads> number of threads to do ETL operations on the files. <n_threads> can be passed from the command line using < -p > argument.  
	Uses multiprocessing queue <log_queue> for logging the operation information to the <etl.log> file. Log file can be found at <log/> 
	Spawns a writer process to write log messages.
	Use < -h > or < --help > on instructions regarding the instructions on running the programs.
	"""

	input = "in/facts"
	imps_path = input + "/imps"
	dimensions_path = "in/dimensions/"
	n_threads = 5
	Kill_Flag = "KILL!"
	
	try:
		os.makedirs("logs")
	except (IOError, OSError) as e:
		if e.errno != 17:
			raise

	log_process = multiprocessing.Process(target=Writer, args=('logs/etl.log', log_queue, Kill_Flag))
	log_process.start()
	try:
		opts, args = getopt.getopt(argv, "hp:",["n_threads="])
	except:
		print 'test.py -p <degree_of_parallelism>'
		log_queue.put(Kill_Flag)
		log_process.join()
		sys.exit(0)
	for opt,arg in opts:
		if opt in ("-h", "--help"):
			print './test.py -p <parallelism_option>'
			log_queue.put(Kill_Flag)
			log_process.join()
			sys.exit(2)
		elif opt in ("-p", "--parallel"):
			n_threads = int(arg)

	print "Running with ", n_threads, " levels of parallelism"
	try:
		with open(dimensions_path + 'connection_type.json') as data_file:
			data = json.load(data_file)
			for [x,y] in data:
				connection_type[x] = str(y)
	except (IOError, OSError) as e:
		#log_queue.put("File Error. Cannot find File or Directory: " + 'device_type.json')
		log_queue.put( Update_Log('connection_type.json', ": Cannot find file or directory.") )
		log_queue.put(Kill_Flag)
		log_process.join()
		sys.exit()

	try:
		with open(dimensions_path + 'device_type.json') as data_file:
			data = json.load(data_file)
			for [x,y] in data:
				device_type[x] = str(y)
	except (IOError, OSError) as e:
		#log_queue.put("File Error. Cannot find File or Directory: " + 'device_type.json')
		log_queue.put( Update_Log('device_type.json', ": Cannot find file or directory.") )
		log_queue.put(Kill_Flag)
		log_process.join()
		sys.exit()
	
	try:
		os.makedirs("out")
	except (IOError, OSError) as e:
		if e.errno != 17:
			raise

	
	number_of_threads = n_threads
	p = ThreadPool(n_threads)		#Creates a pool of n_threads to be passed from the command-line. Defaults to 5.
	p.map( partial(parse_etl, log_queue = log_queue), os.listdir(imps_path) )
	p.close()
	log_queue.put(Kill_Flag)
	log_process.join()
	p.join()

if __name__ == "__main__":
	main(sys.argv[1:])
