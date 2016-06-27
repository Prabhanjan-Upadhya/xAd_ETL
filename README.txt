------------------------
Python Version Used: 2.7
------------------------

- Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010
- Python Software Foundation.
- All rights reserved.


----------------------
Command Line Arguments
----------------------

Example 1: ./run.sh <parallelism_factor>
Example 2: ./run.sh 10

<parallel> | -h | -clean
	<parallel> - (optional) Gives the option of setting the degree of parallelism. 
		                Defaults to 5 if not used.
	-h         - (optional) Gives instructions on running the shell executable.
	-clean     - (optional) Cleans up the generated output directory, log directory,
		                and byte code files. 

Example 3: ./test.py -p 10
Example 4: ./test.py -h

-p | -h
	-p - (optional) Gives the option of setting the degree of parallelism. 
		        Defaults to 5 if not used.
	-h - (optional) Gives instructions on running the python executable.


----------
How to run
----------
- Move all the executable files extracted, to the directory where "/in" is present.
  Also, a sample "in/" is provided with a small dataset, which can be used.
- In order to run the shell as an executable, executable permissions 
  must be given to the shell script.
  Ex: chmod 755 run.sh
- The output generated from the script can be found in the "/out" directory generated.
- A separate log file can be found at "/logs" directory which contains entries for ETL
  operations performed on the files.


-------------------------------------
Parallelism exploited in this project
-------------------------------------
- <multiprocessing> package is used to spawn multiple workers by the methods.
- <multiprocessing> module fully leverages multiple processors on the given machine, and 
  can be run on Unix and Windows platforms.
- As the *.csv files are independent of each others, inherently present data parallelism 
  makes most sense here. 
- A single thread works on a single file, and it further spawns workers internally to process 
  individual rows of the CSV files.

-------------------------------------
Thread Safety method of writing files
-------------------------------------
- multiprocessing Queues are used to queue the data to be written to the output files.
- Writer process is spawned to write the contents of the respective <result_queue> and
  <log_queue> queues to their respective files.

