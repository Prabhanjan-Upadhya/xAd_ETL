# xAd_ETL

Code exercise

Write a program in a language of your choice that spawns n workers (threads, processes, actors, whatever), where each worker simultaneously grabs a chunk of data from a directory named 'in/facts', performs ETL and produces a processed chunk of data to a directory named 'out'. The degree of parallelism n is given on the command line as an option to the program (-p) and defaults to 5.
The 'in' directory is structured as follows (assume local UNIX file system):

in
|__ dimensions
| |__ device_type.json
| |__ connection_type.json
|
|__ facts
|__ imps
| |__ 2016-05-15-05.csv
| |__ 2016-05-15-06.csv
| |__ ...
|
|__ clicks
|__ 2016-05-15-05.csv
|__ 2016-05-15-06.csv
|__ ...

Chunks of input data are represented by files from 'in/facts/imps' and 'in/facts/clicks'. Each chunk is a file from each directory that has the same timestamp as part of the file name (in UTC), which will have granularity of one hour. Records from 'in/facts/imps' files are comma-separated in the following format:
unix_timestamp,transaction_id,connection_type,device_type,count

Records from 'in/facts/clicks' have the following format:
unix_timestamp,transaction_id,count

The program will merge imps and clicks records with the same transaction_id and the same hour and produce a file to 'out' for each input hour where each record is represented as a JSON object and each record separated by a newline:
{"iso8601_timestamp":string,"transaction_id":string,"connection_type":string,"device_type":string,"imps":int,"clicks":int}

Where the timestamp will be in ISO-8601 format (Pacific time zone) of the record in the 'imps' files, and the connection_type and device_type fields will be the string representation found from the in/dimensions lookup files. A complete example of input and output files follows these instructions. Reasonable sanity checks should exist for input data with reasonable behavior for exceptional data and conditions.

A separate 'logs' dir will contain processing log files from program execution. All errors/exceptions should be logged there. In addition, there should be a log line for every input hour read indicating start of processing, and another log line for that input hour when processing completes, which includes elapsed time to process that hour.

Please package your submission with tar. The package must include a README with these instructions, a UNIX shell executable file (or instructions on how to build one) that runs your program and responds appropriately to -h, which gives instructions on running the program (including the parallelism option, -p).


Complete example of input/output files.

in/dimensions/device_type.json (list of id/name tuples)
[[1,"DESKTOP"],[2,"HANDHELD"],[3,"CONSOLE"]]

in/dimensions/connection_type.json (list of id/name tuples)
[[1,"WIFI"],[2,"ETHERNET"],[3,"2G"],[4,"3G"],[5,"4G"]]

in/facts/imps/2016-05-15-05.csv
1463288494,84392d9f0e,2,1,49832
1463288737,80a987feb887a,1,2,743

in/facts/imps/2016-05-15-06.csv
1463292904,83e8aa8710,5,2,9201
1463293759,39e3b0c0a,4,1,367

in/facts/clicks/2016-05-15-05.csv
1463288496,84392d9f0e,42

in/facts/clicks/2016-05-15-06.csv
1463293765,39e3b0c0a,7
1463293781,83e8aa8710,88

out/2016-05-15-05.json
{"iso8601_timestamp":"2016-05-14T22:01:34-07:00","transaction_id":"84392d9f0e","connection_type":"ETHERNET","device_type":"DESKTOP","imps":49832,"clicks":42}
{"iso8601_timestamp":"2016-05-14T22:05:37-07:00","transaction_id":"80a987feb887a","connection_type":"WIFI","device_type":"HANDHELD","imps":743}

out/2016-05-15-06.json
{"iso8601_timestamp":"2016-05-14T23:15:04-07:00","transaction_id":"83e8aa8710","connection_type":"4G","device_type":"HANDHELD","imps":9201,"clicks":88}
{"iso8601_timestamp":"2016-05-14T23:29:19-07:00","transaction_id":"39e3b0c0a","connection_type":"3G","device_type":"DESKTOP","imps":367,"clicks":7}

logs/etl.log
2016-05-16 13:51:37,446 INFO Hour 2016-05-15-05 ETL start.
2016-05-16 13:51:37,489 INFO Hour 2016-05-15-06 ETL start.
2016-05-16 13:51:47,218 INFO Hour 2016-05-15-06 ETL complete, elapsed time: 10s.
2016-05-16 13:51:48,491 INFO Hour 2016-05-15-05 ETL complete, elapsed time: 11s.

