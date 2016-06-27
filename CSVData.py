#!/usr/bin/python

import multiprocessing, time, json, pytz
from datetime import datetime
from collections import OrderedDict

""" List of Global Variable used in the methods. """

connection_type = {}
device_type = {}
tz = pytz.timezone('America/Los_Angeles')
number_of_threads = 5
log_queue = multiprocessing.Queue()

class CSVData:

	"""
	CSVData class used to store the data extracted from the CSV file.

	Data Attributes of this class:
		<data> is an Ordered dictionary.

	Methods defined here:
	convert(<epoch_time>)
		Takes in Epoch time to convert to PST in ISO8061 format and returns the same.
	printData()
		Prints the data stored in the dictionary and the CSV data
	
	UpdateClicks( <clicks> )
		Updates merges data attribute <clicks> with the CSV data stored.
	Make_JSON_Object()
		Returns JSON object to be written to the output file using the data stored in the Ordered Dictionary.
	"""

	def __init__(self, list_of_data, connection_type, device_type):

		"""
		Constructor for CSVData.
		Data Attributes:
		data{} is an Ordered dictionary used to store information for merging and creating JSON output.
		"""

		self.data = OrderedDict()
		self.data["iso8061_timestamp"] = self.convert( float(list_of_data[0]) )
		self.data["transaction_id"] = list_of_data[1]
		self.data["connection_type"] = connection_type[int(list_of_data[2])]
		self.data["device_type"] = device_type[int(list_of_data[3])]
		self.data["imps"] = int(list_of_data[4])


	def convert(self, secs):

		""" Returns PST datetime in ISO8061 format. """

		date = datetime.fromtimestamp(secs, tz)
		return date.strftime('%Y-%m-%dT%H:%M:%S-07:00')


	def printData(self):


		""" Prints the data attributes of the class. """

		print "Start Time: " + self.data["iso8061_timestamp"]
		print "Transaction ID: " + self.data["transaction_id"]
		print "Connection Type: " + self.data["connection_type"]
		print "Device_Type: " + self.data["device_type"]
		print "Count: " + str(self.data["imps"])
		print "Clicks: " + str(self.data["clicks"])


	def UpdateClicks(self, clicks):

		""" Method used for merging clicks with the stored data of the class. """

		self.data["clicks"] = int(clicks)


	def Make_JSON_Object(self):

		""" Creates and returns JSON object to be dumped to the output file. """

		return json.dumps(self.data, encoding = 'utf8')
