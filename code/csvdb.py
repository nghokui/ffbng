import os
import sqlite3
import pandas as pd

class DBLoader():
	def __init__(self, **kwargs):
		self._directory = kwargs['data_directory']
		self.connection = kwargs['database_name']

	@property
	def directory(self):
	    return self._directory

	@property
	def connection(self):
	    return self._connection
	@connection.setter
	def connection(self, in_database):
		self._connection = sqlite3.connect(in_database)

		
	
	

