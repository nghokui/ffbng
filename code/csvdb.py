import os, sqlite3, numpy
import pandas as pd
from code.dbco import DBConnector

class DBLoader():
    def __init__(self, settings):
        self._dbc = DBConnector(settings['db_path'], settings['db_name'])
        self._directory = settings['data_directory']
        self._chunk = settings['chunk_size']
        self._convert = settings['conversion_list']
        self._files, self._filenames = self.processFilesInDirectory()
    @property
    def convert(self):
        return self._convert
    @property
    def dbc(self):
        """ Returns the Database Connector object.
        """
        return self._dbc
    @property
    def directory(self):
        """ Returns the string pointing to the directory in which the csv files
            are located.
        """
        return self._directory
    @property
    def chunk(self):
        """ Returns the chunk size for mass inserts to the database.
        """
        return self._chunk
    @property
    def files(self):
        """ Returns a list of the files in the directory to be loaded.
        """
        return self._files
    @property
    def filenames(self):
        return self._filenames
    def processFilesInDirectory(self):
        """ Sets the file list variable. 

            The file list variable essentially stores a path to each file in 
            the passed in directory.
        """
        for dirpath, dirnames, filenames in os.walk(self.directory):
            file_list, file_names = [], []
            for f in filenames:
                file_list.append(os.path.join(self.directory, f))
                file_names.append(f.lower().split('.')[0])
        return (file_list, file_names)

    def transfer(self):
        for file,fname in zip(self.files, self.filenames):
            data = pd.read_csv(file, low_memory=False)

            field_names = [col for col in list(data.columns.values)]
            field_types = [self.convert[str(d)] for d in list(data.dtypes)]

            # check if datbase exists; create if it doesn't
            if self.dbc.checkTable(fname):
                self.dbc.dropTable(fname)
            self.dbc.createTable(fname, field_names, field_types)

            # add values to database
            chunk_count = 0
            for index_value in list(data.index.values):
                field_values, temp_values = [], list(data.ix[index_value])
                for ftype,val in zip(field_types, temp_values):
                    if ftype == 'INTEGER' and isinstance(val, numpy.int64):
                        field_values.append(int(val))
                    elif ftype == 'REAL' and isinstance(val, numpy.float64):
                        field_values.append(float(val))
                    else:
                        field_values.append(val)

                self.dbc.transactionInsert(fname, field_names, field_values)

                chunk_count += 1
                if chunk_count == self.chunk:
                    self.dbc.transactionEnd()
                    chunk_count = 0
            self.dbc.transactionEnd()








        
    
    

