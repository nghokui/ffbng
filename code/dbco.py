import os, sqlite3

class DBConnector():
    def __init__(self, in_db_path, in_db_name):
        """ Initialization method attempts to open the database and create
            the connection and cursor objects.

            Raises an exception if the database does not exist at the path.
        """
        if not os.path.isfile(in_db_path + in_db_name):
            self._connection, self._cursor = None, None
            raise Exception('PASSED IN DATABASE PATH IS NOT VALID.')
        else:
            self._connection = sqlite3.connect(in_db_path + in_db_name)
            self._cursor = self._connection.cursor()

    """ DATABASE OBJECT PROPERTIES """
    @property
    def connection(self):
        """ Property holds the database connection object.
        """
        return self._connection
    @property
    def cursor(self):
        """ Property holds the database cursor object.
        """
        return self._cursor

    """ DATABASE ACTION METHODS """
    def singleInsert(self, table_name, fields, field_values, field_types=[]):
        """ Method is used to perform a single row insert into the specified
            table in the current database.

            This should be used if only one insert needs to occur as it
            automatically calls the transactionEnd method and commits the
            changes; essentially, calling object/class/script does not need
            to worry about committing the changes.
        """
        if not self.checkTable(table_name):
            self.createTable(table_name, fields, field_types)
        self.transactionInsert(table_name, fields, field_values)
        self.transactionEnd()
    def transactionInsert(self, t_name, fields, field_values):
        """ Method performs a transaction insert.

            The purpose of this method is to allow for a large number of 
            inserts to the specified table in the current database 
            efficiently. This is done by not performing a commit statement
            until the super object/class/script specifically calls the
            transactionEnd method, which then performs the commit.

            That being said, the object/class/script that is calling this
            transactionInsert method MUST also call the transactionEnd method
            when all values have been passed to the database. If not, the
            changes will NOT be written.

            Method works by generating the query parameters (number of values
            specified by ?) and field list (field names separated by commas)
            strings. The query is then generated from the two strings, which
            is then passed (along with the field values) to the database 
            execute method.

            If an error occurs, a rollback is called and all changes are
            discarded.
        """ 
        query = 'INSERT INTO {0} ({1}) VALUES ({2});'
        field_list_string = ','.join(fields)
        query_parameters = ','.join(['?'] * len(fields))
        query = query.format(t_name, field_list_string, query_parameters)
        try:
            self.cursor.execute(query, tuple(field_values))
        except Exception as e:
            print("Error inserting with query '{}'".format(query))
            print("\tWith columns {}".format(fields))
            print("\tWith values {}".format(field_values))
            print("\tRollback will occur.")
            self.connection.rollback()
    def transactionEnd(self):
        """ Method ends the current transaction.

            This essentially means that a commit action occurs.
        """
        self.connection.commit()
    def countTable(self, in_table_name):
        """ Method counts the number of rows in the specified table in the 
            current database.
        """
        self.cursor.execute('SELECT COUNT(*) FROM {};'.format(in_table_name))
        return self.cursor.fetchone()[0]
    def createTable(self, in_table_name, in_field_names, in_field_types):
        """ Method creates a table in the current database of the passed in
            table name with the passed in field names and field types.

            Method first zips together the field names and field types list,
            and generates a fields list of format: 'name type'.

            The table name and the fields list are passed in to the query 
            string using the .format method, and the query is executed.
        """
        zipped_fields = zip(in_field_names, in_field_types)
        fields = ['{} {}'.format(fn, ft) for fn,ft in zipped_fields]
        query = 'CREATE TABLE {} ({});'
        self.cursor.execute(query.format(in_table_name, ','.join(fields)))
        self.connection.commit()
    def dropTable(self, in_table_name):
        """ Method drops the specified table from the currently loaded 
            database.
        """
        self.cursor.execute('DROP TABLE {};'.format(in_table_name))
        self.connection.commit()
    def emptyTable(self, in_table_name):
        """ Method empties all values from the table in the current database specified by the passed in string.
        """ 
        self.cursor.execute('DELETE FROM {};'.format(in_table_name))
        self.connection.commit()
    def checkTable(self, in_table_name):
        """ Method checks to see if the passed in table name string exists in
            the current database.

            It returns True if it exists, False if it does not.
        """
        phrase1 = "SELECT count(*) FROM sqlite_master"
        phrase2 = "type='table' AND name='{}';".format(in_table_name)
        self.cursor.execute("{} WHERE {}".format(phrase1, phrase2))
        return self.cursor.fetchone()[0] == 1
    def close(self):
        """ Method is used to close the datbase connection.
        """
        self.connection.close()