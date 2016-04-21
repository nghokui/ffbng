import imp, os, unittest
class DBLoaderTester(unittest.TestCase):
    def setUp(self):
        script_path = os.path.dirname(os.path.realpath(__file__))
        self.project_path = '/'.join(script_path.split('/')[:-2]) + '/'
        
        #import package
        self.modules= {}
        pkg_name, modules = 'code', ['dbco', 'csvdb']
        pkg_path, mod_path = self.project_path, self.project_path + pkg_name
        file, path, description = imp.find_module('code', [self.project_path])
        pkg = imp.load_module('code', file, path, description)
        for module in modules:
            file, path, description = imp.find_module(module, [mod_path])
            mod = imp.load_module(module, file, path, description)
            self.modules[module] = mod
            file.close()
    def testGetFileList(self):
        db_path = '{}{}'.format(self.project_path, 'testing/')
        test_path = '{}{}/{}/'.format(db_path, 'test_files', 'test_file_list')
        expected_list = ['blank1.csv','blank2.csv', 'blank3.csv']
        expected_files = ['{}{}'.format(test_path, f) for f in expected_list]
        expected_filenames = [f.split('.')[0] for f in expected_list]

        init_settings = {'db_path':db_path, 
                         'db_name':'test.db',
                         'data_directory':db_path+'test_files/test_file_list/',
                         'chunk_size':25,
                         'conversion_list':{
                            'object':'TEXT', 
                            'int64':'INTEGER', 
                            'float64':'REAL'
                         }
                        }
        db_object = self.modules['csvdb'].DBLoader(init_settings)

        for file,filename in zip(expected_files, expected_filenames):
            self.assertTrue(file in db_object.files,
                            msg='File not found: {}'.format(file))
            self.assertTrue(filename in db_object.filenames,
                            msg='File name not found: {}'.format(filename))
    def testTransfer(self):
        db_path = '{}{}'.format(self.project_path, 'testing/')
        test_directory = db_path + 'test_files/test_csvdb_transfer_files/'
        init_settings = {'db_path':db_path, 
                         'db_name':'test.db',
                         'data_directory':test_directory,
                         'chunk_size':50,
                         'conversion_list':{
                            'object':'TEXT', 
                            'int64':'INTEGER', 
                            'float64':'REAL'
                         }
                        }
        dbo = self.modules['csvdb'].DBLoader(init_settings)
        for f,fnm in zip(dbo.files,dbo.filenames):
            self.assertEqual('conv', fnm, msg='Returned: {}'.format(fnm))

        error_msg = 'Expected {} rows; got {} instead.'
        dbo.transfer()
        db_table_count = dbo.dbc.countTable('conv')
        self.assertEqual(1104, db_table_count, 
                         msg=error_msg.format(1104, db_table_count))
        



if __name__ == '__main__':
    unittest.main()