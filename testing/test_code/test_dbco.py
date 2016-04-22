import unittest
from test_template import TesterTemplate

class DatabaseConnectionTester(TesterTemplate):
    def setUp(self):
        self.module_list = ['dbco']
        super().setUp()
    def testSelectQueryNoCondition(self):
        dbc = self.dbObject()
        table_name, fields = 'player', ['player', 'col']
        res = dbc.queryTable(table_name, fields)
        self.assertTrue(len(res) > 0)
    def testSelectQuerySingleCondition(self):
        dbc = self.dbObject()
        table_name, fields = 'player', ['player', 'col']
        condition = [('pname', 'T.Brady')]
        res = dbc.queryTable(table_name, fields, condition)
        self.assertEqual(1, len(res), msg='Multiple results returned.')
        self.assertEqual('TB-2300', res[0][0], msg='ID field not correct.')
        self.assertEqual('Michigan', res[0][1], msg='Col field not correct.')
    def testSelectQueryMultipleCondition(self):
        dbc = self.dbObject()
        table_name, fields = 'player', ['player', 'col']
        condition = [('pname', 'T.Brady'), ('pos1', 'QB')]
        res = dbc.queryTable(table_name, fields, condition)
        self.assertEqual(1, len(res), msg='Multiple results returned.')
        self.assertEqual('TB-2300', res[0][0], msg='ID field not correct.')
        self.assertEqual('Michigan', res[0][1], msg='Col field not correct.')
    def dbObject(self):
        db_path, db_name = self.projectPath + '/data/', 'nflstats.db'
        return self.modules['dbco'].DBConnector(db_path, db_name)

if __name__ == '__main__':
    unittest.main()