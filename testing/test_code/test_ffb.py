import unittest
from test_template import TesterTemplate
class PlayerTester(TesterTemplate):
    def setUp(self):
        self.module_list = ['ffb', 'dbco']
        super().setUp()
    def testPlayerInit(self):
        db = self.databaseObject()
        player_name = ('Tom', 'Brady')
        player = self.modules['ffb'].Player(db, player_name, {})
    def databaseObject(self):
        file_path = self.projectPath + '/data/'
        return self.modules['dbco'].DBConnector(file_path, 'nflstats.db')
if __name__ == '__main__':
    unittest.main()