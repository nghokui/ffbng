import os, unittest, imp

class TesterTemplate(unittest.TestCase):
    def setUp(self):
        self.modules, pkg = {}, 'code'

        current_dir = os.path.dirname(os.path.realpath(__file__))
        self.projectPath = '/'.join(current_dir.split('/')[:-2])

        file, path, description = imp.find_module(pkg, [self.projectPath])
        package = imp.load_module(pkg, file, path, description)

        module_path = '{}/{}'.format(self.projectPath, pkg)
        for mod in self.module_list:
            file, path, description = imp.find_module(mod, [module_path])
            self.modules[mod] = imp.load_module(mod, file, path, description)
            file.close()
