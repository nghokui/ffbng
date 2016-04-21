import os, json
from code.csvdb import DBLoader

def transferCSVtoSQL():
    project_directory = os.path.dirname(os.path.realpath(__file__))
    settings_path = os.path.join(project_directory, 'settings/settings.json')
    with open(settings_path, 'r') as jfile:
        settings = json.load(jfile)

    dblo = DBLoader(settings)
    dblo.transfer()

    print("Transfer complete.")



if __name__ == '__main__':
    transferCSVtoSQL()