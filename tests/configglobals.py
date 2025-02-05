import os

testdir = '/home/mschouwen/testdata/openeo/result'
server = 'http://127.0.0.1:5000'
#server = "http://cityregions.roaming.utwente.nl/openeo"

class TestManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(TestManager, cls).__new__(cls)
        return cls.instance
    
    def init(self):
        os.environ["OPENEO_TEST_COUNT"] = "0"
        os.environ["OPENEO_ERROR_COUNT"] = "0"
        os.environ["OPENEO_ERROR_LIST"] = ""
        os.environ["OPENEO_ERROR_MESSAGES"] = ""

    def incTestCount(self):
        c = int(os.environ["OPENEO_TEST_COUNT"]) 
        c +=1
        os.environ["OPENEO_TEST_COUNT"] = str(c)

    def testCount(self):
        return os.environ["OPENEO_TEST_COUNT"]

    def errorCount(self):
        return os.environ["OPENEO_ERROR_COUNT"]

    def errorMessages(self):
        return os.environ["OPENEO_ERROR_MESSAGES"]
    
    def errorList(self):
        return os.environ["OPENEO_ERROR_LIST"]    

    def addErrorNumber(self, num, msg=''):
        c = int(os.environ["OPENEO_ERROR_COUNT"]) 
        c +=1
        os.environ["OPENEO_ERROR_COUNT"] = str(c)

        en = os.environ["OPENEO_ERROR_LIST"]
        en += ":" + str(num)
        os.environ["OPENEO_ERROR_LIST"] = en
        en = os.environ['OPENEO_ERROR_MESSAGES']
        en += str(num) + ' : ' + msg + '\n'
        os.environ['OPENEO_ERROR_MESSAGES'] = en


def clean_subfolders(folder_path):
# Iterate over all items (files and subfolders) in the specified folder
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        
        if os.path.isfile(item_path):
            if folder_path != testdir: # we dont remove files in the root
                os.remove(item_path)
        elif os.path.isdir(item_path):
            # If it's a subfolder, recursively clean its contents
            clean_subfolders(item_path)

def cleanup():
    clean_subfolders(testdir)
# Iterate over all items (files and subfolders) in the specified folder


        