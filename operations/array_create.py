from openeooperation import *
from operationconstants import *
from constants import constants

class ArrayCreate(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_create.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        self.array = arguments['data']['resolved'] 
        self.repeat = 1
        if 'repeat' in arguments:
            self.repeat = arguments['repeat']['resolved']

                  
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            a = []
            for i in range(self.repeat):
                a = a + self.array
            return createOutput(constants.STATUSFINISHED, a, DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return ArrayCreate()                                         
