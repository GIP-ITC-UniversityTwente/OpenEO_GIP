from openeooperation import *
from operationconstants import *
from constants import constants

class ArrayCreate(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_create.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                
        self.array = arguments['data']['resolved'] 
        self.repeat = 1
        if 'repeat' in arguments:
            self.repeat = arguments['repeat']['resolved']

                  
        self.runnable = True
        self.logEndPrepareOperation(job_id)                    

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            a = []
            for i in range(self.repeat):
                a = a + self.array
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, a, DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return ArrayCreate()                                         
