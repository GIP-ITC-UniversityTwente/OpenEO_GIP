from openeooperation import *
from operationconstants import *
from constants import constants

class First(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('first.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments)
        self.logStartPrepareOperation(job_id)  
        self.array = arguments['data']['resolved']
        self.ignore_data = True
        if 'ignore_data' in arguments:
            self.ignore_data = arguments['igonre_data']['resolved']
                 
        self.runnable = True  
        self.logEndPrepareOperation(job_id)          

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            b = None
            if len(self.array) > 0:
                if self.ignore_data:
                    for e in self.array:
                        if e != None:
                            b = e
                            break
                else:
                    b = self.array[0]
                
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, b, self.type2type(b))
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return First()                                         
