from openeooperation import *
from operationconstants import *
from constants import constants
import math

class Is_Nan(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('is_nan.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)         
        self.value = arguments['x']['resolved']
                   
        self.runnable = True 
        self.logEndPrepareOperation(job_id)           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            v = math.isnan(self.value)
                
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, v, constants.DTBOOL)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Is_Nan()       