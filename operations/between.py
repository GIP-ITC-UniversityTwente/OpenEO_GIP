from openeooperation import *
from operationconstants import *
from constants import constants

class Between(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('between.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                
        self.min = arguments['min']['resolved']
        self.max = arguments['max']['resolved']
        self.value = arguments['x']['resolved']
        if self.min > self.max:
             self.handleError(toServer, job_id, 'min/max',"minimum must be smaller than maximum", 'ProcessParameterInvalid')
        self.exclude_max = False             
        if 'exclude_max' in arguments:
             self.exclude_max = arguments['exclude_max']['resolved']
                  
        self.runnable = True   
        self.logEndPrepareOperation(job_id)       

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            b = False
            if self.exclude_max:
                 b = self.min <= self.value and self.max > self.value
            else:
                 b = self.min <= self.value and self.max >= self.value
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, b, DTBOOL)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Between()   