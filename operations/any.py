from openeooperation import *
from operationconstants import *
from constants import constants


class Any(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('any.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False 
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                   
        if 'data' in arguments and isinstance(arguments['data']['resolved'], list):
            self.data = arguments['data']['resolved']
            for b in self.data:
                if type(b) != bool:
                    self.handleError(toServer, job_id, 'Input list','Input may only contain bools', 'ProcessParameterInvalid')
            self.runnable = True
        else:
            self.handleError(toServer, job_id, 'Input list','Input must be a list', 'ProcessParameterInvalid')
        self.logEndPrepareOperation(job_id)            

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            result = True
            for b in self.data:
                if b == False:
                    result = result or b  
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, result, DTBOOL)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Any()   