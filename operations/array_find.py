from openeooperation import *
from operationconstants import *
from constants import constants

class ArrayFind(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_find.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                 
        self.array = arguments['data']['resolved']
        if not isinstance(self.array, list):
            self.handleError(toServer,job_id, 'array_create', "array must of the 'list' type", 'ProcessParameterInvalid') 
        self.value = arguments['value']['resolved']
        self.reverse = False
        if 'reverse' in arguments:
            self.repeat = arguments['reverse']['resolved']
        self.runnable = True
        self.logEndPrepareOperation(job_id)                    

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            try:
                if self.reverse:
                    idx = self.array.rfind(self.value)
                else:
                    idx =  self.array.index(self.value)
            except ValueError as ex:
                idx = None

            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, idx, DTINTEGER)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return ArrayFind()      