from openeooperation import *
from operationconstants import *
from constants import constants

class Clip(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('clip.json')
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
                       
        self.runnable = True
        self.logEndPrepareOperation(job_id)             

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if self.value == None:
                 v = None
            else:
                v = self.value
                if v < self.min:
                    v = self.min
                if v > self.max:
                    v = self.max                                     
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, v, self.type2type(v))
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Clip()   