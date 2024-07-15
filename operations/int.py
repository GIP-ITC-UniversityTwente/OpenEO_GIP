from openeooperation import *
from operationconstants import *
from constants import constants
import numbers

class Int(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('first.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']        
        self.value = arguments['x']['resolved']
        if not isinstance(self.value, numbers.Number):
            self.handleError(toServer, job_id, "x", "Value is not a number " + str(self.value), 'ProcessParameterInvalid')         
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            b = self.value
            if b != None:
                b = int(self.value)
                
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, b, self.type2type(b))
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Int()       