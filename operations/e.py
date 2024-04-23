from openeooperation import *
from operationconstants import *
from constants import constants

##unclear how to use this process

class E(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('e.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            e = 2.71828182845904523536028747135266249
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, e, DTFLOAT)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return E()   