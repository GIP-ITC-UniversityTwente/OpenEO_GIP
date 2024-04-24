from openeooperation import *
from operationconstants import *
from constants import constants


class Pi(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('pi.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            pi = 3,14159265358979323846
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, pi, DTFLOAT)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Pi()   