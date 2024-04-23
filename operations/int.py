from openeooperation import *
from operationconstants import *
from constants import constants

class Int(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('first.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        self.value = arguments['x']['resolved']
                   
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