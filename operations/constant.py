from openeooperation import *
from operationconstants import *
from constants import constants

##unclear how to use this process

class Constant(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('constant.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        self.x = arguments['x']['resolved']
                  
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, self.x, self.type2type(self.x))
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Constant()   