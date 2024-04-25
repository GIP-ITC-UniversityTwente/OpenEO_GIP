from openeooperation import *
from operationconstants import *
from constants import constants

class AddDimension(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('add_dimension.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
                   
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            return createOutput(constants.STATUSFINISHED, 2, DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return AddDimension()      