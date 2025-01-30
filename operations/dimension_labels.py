from openeooperation import *
from operationconstants import *
from constants import constants

class DimensionLabels(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('dimension_labels.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                
        self.data = arguments['data']['resolved'][0]
        self.dimension = self.mapname(arguments['dimension']['resolved'])
        if not self.dimension in self.data[DIMENSIONSLABEL]:
            self.handleError(toServer, job_id, 'Input dimension',"unknown dimension:" + arguments['dimension']['resolved'], 'ProcessParameterInvalid')
   
        self.runnable = True 
        self.logEndPrepareOperation(job_id)           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            result = self.data.getLabels(self.dimension)
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, result, DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return DimensionLabels()      