from openeooperation import *
from operationconstants import *
from constants import constants

class DimensionLabels(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('dimension_labels.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        self.data = arguments['data']['resolved'][0]
        self.dimension = self.mapname(arguments['dimension']['resolved'])
        if not self.dimension in self.data[STRUCTUREDEFDIM]:
            self.handleError(toServer, job_id, 'Input dimension',"unknown dimension:" + arguments['dimension']['resolved'], 'ProcessParameterInvalid')
   
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            result = []
            if 'labels' in self.data[METADATDEFDIM][self.dimension]:
                result = self.data[METADATDEFDIM][self.dimension]['labels']
                
            self.logEndOperation(processOutput,openeojob)                
            return createOutput(constants.STATUSFINISHED, result, DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return DimensionLabels()      