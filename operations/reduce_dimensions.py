from openeooperation import *
from operationconstants import *
from constants import constants
from workflow import processGraph
from globals import getOperation
import common

class ReduceDimensionsOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('reduce_dimension.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        self.reducer= arguments['reducer']
        self.data = arguments['data']
        self.runnable = True
        return ""
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if self.reducer['resolved'] == None:
                pgraph = self.reducer['process_graph']
                args = self.data['base']
                process = processGraph.ProcessGraph(pgraph, args, getOperation)
                output =  process.run(openeojob, processOutput, processInput)
                self.logEndOperation(processOutput,openeojob)
                return output
            else:
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, self.reducer['resolved'], constants.DTRASTER)
        message = common.notRunnableError(self.name, openeojob.job_id)         
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return ReduceDimensionsOperation()
  




