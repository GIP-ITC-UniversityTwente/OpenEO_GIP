from openeooperation import *
from operationconstants import *
from constants import constants
from workflow import processGraph
from globals import getOperation
import common
import copy

class ReduceDimensionsOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('reduce_dimension.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        self.reducer= arguments['reducer']['resolved']
        self.data = arguments['data']
        self.dimension = arguments['dimension']['resolved']
        self.runnable = True
        return ""
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            pgraph = self.reducer['process_graph']
            process = processGraph.ProcessGraph(pgraph, {'data' : self.data}, getOperation)
            output =  process.run(openeojob, processOutput, processInput)
            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, output['value'], self.type2type(output))
        message = common.notRunnableError(self.name, openeojob.job_id)         
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return ReduceDimensionsOperation()
  




