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
        self.dimension = arguments['dimension']['resolved']
        pgraph = self.reducer['process_graph']
        rootNode = next(iter(pgraph))
        args = pgraph[rootNode]['arguments'] 
        self.args = {} 
        for key, value in args.items(): 
            if isinstance(value, dict) and 'from_parameter' in value:
                self.args[key] =  arguments['data']
        self.runnable = True
        return ""
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            pgraph = self.reducer['process_graph']
            process = processGraph.ProcessGraph(pgraph, self.args, getOperation)
            process.addLocalArgument('dimensions',  {'base' : self.dimension, 'resolved' : self.dimension})
        
            output =  process.run(openeojob, processOutput, processInput)
            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, output['value'], self.type2type(output))
        message = common.notRunnableError(self.name, openeojob.job_id)         
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return ReduceDimensionsOperation()
  




