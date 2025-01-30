from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config
from workflow import processGraph
from globals import getOperation


class ApplyOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('apply.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = True
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)        
        self.apply = arguments['process']['base']
        self.pgraph = self.apply['process_graph']
        rootNode = next(iter(self.pgraph))
        args = self.pgraph[rootNode]['arguments'] 
        self.args = {} 
        for key, value in args.items(): 
            if isinstance(value, dict) and 'from_parameter' in value:
                self.args = { key : arguments['data']}
                
        self.logEndPrepareOperation(job_id)      


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if self.apply != None:
                process = processGraph.ProcessGraph(self.pgraph, self.args, getOperation)
                return process.run(openeojob, processOutput, processInput)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return ApplyOperation()