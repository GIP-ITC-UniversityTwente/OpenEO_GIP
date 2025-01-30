from openeooperation import *
from operationconstants import *
from constants import constants
from globals import getOperation
from workflow.processGraph import ProcessGraph
import copy

# for the moment unclear how to call this operation

class ApplyDimension(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('apply_dimension.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments)
        self.logStartPrepareOperation(job_id)                
        self.data = self.getMandatoryParam(toServer, job_id, arguments, 'data')
        self.process_graph  = self.getMandatoryParam(toServer, job_id, arguments, 'process')
        self.dimension = self.getMandatoryParam(toServer, job_id, arguments, 'dimension')

        self.runnable = True
        self.logEndPrepareOperation(job_id)                   

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            pgraph = self.process_graph['process_graph']
            copyPg = copy.deepcopy(pgraph)
            process = ProcessGraph(copyPg, [], getOperation)
            # the 'data' parameter must be matched against the name of the parameter needed
            # for the 'process function. So we look witch name in the arguments of the process function
            # has as input 'data', which is its equivalent when 'apply_dimension' is called (so from_parameter)
            first = next(iter(copyPg))
            args = copyPg[first]['arguments']
            matchingName = 'data'
            for item in args.items():
                if isinstance(item[1], dict):
                    if 'from_parameter' in item[1] and item[1]['from_parameter'] == 'data':
                        matchingName = item[0]
            process.addLocalArgument(matchingName,  {'base' : '?', 'resolved' :self.data})
            process.addLocalArgument('dimension',  {'base' : '?', 'resolved' : self.dimension})                
            oInfo = process.run(openeojob, processOutput, processInput)
            outputs = oInfo['value'] 
            return createOutput(constants.STATUSFINISHED, outputs, constants.DTRASTERLIST)                    

        return createOutput('error', "operation no runnable", constants.DTERROR )  


def registerOperation():
   return ApplyDimension()   