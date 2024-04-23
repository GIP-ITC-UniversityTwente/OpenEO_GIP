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
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        self.data = arguments['data']['resolved']
        self.process_graph  = arguments['process']['resolved']

        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            
            for rasterData  in self.data:
                outputRasters = []
              
                pgraph = self.process_graph['process_graph']
                copyPg = copy.deepcopy(pgraph)
                processArgs = {"data" : {"resolved": [rasterData]}}
                process = ProcessGraph(copyPg, processArgs, getOperation)
                oInfo = process.run(openeojob, processOutput, processInput) 
                outputRasters.append(oInfo['value'][0])
        createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)                    

        return createOutput('error', "operation no runnable", constants.DTERROR )  


def registerOperation():
   return ApplyDimension()   