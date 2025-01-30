from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config
from workflow import processGraph
from globals import getOperation


class RasterCalc(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('rastercalc.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
        self.finalRasters = {}
        if isinstance(arguments['v']['resolved'], dict):
            rasterParms = list(arguments['v']['resolved'].items())
        else: # special case; only one element in a parameter value; as the one value per parm is the default for all most all operations this
            # is used everywhere else. This operation may have mutiple values per parameter so we need a 'translation' 
            org = arguments['v']['base']
            name = org['from_node'][0]
            rasterParms = [(name, arguments['v']['resolved'])]

        item = rasterParms[0]           
        firstRaster = item[1]
        self.finalRasters[item[0]] = firstRaster[0].getRaster()
        for raster in rasterParms[1:]:
            if self.needResample(firstRaster[0], raster[1][0]):
                newRaster =  self.resample(firstRaster[0], raster[1][0])
                self.finalRasters[raster[0]] = newRaster
            else:
                self.finalRasters[raster[0]] = raster[1][0].getRaster()  
        self.expr = arguments['expression']['resolved']
        self.rest = ''
        count = 1
        for r in self.finalRasters.items():
            self.expr = self.expr.replace(r[0], '@' + str(count))
            if self.rest != '':
                self.rest = self.rest + ','
            key = r[0]                
            self.rest = self.rest + 'self.finalRasters["' + key + '"]'
            count = count + 1
        self.expr = 'ilwis.do("mapcalc","' + self.expr + '",' + self.rest + ')'
        self.createExtra(firstRaster[0], basename=self.name) 
        setWorkingCatalog(firstRaster[0]) 
        self.runnable = True
    
             


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outputRasters = []             
            outputs = []             
            outputRc = eval(self.expr)
            outputs.append(outputRc)
            common.registerIlwisIds(outputs)  
            outputRasters.extend(self.makeOutput(outputs, self.extra))
            out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)    
            self.logEndOperation(processOutput, openeojob)          
            return out
        
        return createOutput('error', "operation not runnable", constants.DTERROR)
        
def registerOperation():
     return RasterCalc()