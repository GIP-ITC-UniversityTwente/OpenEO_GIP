from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import RasterData
from globals import getOperation
from workflow.processGraph import ProcessGraph
import copy


class MergeCubes(OpenEoOperation):
    def __init__(self):
         self.loadOpenEoJsonDef('merge_cubes.json') 

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id'] 
        self.rasterSizesEqual = True                     
        self.targetRasters = arguments['cube1']['resolved'] 
        self.mergeRasters = arguments['cube2']['resolved']
        if 'overlap_resolver' in arguments:
            self.overlap_resolver = arguments['overlap_resolver']
        fixedCount = -1
        fixedCount = self.checkSublayerCount(toServer, job_id, self.targetRasters, fixedCount)
        self.checkSublayerCount(toServer, job_id, self.mergeRasters, fixedCount)
        self.mergeCases = self.checkMergeConditions(toServer, job_id) 
                                    
        self.runnable = True 

    def checkMergeConditions(self, toServer, job_id):
        mergeCases = []
        usedEpsg = -1
        for idx in range(len(self.targetRasters)):
            targetRaster = self.targetRasters[idx]
            if usedEpsg == -1:
                usedEpsg = targetRaster.epsg
            else:
                if usedEpsg != targetRaster.epsg:
                    self.handleError(toServer, job_id, 'Input raster', 'merge not possible. different geometries in target cube', 'ProcessParameterInvalid')                 
            for idx2 in range(len(self.mergeRasters)):
                mergeRaster = self.mergeRasters[idx2]                            
                mergeCases.append({'target' : targetRaster, 'merge': mergeRaster, 'mergeCondition' :self.determineMergeCondition(targetRaster, mergeRaster)})
        return mergeCases

    def checkSublayerCount(self, toServer, job_id, rasters, fixedCount):
        for rd in rasters:
            for rl in rd.layers:
                if fixedCount == -1:
                    fixedCount = rl.sublayerCount
                else:
                    if fixedCount != rl.sublayerCount:
                        self.handleError(toServer, job_id, 'Input raster','Raster can not be merged due to incompatible count', 'ProcessParameterInvalid')    
        return fixedCount                        

   


    def run(self,openeojob, toServer, fromServer):
        if self.runnable:
            self.logStartOperation(toServer, openeojob)
            outputRasters = [] 
            allUnique = True  
            for mc in self.mergeCases:
                nc = mc['mergeCondition']['nameclash']
                allUnique = allUnique and not nc
                se = mc['mergeCondition']['sizesEqual']
                if not se:
                    resampledRaster = self.resample(mc['target'], mc['merge'])
                    mc['merge'] = resampledRaster
            if allUnique:
                for raster in self.mergeRasters:
                    outputRasters.append(raster)
            else:
                if not hasattr(self, 'overlap_resolver'):
                    self.handleError(openeojob.job_id, toServer, 'overlap_resolver', 'missing process graph', 'ProcessParameterInvalid')

                for mc in self.mergeCases:
                    if mc['mergeCondition']['nameclash']:
                        outputRasters.append(self.combineRasters(openeojob, toServer, fromServer, mc))
                    else:
                        outputRasters.append(mc['merge'])                        
                    
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)

        message = common.notRunnableError(self.name, openeojob.job_id)
        return createOutput('error', message, constants.DTERROR) 

    def combineRasters(self, openeojob, toServer, fromServer, mc):
        args = []
        args.append(mc['target'].getRaster().rasterImp())
        args.append(mc['merge'].getRaster().rasterImp())
        mergedRaster = self.collectRasters(args)
        extra = self.constructExtraParams(mc['target'], mc['target'].temporalExtent, 0)
        rasterData = RasterData()
        rasterData.load(mergedRaster, 'ilwisraster', extra )

        pgraph = self.overlap_resolver['resolved']['process_graph']
        copyPg = copy.deepcopy(pgraph)
        first = next(iter(copyPg))
        copyPg[first]['arguments'] = {'data' : [rasterData]}
        process = ProcessGraph(copyPg, [rasterData], getOperation)
        oInfo = process.run(openeojob, toServer, fromServer)  
        return oInfo['value'][0]
            
        
    
    def nameUnique(self, targetBands, name):
        for targetBand in targetBands:
            if name == targetBand['name']:
                return False
        return True
                
    def determineMergeCondition(self, targetRaster : RasterData, mergeRaster : RasterData):
        result = {'nameclash' : False, 'sizesEqual' : False, 'projectionsUnequal' : False}

        
        for mergeBand in mergeRaster.bands:
            result['nameclash'] = not self.nameUnique(targetRaster.bands,mergeBand['name']) 

        result['projectionsUnequal'] = targetRaster.epsg != mergeRaster.epsg
        result['sizesEqual']  = self.checkSpatialDimensions([targetRaster, mergeRaster]) 
       
        return result

def registerOperation():
    return MergeCubes()               