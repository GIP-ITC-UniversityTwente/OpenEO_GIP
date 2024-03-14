from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import RasterData
from enum import Enum


class MergeCubes(OpenEoOperation):
    def __init__(self):
         self.loadOpenEoJsonDef('merge_cubes.json') 

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']          
        self.targetRasters = arguments['cube1']['resolved'] 
        self.mergeRasters = arguments['cube2']['resolved']
        self.mergeCases = []
        lenTarget = len(self.targetRasters)
        lenMerge = len(self.mergeRasters)
        if lenTarget != lenMerge:
            self.handleError(toServer, job_id, 'Input raster','Raster can not be merged due to incompatible numbers', 'ProcessParameterInvalid')
        for idx in range(lenTarget):
            targetRaster = self.targetRasters[idx]
            mergeRaster = self.mergeRasters[idx]                            
            self.mergeCases.append({'target' : targetRaster, 'merge': mergeRaster, 'mergeCondition' :self.determineMergeCondition(targetRaster, mergeRaster)})
        self.runnable = True     


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            return None
        lenTarget = len(self.targetRasters)
        for idx in range(lenTarget):
            ilwRaster = self.targerRaster[idx].getRaster().rasterImp()
            rcClone = ilwRaster.clone()
            mergeRaster = self.targerRaster[idx]
            mergeIlwRaster = mergeRaster.getRaster().rasterImp()
            rc = ilwis.do("mergeraster", rcClone, mergeIlwRaster)

        return createOutput('error', "operation not runnable", constants.DTERROR)  
    
    def nameUnique(self, targetBands, name):
        for targetBand in targetBands:
            if name == targetBand['name']:
                return False
        return True
                
    def determineMergeCondition(self, targetRaster : RasterData, mergeRaster : RasterData):
        result = {'nameclash' : [], 'sizesEqual' : False, 'projectionsUnequal' : False}

        
        for mergeBand in mergeRaster.bands:
            result['nameclash'].append({'name': mergeBand['name'], 'unique' : self.nameUnique(targetRaster.bands,mergeBand['name']) })

        result['projectionsUnequal'] = targetRaster.epsg != mergeRaster.epsg
        result['sizesEqual']  = self.checkSpatialDimensions([targetRaster, mergeRaster]) 
       
        return result

def registerOperation():
    return MergeCubes()               