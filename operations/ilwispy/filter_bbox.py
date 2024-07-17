from openeooperation import *
from operationconstants import *
from constants import constants

class FilterBBox(OpenEoOperation):
        def __init__(self):
                self.loadOpenEoJsonDef('filter_bbox.json')
                
                self.kind = constants.PDPREDEFINED

        def prepare(self, arguments):
                self.runnable = False
                if 'serverChannel' in arguments:
                        toServer = arguments['serverChannel']
                        job_id = arguments['job_id']
                self.data = self.getMandatoryParam(toServer, job_id, arguments, 'data')
                
                if len(self.data) == 0:
                    self.handleError(toServer, job_id, "parameter", "Input data is empty", 'ProcessParameterInvalid')
                if not isinstance(self.data[0], RasterData):
                    self.handleError(toServer, job_id, "parameter", "Input data is not rasterdata", 'ProcessParameterInvalid')

                self.data = self.data[0]
                ext = bb = self.getMandatoryParam(toServer, job_id, arguments, 'extent')
                crs = 4326
                if isinstance(bb, list):
                        if len(bb) < 4:
                                self.handleError(toServer, job_id, "bounding box", "Input bounding box has insufficient data", 'ProcessParameterInvalid')
                        ext = {'west' : bb[0], 'south' : bb[1], 'east' : bb[2], 'north' : bb[3]}
                        if len(bb) == 5:
                                ext['crs'] = bb[4]
                if isinstance(ext, dict):
                        self.checkSpatialExt(toServer, job_id, ext)
                        if 'crs' in bb:
                                crs = bb['crs']
                else:
                        self.handleError(toServer, job_id, "bounding box", "Invalid extents deifinition", 'ProcessParameterInvalid')                                      
                
                self.env = ilwis.Envelope(ilwis.Coordinate(ext['west'], ext['south']), ilwis.Coordinate(ext['east'], ext['north']))
                if self.data['proj:epsg'] != crs:
                       csyExt = ilwis.CoordinateSystem("epsg:" + str(crs))
                       csyData = ilwis.CoordinateSystem("epsg:" + self.data['proj:epsg'])
                       self.env = csyData.convertEnvelope(csyExt, self.env)
                key = next(iter(self.data[DATAIMPLEMENTATION]))
                rband = self.data[DATAIMPLEMENTATION][key] 
                ss = str(rband.envelope())                                         
                self.checkOverlap(toServer, job_id, self.env, rband.envelope())
                self.runnable = True



        def run(self,openeojob, processOutput, processInput):
                if self.runnable:
                        outputRasters = []
                        ilwRasters = []
                        bandIdxs = self.data.getBandIndexes([])
                        for bandIndex in bandIdxs:
                                for raster in self.data[DATAIMPLEMENTATION].values(): 
                                        v = str(self.env.minCorner().x) + " " + str(self.env.minCorner().y) + "," + str(self.env.maxCorner().x) + " " + str(self.env.maxCorner().y) 
                                        rc = ilwis.do("selection", raster, "envelope(" + v + ")") 
                                        ilwRasters.append(rc)
                        self.createExtra(self.data)
                        common.registerIlwisIds(ilwRasters)  
                        outputRasters.append(self.createOutput(0, ilwRasters, self.extra))

                        self.logEndOperation(processOutput,openeojob, outputs=outputRasters)                      
                        return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTLIST)
                    
                return createOutput('error', "operation no runnable", constants.DTERROR)  

def registerOperation():
     return FilterBBox()    