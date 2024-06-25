from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import RasterData
import ilwis
from workflow import processGraph
from globals import getOperation

class AggregateTemporal(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('aggregate_temporal.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
        self.inputRaster = arguments['data']['resolved']
        self.dates = arguments['intervals']['resolved']
        if len(self.dates) == 0:
            self.handleError(toServer, job_id, 'intervals','number of intervals must be larger than 0', 'ProcessParameterInvalid')
                
        dbegin = self.dates[0][0]
        dend = self.dates[-1][1]
        self.reducedBands = []
        self.labels = []
        if 'labels' in arguments:
            self.labels = arguments['labels']['resolved']
            if len(self.labels) != len(self.dates):
                self.handleError(toServer, job_id, 'labels','number of labels must match number of intervals', 'ProcessParameterInvalid')    
        # check if given temporal selection fits the actual data in the raster
        for raster in self.inputRaster:
            dt1 = parser.parse(dbegin) 
            dt2 =  parser.parse(dend)
            dr1 = parser.parse(raster['temporalExtent'][0])
            dr2 = parser.parse(raster['temporalExtent'][1])
            if dt1 > dt2:
                self.handleError(toServer, job_id, 'temporal intervals','invalid intervals', 'ProcessParameterInvalid')
            if (dt1 < dr1 and dt2 < dr1) or ( dt1 > dr2 and dt2 > dr2):
                self.handleError(toServer, job_id, 'temporal extents','intervals dont overlap', 'ProcessParameterInvalid')
            bands = raster.getBands()
            d = {}
            for bnd in bands:
                d[ bnd['name']] = []
            self.reducedBands.append(d)

        self.tempExtent =[dbegin, dend]
        self.reducer= arguments['reducer']['resolved']
        pgraph = self.reducer['process_graph']
        rootNode = next(iter(pgraph))
        args = pgraph[rootNode]['arguments'] 
        self.args = {} 
        for key, value in args.items(): 
            if isinstance(value, dict) and 'from_parameter' in value:
                self.args[key] =  arguments['data']                    

        self.runnable = True                


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            self.createExtra(self.inputRaster[0], False, basename='agg_temp')
            outputRasters = []
            for count, raster in enumerate(self.inputRaster):
                tmpExtents = []
                outputs = {}
                for interv in self.dates:
                    lyrIndexes = raster.getLayerIndexes(interv)
                    layerIndexes = 'rasterbands(' + str(lyrIndexes[0])
                    # construct a string which identifies the temporal layers used in the layer selection
                    for idx in range(1,len(lyrIndexes)):
                        layerIndexes += "," + str(lyrIndexes[idx])
                    layerIndexes += ')'
                    rasters = []
                    # retrieve the selected temporal layers from the whole raster
                    for rimpl in raster[DATAIMPLEMENTATION].values():
                        rc = ilwis.do("selection", rimpl,"with: " + layerIndexes) 
                        rasters.append(rc)
                    self.extra['temporalExtent'] = interv
                    # translate the ilwis raster to a rasterdata as we need to pass it to the 
                    # reducer graph which assumes all input are rasterdata (in this case)
                    rd = self.makeOutput(rasters, self.extra)
                    pgraph = self.reducer['process_graph']
                    # start the reducer graph
                    self.args['data'] = {'base' : '?', 'resolved' : rd}
                    process = processGraph.ProcessGraph(pgraph, self.args, getOperation)
                    output =  process.run(openeojob, processOutput, processInput)
                    # get the produced rasters
                    rasterDatas = list(output['value'][0][DATAIMPLEMENTATION].values())
                    bands = raster.getBands()
                    # register the produced rasters with the correct band. Note that the
                    # produced rasters are per band
                    for count2, bnd in enumerate(bands):
                        self.reducedBands[count][bnd['name']].append(rasterDatas[count2])
                    # register which time interval belongs to this                         
                    tmpExtents.append(interv) 
                aggRasters = [] 
                # merge the rasters (temporal layers) to one ilw raster (which contains the layers)
                for item in self.reducedBands[count].values():
                    aggRasters.append(self.collectRasters(item))                                   
                self.extra['temporalExtent'] = self.tempExtent
                self.extra['textsublayers'] = tmpExtents
                self.extra['rasterkeys'] = list(raster[DATAIMPLEMENTATION].keys())

                outputRasters.extend(self.makeOutput(aggRasters, self.extra))
                if len(self.labels) > 0:
                    outputRasters[count].setLabels(DIMTEMPORALLAYER, self.labels)
                out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)  
            
            self.logEndOperation(processOutput,openeojob)
            return out

             #process.addLocalArgument('dimensions',  {'base' : self.dimension, 'resolved' : self.dimension})



def registerOperation():
   return AggregateTemporal()                            