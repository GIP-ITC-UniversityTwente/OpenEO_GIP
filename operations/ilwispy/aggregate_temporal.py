from openeooperation import *
from operationconstants import *
from constants import constants
from datacube import DataCube
import ilwis
from workflow import processGraph
from globals import getOperation
from datetime import datetime, timedelta
import calendar

def calculate_dekad_number(date_obj):
    # Calculate the day of the month
    day_of_month = date_obj.day
    month = date_obj.month

    # Determine the dekad number based on the day of the month
    if day_of_month <= 10:
        dekad_number = 1
    elif day_of_month <= 20:
        dekad_number = 2
    else:
        dekad_number = 3

    return 3 * (month-1) + dekad_number

def iso_season(beginDate, endDate):
    current_date = beginDate

def iso_week_range(begin_year, begin_week, end_year, end_week):
    # Create datetime objects for the beginning and end of the range
    begin_date = datetime.strptime(f"{begin_year}-W{begin_week}-1", "%Y-W%U-%w")
    end_date = datetime.strptime(f"{end_year}-W{end_week}-1", "%Y-W%U-%w")

    # Iterate through the weeks and add ISO periods to the list
    current_date = begin_date
    date_pairs = []
    while current_date <= end_date:
        week_end = current_date + timedelta(days=6)
        date_pairs.append([current_date.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")])
        current_date += timedelta(weeks=1)

    return date_pairs

def iso_month_range(begin_year, begin_month, end_year, end_month):
    # Initialize the list to store date pairs
    date_pairs = []

    # Create datetime objects for the beginning and end of the range
    begin_date = datetime(begin_year, begin_month, 1)
    end_date = datetime(end_year, end_month, 1)

    # Iterate through the months and add date pairs to the list
    current_date = begin_date
    while current_date <= end_date:
        # Calculate the end of the month
        next_month = current_date.replace(day=28) + timedelta(days=4)
        month_end = next_month - timedelta(days=next_month.day)
        date_pairs.append([current_date.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")])
        current_date = next_month

    return date_pairs

def dateRangeDekad(date):
    day_of_month = date.day
    month = date.month

    # Determine the dekad number based on the day of the month
    if day_of_month <= 10:
        d1 = 1
        d2 = 10
    elif day_of_month <= 20:
        d1 = 11
        d2 = 20
    else:
        d1 = 21
        d2 = calendar.monthrange(date.year, month)[1]

    return (d1,d2)        

def iso_dekad_range(dateBegin, dateEnd):

    # Initialize the list to store date pairs
    date_pairs = []

    dbegin = dateRangeDekad(dateBegin)
    dend = dateRangeDekad(dateEnd)
    begin_date = date(dateBegin.year, dateBegin.month, dbegin[0]) 
    end_date = date(dateEnd.year, dateEnd.month, dend[1]) 

    # Iterate through the dekads and add date pairs to the list
    current_date = begin_date
    while current_date <= end_date:
        rng = dateRangeDekad(current_date)
        s1 = "{0}-{1}-{2}".format(current_date.year, current_date.month, rng[0])
        s2 = "{0}-{1}-{2}".format(current_date.year, current_date.month, rng[1])
        date_pairs.append([s1, s2])
        current_date = current_date + timedelta(days=rng[1] - rng[0] + 1)
  
    return date_pairs

class AggregateTemporal(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('aggregate_temporal.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)
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
            setWorkingCatalog(raster, self.name)
            dt1 = parser.parse(dbegin) 
            dt2 =  parser.parse(dend)
            dr1 = parser.parse(raster[TEMPORALEXTENT][0])
            dr2 = parser.parse(raster[TEMPORALEXTENT][1])
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
        self.logEndPrepareOperation(job_id)                       


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
                    for rimpl in raster.getRasters():
                        rc = ilwis.do("selection", rimpl,"with: " + layerIndexes) 
                        rasters.append(rc)
                    common.registerIlwisIds(openeojob.job_id,rasters)                        
                    self.extra[TEMPORALEXTENT] = interv
                    # translate the ilwis raster to a rasterdata as we need to pass it to the 
                    # reducer graph which assumes all input are rasterdata (in this case)
                    rd = self.makeOutput(rasters, self.extra)
                    pgraph = self.reducer['process_graph']
                    # start the reducer graph
                    self.args['data'] = {'base' : '?', 'resolved' : rd}
                    process = processGraph.ProcessGraph(pgraph, self.args, getOperation)
                    output =  process.run(openeojob, processOutput, processInput)
                    # get the produced rasters
                    rasterDatas = list(output['value'][0].getRasters())
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
                self.extra[TEMPORALEXTENT] = self.tempExtent
                self.extra['textsublayers'] = tmpExtents

                outputRasters.extend(self.makeOutput(aggRasters, self.extra))
                if len(self.labels) > 0:
                    outputRasters[count].setLabels(DIMTEMPORALLAYER, self.labels)
                out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)  
            
            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            return out

             #process.addLocalArgument(DIMENSIONSLABEL,  {'base' : self.dimension, 'resolved' : self.dimension})


class AggregateTemporalPeriod(AggregateTemporal):
    def __init__(self):
        self.loadOpenEoJsonDef('aggregate_temporal_period.json')
        
        self.kind = constants.PDPREDEFINED

  
    
    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)      
        inputRaster = arguments['data']['resolved']
        raster = inputRaster[0]
        tempExtent = raster[TEMPORALEXTENT]
        dr1 = parser.parse(tempExtent[0])
        dr2 = parser.parse(tempExtent[1])
        year1 = dr1.isocalendar()[0]
        year2 = dr2.isocalendar()[0]        
        period = arguments['period']['resolved']
        periods = []
        if period == 'day':
            periods = iso_week_range(year1, dr1.isocalendar()[1], year2,  dr2.isocalendar()[1])
        elif period == 'month':
            periods = iso_month_range(year1, dr1.month, year2, dr2.month)
        elif period == 'dekad':
            periods = iso_dekad_range(dr1, dr2)
        elif period == 'season':
            periods = iso_season(dr1, dr2)
        else:
           self.handleError(toServer, job_id, 'temporalperiodtag','period tag not supported', 'ProcessParameterInvalid')


        arguments['intervals'] = {'base' : period, 'resolved' : periods}
        super().prepare(arguments)
        self.logEndPrepareOperation(job_id)             





def registerOperation():
   funcs = []
   funcs.append(AggregateTemporal())
   funcs.append(AggregateTemporalPeriod())   

   return funcs                        