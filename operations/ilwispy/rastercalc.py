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
        """
        Prepares the RasterCalc operation by processing input arguments and setting up the operation.

        Args:
            arguments: The input arguments for the operation.
        """
        toServer, job_id = self._initializePreparation(arguments)
        rasterParms = self._processRasterParameters(arguments)
        self.finalRasters = self._processFinalRasters(rasterParms)
        self.expr, self.rest = self._prepareExpression(arguments, self.finalRasters)
        self._finalizePreparation(rasterParms, job_id)


    def _initializePreparation(self, arguments):
        """
        Initializes the preparation process by extracting default arguments.

        Args:
            arguments: The input arguments for the operation.

        Returns:
            A tuple containing the server object and job ID.
        """
        toServer, job_id = self.getDefaultArgs(arguments)
        self.logStartPrepareOperation(job_id)
        return toServer, job_id

    def _processRasterParameters(self, arguments):
        """
        Processes the 'v' argument to extract raster parameters.

        Args:
            arguments: The input arguments for the operation.

        Returns:
            A list of raster parameters.
        """
        if isinstance(arguments['v']['resolved'], dict):
            return list(arguments['v']['resolved'].items())
        else:
            org = arguments['v']['base']
            name = org['from_node'][0]
            return [(name, arguments['v']['resolved'])]

    def _processFinalRasters(self, rasterParms):
        """
        Processes the final rasters by resampling if necessary.

        Args:
            rasterParms: A list of raster parameters.

        Returns:
            A dictionary of final rasters.
        """
        finalRasters = {}
        firstRaster = rasterParms[0][1]
        finalRasters[rasterParms[0][0]] = firstRaster[0].getRaster()

        for raster in rasterParms[1:]:
            if self.needResample(firstRaster[0], raster[1][0]):
                newRaster = self.resample(firstRaster[0], raster[1][0])
                finalRasters[raster[0]] = newRaster
            else:
                finalRasters[raster[0]] = raster[1][0].getRaster()

        return finalRasters

    def _prepareExpression(self, arguments, finalRasters):
        """
        Prepares the mapcalc expression and the rest string.

        Args:
            arguments: The input arguments for the operation.
            finalRasters: A dictionary of final rasters.

        Returns:
            A tuple containing the mapcalc expression and the rest string.
        """
        expr = arguments['expression']['resolved']
        rest = ''
        count = 1

        for key, raster in finalRasters.items():
            expr = expr.replace(key, '@' + str(count))
            if rest != '':
                rest += ','
            rest += f'self.finalRasters["{key}"]'
            count += 1

        expr = f'ilwis.do("mapcalc", "{expr}", {rest})'
        return expr, rest

    def _finalizePreparation(self, rasterParms, job_id):
        """
        Finalizes the preparation process by setting up the working catalog and marking the operation as runnable.

        Args:
            rasterParms: A list of raster parameters.
            job_id: The job ID for logging.
        """
        firstRaster = rasterParms[0][1]
        self.createExtra(firstRaster[0], basename=self.name)
        setWorkingCatalog(firstRaster[0], self.name)
        self.runnable = True
        self.logEndPrepareOperation(job_id)
 
    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outputRasters = []             
            outputs = []             
            outputRc = eval(self.expr)
            outputs.append(outputRc)
            common.registerIlwisIds(openeojob.job_id, outputs)  
            outputRasters.extend(self.makeOutput(outputs, self.extra))
            out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)    
            self.logEndOperation(processOutput, openeojob)          
            return out
        
        return createOutput('error', "operation not runnable", constants.DTERROR)
        
def registerOperation():
     return RasterCalc()