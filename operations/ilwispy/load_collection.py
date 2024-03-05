from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import *
from common import getRasterDataSets, saveIdDatabase
import ilwis
from pathlib import Path
from eoreader import *
from eoreader.bands import *
import posixpath
import shutil
import common


class LoadCollectionOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('load_collection.json')
        ##print('aaaaaaaaaaaa '+ openeo.testingvar)

        self.kind = constants.PDPREDEFINED
        self.bandIdxs = []
        self.lyrIdxs = []

    def unpack(self, data, folder):
        #os.mkdir(folder)
        reader = Reader()
        prod = reader.open(data)
        prod.output = folder
        unpackFolderName = "unpacked_" + self.inputRaster.id
        unpack_folder = os.path.join(folder, unpackFolderName)
      
        oldoutputs = []
        sourceList = {}
        for band in self.inputRaster.bands:
            bandname = band['normalizedbandname']
            nn = to_band(bandname)
            prod.load(nn)
            outputs = [f for f in prod.output.glob("tmp*/*.tif")]
            s1 = set(oldoutputs)
            s2 = set(outputs)
            diff = list(s2 - s1)
            oldoutputs = outputs
            if len(diff) == 1:
                sourceList[band['name']] = diff[0].name
              

        
        os.rename(posixpath.dirname(outputs[0]), unpack_folder)
        return sourceList, unpackFolderName


    def prepare(self, arguments):
        try:
            self.runnable = False 
            processOutput = None
            job_id = None
            if 'serverChannel' in arguments:
                processOutput = arguments['serverChannel']
                job_id = arguments['job_id']
                           
            fileIdDatabase = getRasterDataSets()          
            self.inputRaster = fileIdDatabase[arguments['id']['resolved']]
            if self.inputRaster == None:
                return "NotFound"
            
            self.dataSource = ''
            oldFolder = folder = self.inputRaster.dataFolder
            if  self.inputRaster.type == 'file':
                self.logProgress(processOutput, job_id,"load collection : transforming data", constants.STATUSRUNNING)                   
                folder = self.transformOriginalData(fileIdDatabase, folder, oldFolder)                  
                
            
            if 'bands'in arguments :
                if arguments['bands']['resolved'] != None:
                    self.bandIdxs = self.inputRaster.getBandIndexes(arguments['bands']['resolved'])
                else:
                    self.bandIdxs.append(0)
            else:
                self.bandIdxs.append(0)

            if 'temporal_extent' in arguments:
                self.temporalExtent = arguments['temporal_extent']['resolved']
                #if arguments['temporal_extent']['resolved'] != None:
                self.lyrIdxs = self.inputRaster.getLayerIndexes(arguments['temporal_extent']['resolved'])
               # else:
               #     self.lyrIdxs.append(0)  
            path = Path(folder).as_uri()
            ilwis.setWorkingCatalog(path)

            if 'spatial_extent' in arguments:
                sect = arguments['spatial_extent']['resolved']
                if sect != None:
                    sext = [sect['west'], sect['south'], sect['east'], sect['north']]
                    if self.inputRaster.grouping == 'layer':
                        source = self.inputRaster.layers[0].dataSource
                    else:
                        source = self.inputRaster.bands[0].source                        
                    datapath = os.path.join(path, source)                            
                    rband = ilwis.RasterCoverage(datapath)
                    csyLL = ilwis.CoordinateSystem("epsg:4326")
                    llenv = ilwis.Envelope(ilwis.Coordinate(sect['west'], sect['south']), ilwis.Coordinate(sect['east'], sect['north']))
                    e = str(rband.coordinateSystem().convertEnvelope(csyLL, llenv))
                    env = e.split(' ')
                    self.inputRaster.spatialExtent = [env[0], env[2], env[1], env[3]]
           
            self.runnable = True
            self.rasterSizesEqual = True
 
        except Exception as ex:
            return ""

        return ""

    def transformOriginalData(self, fileIdDatabase, folder, oldFolder):
        self.dataSource = self.inputRaster.dataSource
                
        sourceList, unpack_folder = self.unpack(self.dataSource, folder)
        for band in self.inputRaster.bands:
            source = sourceList[band['name']]
            band['source'] = source

        folder = os.path.join(folder,unpack_folder)                     
        self.inputRaster.dataFolder = folder
        self.dataSource = folder
        newDataSource = self.inputRaster.toMetadataFile(oldFolder)
        mvfolder = os.path.join(oldFolder, 'original_data')
        file_name = os.path.basename(self.inputRaster.dataSource)
        common.makeFolder(mvfolder)
        shutil.move(self.inputRaster.dataSource, mvfolder + "/" + file_name) 
        self.inputRaster.dataSource = newDataSource
        fileIdDatabase[self.inputRaster.id] = self.inputRaster
        saveIdDatabase(fileIdDatabase)
        return folder
   
    def byLayer(self, bandIndexes, env):
        outputRasters = []
        for idx in bandIndexes:
            bandIdxList = 'rasterbands(' + str(idx) + ')'
            ilwisRasters = []
            for lyrIdx in self.lyrIdxs:
                layer = self.inputRaster.idx2layer(lyrIdx)
                if layer != None: 
                    datapath = os.path.join(self.dataSource, layer.dataSource)
                    rband = ilwis.RasterCoverage(datapath)
                    ev = ilwis.Envelope("(" + env + ")")
                    if not ev.equalsP(rband.envelope(), 0.001, 0.001, 0.001):
                        rc = ilwis.do("selection", rband, "envelope(" + env + ") with: " + bandIdxList)
                        ilwisRasters.append(rc)
                    else:
                        ilwisRasters.append(rband) 

            extra = self.constructExtraParams(self.inputRaster, self.temporalExtent, idx)
            outputRasters.extend(self.setOutput(ilwisRasters, extra)) 

        return outputRasters                   

    def byBand(self, bandIndexes, env):
        
        outputRasters = []        
        for idx in bandIndexes:
            ilwisRasters = []
           ## bandIdxList = 'rasterbands(' + str(0) + ')'
            datapath = os.path.join(self.dataSource, self.inputRaster.bands[idx]['source'])                            
            rband = ilwis.RasterCoverage(datapath)
            ev = ilwis.Envelope("(" + env + ")")
            if not ev.equalsP(rband.envelope(), 0.001, 0.001, 0.001):
                rc = ilwis.do("selection", rband, "envelope(" + env + ")" )
                ilwisRasters.append(rc)
            else:
                ilwisRasters.append(rband) 
            extra = self.constructExtraParams(self.inputRaster, self.temporalExtent, idx)
            outputRasters.extend(self.setOutput(ilwisRasters, extra))    

        return outputRasters                  
        
    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            
            indexes = str(self.bandIdxs).lstrip('[').rstrip(']')
            indexes = [int(ele) for ele in indexes.split(',')]
            ext = self.inputRaster.spatialExtent
            env = str(ext[0]) + " " + str(ext[2]) + "," + str(ext[1]) + " " +str(ext[3])

            if self.inputRaster.grouping == 'layer':
                outputRasters = self.byLayer(indexes, env)
            if self.inputRaster.grouping == 'band':                
                outputRasters = self.byBand(indexes, env)

            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
           
def registerOperation():
     return LoadCollectionOperation()