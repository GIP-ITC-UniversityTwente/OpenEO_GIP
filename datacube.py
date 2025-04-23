import os
import json
from eoreader.reader import Reader
from eoreader.bands import *
from datetime import datetime, date
from dateutil import parser
import ilwis
from constants.constants import *
import common
import copy
import pathlib
import logging



# gets a value from a dict and if no value is present it returns a default value
def getValue(key, extraMetaData, defValue):
        if extraMetaData == None:
            return defValue

        if key in extraMetaData:
            return extraMetaData[key]
        return defValue

# gets a value from a dict and if no value is present it raised an exception
def getMandatoryValue(key, extraMetaData):
        if extraMetaData == None:
            raise Exception("missing mandatory key in metadata :" + key)

        if key in extraMetaData:
            return extraMetaData[key]
        raise Exception("missing mandatory key in metadata :" + key)   

def isPrimitive(obj):
    return not hasattr(obj, '__dict__')

"""
def createNewRaster(job_id, rasters):
    stackIndexes = []
    for index in range(0, len(rasters)):
        stackIndexes.append(index)

    dataDefRaster = rasters[0].datadef()

    for index in range(0, len(rasters)):
        dfNum = rasters[index].datadef()
        dataDefRaster = ilwis.DataDefinition.merge(dfNum, dataDefRaster)

    grf = ilwis.GeoReference(rasters[0].coordinateSystem(), rasters[0].envelope() , rasters[0].size())
    rc = ilwis.RasterCoverage()
    rc.setGeoReference(grf) 
    dom = ilwis.NumericDomain("code=integer")
    rc.setStackDefinition(dom, stackIndexes)
    rc.setDataDef(dataDefRaster)

    common.registerIlwisIds(job_id, [grf, rc])

    for index in range(0, len(rasters)):
        rc.setBandDefinition(index, rasters[index].datadef())

    return rc 
"""

# class that collects all metadata and binary data of a dataset. A RasterData is multidimensional,
# though limited to 4 dimensions atm. I can use .metadat files, primarr satlite data and 
# Ilwis.RasterCoverage objects to creates its metadata/data structure. Apart from adminstrative members
# The RasterData has a dimensional structure ( a simple list of tags), a dict containing the metadata for
# each dimensional level (e.g spectral bands, how mnay, type etc..) and where in the structure the actual 
# implementation can be found. De location is described by an index which has the form '3:1:2' or somethign similar.
# meaning that the implementaion is for element 3 of the outer dimension, element 1 for the inner and element 2 for the inner most
# in practice this index has often only one or two levels. Meaning (often) which spectral band and which temporal layer.
# note that the XY dimenions is present but not actually used as it is assumed to be always there
# the content of the 'extra' parameter may shape how the dimensional structure is actually build.
class DataCube(dict):
    def load( self, layerDataLink, method = 'eoreader', extra = None ) : 
        if method == 'eoreader':
            namepath = os.path.splitext(layerDataLink)[0]
            head, tail = os.path.split(namepath)
            mttime = os.path.getmtime(layerDataLink)
            self['lastmodified'] = datetime.fromtimestamp(mttime)
            prod = Reader().open(layerDataLink) 
            self['id'] = tail
            self['title'] = prod.stac.title  
            self['dataSource'] = layerDataLink
            self['dataFolder'] = head
            self['type'] = 'file'
            self.fromEOReader(prod)
        if method == 'ilwisraster':
            self.fromIlwisRaster(layerDataLink, extra)
        if method == 'metadata':
            metafile = open(layerDataLink)
            metadata = json.load(metafile)
            mttime = os.path.getmtime(layerDataLink)
            self['lastmodified'] = datetime.fromtimestamp(mttime)
            namepath = os.path.splitext(layerDataLink)[0]
            head, tail = os.path.split(namepath)
            self['dataSource'] = layerDataLink
            self.fromMetadata(metadata)
            self['dataFolder'] = metadata['dataFolder']          

        if self['type'] == 'file' and extra != None:
            if self['id'] in extra:
                self['description'] =  getMandatoryValue('description', extra[self['id']]), 
                self['license'] = getMandatoryValue('license', extra[self['id']]),                   
                self['keywords'] = getValue('keywords', extra[self['id']], [])
                self['providers'] = getValue('providers', extra[self['id']], 'unknown'),
                self['links'] = getMandatoryValue('links', extra[self['id']])

    def fromEOReader(self, prod):
        time = [str(prod.stac.datetime), str(prod.stac.datetime)]
        self[TEMPORALEXTENT] = time
        self['proj'] = prod.stac.proj.epsg
        self['spatialExtent'] = prod.stac.proj.bbox
        self['summaries']= {}
        self.setSummariesValue('constellation', prod.stac)
        self.setSummariesValue('instrument', prod)
        self['eo:cloud_cover'] = prod.get_cloud_cover()
        self['nodata'] = prod.nodata  
        self[DIMENSIONSLABEL] = {}
     
        bandIndex = 0
        for eoband in prod.bands.items():
            band = RasterBand()
            band.fromEoReader(eoband, bandIndex)
            if eoband[1] != None:
                self.setItem(DIMSPECTRALBANDS, band)
            bandIndex = bandIndex+1                
        self[DIMENSIONSLABEL]['boundingbox'] = prod.stac.bbox    
        layer = RasterLayer()
        # layer 0 is a special layer as its the extent all the layers
        layer[TEMPORALEXTENT] = self[TEMPORALEXTENT]
        layer[DATASOURCE] = 'all'
        layer[LAYERINDEX] = 0
        layer['label'] = str(self[TEMPORALEXTENT][1:-1]).replace("'", "")
        self['eo:cloud_cover'] = 0
        self.setItem(DIMTEMPORALLAYER, layer)
        layer = RasterLayer()     
        layer[TEMPORALEXTENT] = self[TEMPORALEXTENT]
        layer[DATASOURCE] = self['dataSource']
        layer[LAYERINDEX] = 1
        layer['label'] = str(self[TEMPORALEXTENT][1:-1]).replace("'", "")
        layer['eo:cloud_cover'] = prod.get_cloud_cover()  
        self.setItem(DIMTEMPORALLAYER, layer)
        self['implementation'] = [DIMSPECTRALBANDS, DIMTEMPORALLAYER]

        if not DIMXRASTER in self:
            ext = self['spatialExtent']
            self.setItem(DIMXRASTER, {'extent' : [ float(ext[0]), float(ext[2])], 'unit' : 'meter', 'reference_system' : self['proj']} )
            self.setItem(DIMYRASTER, {'extent' : [ float(ext[1]), float(ext[3])], 'unit' : 'meter', 'reference_system' : self['proj']} )            

    def fromMetadata(self, metadata):
        self['type'] = 'metadata' 
        self['id'] = metadata["id"]
        self['title'] = metadata["title"] 
        self['description'] = getMandatoryValue("description", metadata) 
        self['license'] = getMandatoryValue("license", metadata)                   
        self['keywords'] = getValue('keywords', metadata, [])
        self['providers'] = getValue('providers', metadata, 'unknown')
        self['links'] = getMandatoryValue("links", metadata) 
        ext = getMandatoryValue("dimensions", metadata)
        self['boundingbox'] = getMandatoryValue("boundingbox", ext)
        self['proj'] = getValue('proj' , metadata, '0')
        if self['proj'] == '0':
           self['proj'] = str(getValue('proj:epsg' , metadata, '0'))
        self['nodata'] = getValue('nodata' , metadata, -9999)
        self[DIMENSIONSLABEL] = {}
        bands = getMandatoryValue(DIMSPECTRALBANDS, ext)
        for idx in range(len(bands)):
            band = RasterBand()
            band['name'] = bands[idx]['name']
            band['commonbandname']= bands[idx]['commonbandname']
            band['details'] = bands[idx]['details']
            band[BANDINDEX] = idx
            band['type'] = bands[idx]['type']
            band[ RASTERDATA ] = ilwis.RasterCoverage()
            band['label'] = band['name']
            if DATASOURCE in bands[idx]: # source can be in layer oriented organization
                band[DATASOURCE] = bands[idx][DATASOURCE]
            self.setItem(DIMSPECTRALBANDS, band)

        if 'summaries' in metadata:
            self['summaries'] = metadata['summaries']         
        temporal = getMandatoryValue(DIMTEMPORALLAYER, ext)
        if len(temporal) == 0:
            raise Exception("missing mandatory temporal extent value") 
     
        #layer = RasterLayer({DATASOURCE : 'all', TEMPORALEXTENT :  temporal[0], LAYERINDEX : 0, 'eo:cloud_cover' : 0, 'label' : 'all', 'reference_system' : 'Gregorian calendar / UTC', 'unit' : STATUSUNKNOWN})
        self[TEMPORALEXTENT] = temporal[0]['extent']
        #self.setItem(DIMTEMPORALLAYER, layer)     
        for index in range(0, len(temporal)):
            layer = RasterLayer() 
            layer.fromMetadataFile(temporal, index)
            layer['reference_system'] = 'Gregorian calendar / UTC'
            layer['unit'] = STATUSUNKNOWN
            self.setItem(DIMTEMPORALLAYER, layer)
   
        xext = ext['x'][0]['extent']
        yext = ext['y'][0]['extent']
        self['spatialExtent'] = [xext[0], xext[1], yext[0], yext[1]]
        self['eo:cloud_cover'] = DTUNKNOWN
        if self.bandBased():
            self['implementation'] = [DIMSPECTRALBANDS, DIMTEMPORALLAYER]
        else:
            self['implementation'] = [DIMTEMPORALLAYER, DIMSPECTRALBANDS]            
        self[DIMENSIONSLABEL]['boundingbox'] = metadata[DIMENSIONSLABEL]['boundingbox']
        if not DIMXRASTER in self:
            self.setItem(DIMXRASTER, {'extent' : [ float(xext[0]), float(xext[1])], 'unit' : 'meter', 'reference_system' : self['proj']} )
            self.setItem(DIMYRASTER, {'extent' : [ float(yext[0]), float(yext[1])], 'unit' : 'meter', 'reference_system' : self['proj']} )        
      

    def fromIlwisRaster(self, ilwisRaster, extraParams):
        rasterList = []
        if isinstance(ilwisRaster, list):
            rasterList = ilwisRaster
            ilwisRaster = rasterList[0]
        else:
            rasterList = [ilwisRaster]            
            
        self.lastmodified = datetime.now()
        self['id'] = str(ilwisRaster.ilwisID())
        n = ilwisRaster.name()
        parts = n.split('.')
        self['title'] = parts[0]
        if not extraParams == None and 'basename' in extraParams:
            self['title'] = extraParams['basename'] + '_' + str(self['id'])
        self['description'] = "internally generated"
        self['license'] = "none"            
        self[ 'keywords'] = "raster"
        self['providers'] = "internal"
        self['type'] = 'data'
        self['links'] = ''
        ext = str(ilwisRaster.envelope())
        csyLL = ilwis.CoordinateSystem("code=epsg:4326")
        env = csyLL.convertEnvelope(ilwisRaster.coordinateSystem(), ilwisRaster.envelope())
        self['boundingbox'] = str(env)
        if not extraParams == None and 'epsg' in extraParams:
            prj = extraParams['epsg']
        else:
            prj = ilwisRaster.coordinateSystem().toEpsg();            
        self['proj'] = prj
        if prj == '?':
            self['proj'] = ilwisRaster.coordinateSystem().toProj4();
        self[TEMPORALEXTENT] = getValue(TEMPORALEXTENT, extraParams, [str(date.today()),str(date.today())])
        extparts = ext.split()
        x1 = float(extparts[0])
        x2 = float(extparts[2])
        y1 = float(extparts[1])
        y2 = float(extparts[3])
        self['spatialExtent'] = [min(x1,x2),min(y1,y2) ,max(x1,x2) ,max(y1,y2) ]
        url = ilwisRaster.url()
        path = url.split('//')
        head = os.path.dirname(path[1])
        self['dataSource'] = path[1]
        self['dataFolder'] = head
        self['nodata'] = RUNDEFFL
        self[DIMENSIONSLABEL] = {}
        count = 0
        self['implementation'] = [DIMSPECTRALBANDS, DIMTEMPORALLAYER]

        if extraParams != None and DIMSPECTRALBANDS in extraParams:
            for b in extraParams[DIMSPECTRALBANDS]:
                band = RasterBand()
                band['name'] = b['name']
                if 'commonbandname' in b:
                    band['commonbandname'] = b['commonbandname']
                else:
                    band['commonbandname'] = b['name']
                if 'details' in b:
                    band['details'] = b['details']
                else:
                    band['details'] = {}
                if BANDINDEX in  b:
                    band[BANDINDEX] = b[BANDINDEX]
                else:
                    band['commonbandname'] = count                
                if 'type' in b:
                    band['type'] = b['type']
                else:
                    band['type'] =  'float'
                band[RASTERDATA] = rasterList[count]
                band['label'] = band['name']
                self.setItem(DIMSPECTRALBANDS, band)

                count = count + 1 
        else:
            ilwRaster = rasterList[0]
            z = ilwRaster.size().zsize
            for b in range(0,z):
                band = RasterBand()
                band['commonbandname']  = band['name'] = 'band' + str(b)
                band['type'] = 'float'            
                band['label'] = band['name']
                band['details'] = {'gsd' : rasterList[0].geoReference().pixelSize()}
                band[BANDINDEX] = b
                name = rasterList[0].name()
                band[DATASOURCE] = name
                self.setItem(DIMSPECTRALBANDS, band)
     
        if extraParams != None and TEMPORALEXTENT in extraParams:
            self.layerIndex = 0
            if 'textsublayers' in extraParams:
                textsublayers = extraParams['textsublayers']
                layer = RasterLayer({DATASOURCE : 'all', TEMPORALEXTENT :  self[TEMPORALEXTENT], LAYERINDEX : 0, 'eo:cloud_cover' : 0, 'label' : str(self[TEMPORALEXTENT])[1:-1].replace("'", ""), 'reference_system' : 'Gregorian calendar / UTC', 'unit' : STATUSUNKNOWN})
                self.setItem(DIMTEMPORALLAYER, layer)
          
                for index in range(0, len(textsublayers)):
                    layer = RasterLayer({DATASOURCE : '', TEMPORALEXTENT : textsublayers[index], LAYERINDEX : index + 1, 'eo:cloud_cover' : 0, 'label': str(textsublayers[index][1:-1]).replace("'", ""), 'reference_system' : 'Gregorian calendar / UTC', 'unit' : STATUSUNKNOWN})
                    self.setItem(DIMTEMPORALLAYER, layer)
        else:
            now = datetime.now()
            date_string = now.strftime("%Y-%m-%dT%H:%M:%S")
            time = [date_string, date_string]
            layer = RasterLayer({DATASOURCE : 'all', TEMPORALEXTENT :  self[TEMPORALEXTENT], LAYERINDEX : 0, 'eo:cloud_cover' : 0, 'label' : str(time)[1:-1].replace("'", ""), 'reference_system' : 'Gregorian calendar / UTC', 'unit' : STATUSUNKNOWN})                    
            self.setItem(DIMTEMPORALLAYER, layer)
            name = rasterList[0].name()
            layer = RasterLayer({DATASOURCE : '', TEMPORALEXTENT :  self[TEMPORALEXTENT], LAYERINDEX : 1, 'eo:cloud_cover' : 0, 'label' : str(time)[1:-1].replace("'", ""), 'reference_system' : 'Gregorian calendar / UTC', 'unit' : STATUSUNKNOWN})                    
            self.setItem(DIMTEMPORALLAYER, layer)
            parts = name.split('.')
            self['dataSource'] = os.path.join(head, parts[0] + '.metadata')
            self['dataFolder'] = os.path.join(head, parts[0])

        self[DIMENSIONSLABEL]['boundingbox'] = [float(i) for i in extparts]
        if not DIMXRASTER in self:
            self.setItem(DIMXRASTER, {'extent' : [ float(extparts[0]), float(extparts[2])], 'unit' : 'meter', 'reference_system' : self['proj']} )
            self.setItem(DIMYRASTER, {'extent' : [ float(extparts[1]), float(extparts[3])], 'unit' : 'meter', 'reference_system' : self['proj']} )

    def setItem(self, dimension, item):
        if not dimension in self[DIMENSIONSLABEL]:
            self[DIMENSIONSLABEL][dimension] = []

        self[DIMENSIONSLABEL][dimension].append(item)
    
    def adaptBoundingBox(self, dimension, item):
        if dimension == 'x':
            if item['extent'][0] <  self[DIMENSIONSLABEL]['boundingbox'][0]:
                self[DIMENSIONSLABEL]['boundingbox'][0] = item['extent'][0]
            if item['extent'][1] <  self[DIMENSIONSLABEL]['boundingbox'][2]:
                self[DIMENSIONSLABEL]['boundingbox'][2] = item['extent'][1]
        if dimension == 'y':
            if item['extent'][0] <  self[DIMENSIONSLABEL]['boundingbox'][1]:
                self[DIMENSIONSLABEL]['boundingbox'][1] = item['extent'][0]
            if item['extent'][1] <  self[DIMENSIONSLABEL]['boundingbox'][3]:
                self[DIMENSIONSLABEL]['boundingbox'][3] = item['extent'][1]                                             



    # extra metadata is metadata that is stored in a seperate file and contains metadata that
    # is presetn in the STAC specs but not can be directly found in the primary satelite data
    # e.g. license or owner ship. This method constructs this extrametadata (if present) and returns
    # it to the main loader and adds its metadata to the rest
    def loadExtraMetadata(self, datapath, name)  :
        headpath = os.path.split(datapath)[0]
        filename = os.path.split(datapath)[1]
        extraPath = os.path.join(headpath, name + '.extrametadata')
        extraMetadataAll = None
        extraMetadata = None
        if os.path.exists(extraPath):
            extraMd = open(extraPath)
            extraMetadataAll = json.load(extraMd)  
            if filename in extraMetadataAll:
                extraMetadata = extraMetadataAll[filename]
        return extraMetadata
    

    def setSummariesValue(self, key, source):
        if hasattr(source,key):
            p = getattr(source, key)
            if type(p) == str:
                self['summaries'][key] = p
            else:
                if hasattr(source, 'name') and hasattr(source, 'value'):
                    self['summaries'][key] = getattr(source,'value')
                else:
                    self['summaries'][key] = str(source)  

    def toLongDictDefinition(self):
        dictDef = self.toShortDictDefinition()
        dictDef['cube:dimensions'] = self.getJsonExtent()
        dictDef['summaries'] = {}
        if 'summaries' in self:
            dictDef['summaries'] = {"constellation" : self['summaries']["constellation"], "instrument" : self['summaries']['instrument']}
        dictDef['summaries']['eo:cloud_cover'] = [0,0]
        if 'eo:cloud_cover' in self:
            dictDef['summaries']['eo:cloud_cover'] = [0, self['eo:cloud_cover']]
        dictDef['eo:snow'] = [0,0]
        if 'eo:snow' in self:
            dictDef['summaries']['eo:snow'] = [0, self['snow']] 
        dictDef['summaries']['proj:epsg'] = { 'min' : '?', 'max' : '?'}             
        if self.projType() == 'epsg':           
            dictDef['summaries']['proj:epsg'] = { 'min' :self['proj'], 'max' : self['proj']} 

        gsds = set()
        bandlist = []
        bands = self.getBands()
        for b in bands:
            bdef = {"name": b['name']}
            bdef['commonbandname'] = b['commonbandname']
            for kvp in b['details'].items():
                bdef[kvp[0]] = kvp[1]
                if kvp[0] == 'gsd':
                    gsds.add(kvp[1])                        
            bandlist.append(bdef)
        dictDef['eo:bands'] = bandlist
        dictDef['eo:gsd'] = list(gsds)

        return dictDef

    def toShortDictDefinition(self):
        toplvl_dict = {}

        if 'id' in self:
            bbox = {}
            bbox['bbox'] = self['spatialExtent']
            lyr = self.idx2layer(0)
            if lyr == None:
                 time = self[TEMPORALEXTENT]
            else:                 
                time = lyr[TEMPORALEXTENT]
               
            interval = {}
            interval['interval'] = [time]
            ext = {'spatial' : bbox, 'temporal' : interval}        

            toplvl_dict = {'stac_version' : "1.2", 
                    'type' : 'Collection', 
                    'id' : self['id'], 
                    'title' : self['title'],
                    'description' : self['description'], 
                    'extent' : ext,
                    'license' : self['license'],                 
                    'keywords' : self['keywords'],
                    'providers' : self['providers'],
                    'links' : self[ 'links']
                    }
        return toplvl_dict
    
    def toMetadataFile(self, folder, name=''): 
        nm = self['id'] if name=='' else name 
        filename = os.path.join(folder, nm)
        meta = copy.deepcopy(self)
        

        with open(filename, "w") as write_file:
            s = json.dumps(meta, indent=4, default=str) 
            write_file.write(s)

        return filename             

    # translates the spatial extent to a json format. Used to translate a rasterdata instance to a dict
    def getJsonExtent(self):
        bbox = self['spatialExtent']
        epsg = self['proj']
        lyr = self.idx2layer(0)
        if lyr == None:
            time = self[TEMPORALEXTENT]
        else:                 
            time = lyr[TEMPORALEXTENT]        
        bnds = self.getBands()
        eobandlist = []
        for bnd in bnds:
            eobandlist.append(bnd['name'])

        x =   { 'type' : 'spatial', 'axis' : 'x', 'extent' : [bbox[0], bbox[2]] , 'reference_system' : epsg}
        y =   { 'type' : 'spatial', 'axis' : 'x', 'extent' : [bbox[1], bbox[3]], 'reference_system' : epsg}
        t =   { 'type' : 'temporal', 'extent' : time}

        d =  { 'x' : x, 'y' : y, 't' : t}

        if len(eobandlist) > 0:
            d['bands'] = { 'type' : 'bands', 'values' : eobandlist}

        return d            

    # translates the spatial extent to a json format. Used to translate a rasterdata instance to a dict
    def getExtentEOReader(self, prod):
       proj = prod.stac.proj
       bbox = proj.bbox
       epsg = proj.epsg
       time = [str(prod.stac.datetime)]
       bands = prod.bands
       x =   { 'type' : 'spatial', 'axis' : 'x', 'extent' : [bbox[0], bbox[2]] , 'reference_system' : epsg}
       y =   { 'type' : 'spatial', 'axis' : 'x', 'extent' : [bbox[1], bbox[3]], 'reference_system' : epsg}
       t =   { 'type' : 'temporal', 'extent' : time}

       bandlist = []
       for band in bands.items():
            b = band[1]
            if ( b != None):
                bandlist.append(b.name)

       return { 'x' : x, 'y' : y, 't' : t, 'bands' : { 'type' : 'bands', 'values' : bandlist}}
    
    #translate a list of bandnames to its corresponding indexes
    def getBandIndexes(self, requestedBands):
        idxs = []
        if len(requestedBands) == 0: # all bands
            for b in self.getBands():
                idxs.append(b[BANDINDEX])
        else:
            bands = self.getBands()
            for reqBandName in requestedBands:
                idx = 0 
                for b in bands:
                    if b['name'] == reqBandName or b['commonbandname'] == reqBandName:
                        if BANDINDEX in b:
                            idxs.append(b[BANDINDEX])
                            break
                        else:                    
                            idxs.append(idx)
                    idx = idx + 1
        return idxs           

    #translates a list of temporal extents to its corresponding layer indexes
    #note that the first layer is a general layer, not a real one. It describes the metadata of all the layers

    def getLayerIndexes(self, temporalExtent):
            idxs = []
            layers = self.getLayers()
            if temporalExtent == None:
                for layer in layers:
                    idxs.append(layer[LAYERINDEX])

            else:
                first = parser.parse(temporalExtent[0])
                last = parser.parse(temporalExtent[1])
                for layer in layers:
                    layerTempFirst = parser.parse(layer[TEMPORALEXTENT][0])
                    layerTempLast = parser.parse(layer[TEMPORALEXTENT][1])
                    if layerTempFirst >=  first and layerTempLast <= last:
                        idxs.append(layer[LAYERINDEX])

            return idxs
    # gets all the layers temporal extents of a rasterdata instance except for the first. The
    # first has a special meaning as its the temporal extent of all the layers
    def getLayersTempExtent(self):
        result = []
        if DIMTEMPORALLAYER in self[DIMENSIONSLABEL]:
            result = []
            first = True
            layers = self.getLayers()
            for layer in layers:
                if not first:
                    result.append(layer[TEMPORALEXTENT])
                first = False                
        return result

    def getLayers(self):
        result = []
        if DIMTEMPORALLAYER in self[DIMENSIONSLABEL]:
            result = []
            first = True
            layers = self[DIMENSIONSLABEL][DIMTEMPORALLAYER]
            for layer in layers:
                if not first:
                    result.append(layer)
                first = False                
        return result
    
    def getBands(self):
        result = []
        if DIMSPECTRALBANDS in self[DIMENSIONSLABEL]:
            result = []
            bands = self[DIMENSIONSLABEL][DIMSPECTRALBANDS]
            for band in bands:
               result.append(band)
        return result

    # translates an index to an actual band instance
    def index2band(self, idx):
        if DIMSPECTRALBANDS in self[DIMENSIONSLABEL]:
            meta = self.getBands()
            for b in meta:
                if b[BANDINDEX] == int(idx):
                    return b
        return None                    

    #translates an index to an actual layer index
    def idx2layer(self, index):
        if DIMTEMPORALLAYER in self[DIMENSIONSLABEL]:
            items = self[DIMENSIONSLABEL][DIMTEMPORALLAYER]
            for layer in items:
                if layer[LAYERINDEX] == index:
                    return layer
        return None  
    
    def bandBased(self):
        #if there is more than two layers the implementation can be found in the layers metadata
        #note that the first layers is always the 'aggregate' layer(metadata). The second layer is a 'real'
        #layer with a 'source' attribute pointing (if its layer based, else its empty) to the file implementation
        # that may contain multuiple bands. If its band based the  band implementation data 
        # will contain one or more (temporal) layers
        return len(self[DIMENSIONSLABEL][DIMTEMPORALLAYER]) == 2
    
    def hasData(self, d = None):
         if d == None:
            implDim = next(iter(self['implementation']))
            d = self.firstImplDimItem(implDim)
         if RASTERDATA in d:
            if isinstance(d[RASTERDATA], ilwis.RasterCoverage):
                b = bool(d[RASTERDATA])
                return b
         return False

    def firstImplDimItem(self, implDim):
        if len(self[DIMENSIONSLABEL][implDim]) > 0:
            if implDim == DIMSPECTRALBANDS:
                d = self[DIMENSIONSLABEL][implDim][0]
            else: #temporal layers; element 0 is aggregate
                d = self[DIMENSIONSLABEL][implDim][1]
            return d                
        return None

    #gets a raster implementation for this RasterData objects. Note that if it contain multiple
    #implementation ( meaning multiple bands) you must include a band name. If its just a layer based
    # implementation there will only be one impl ( ilwisrasters are 3D)
    def getRaster(self, implId=None, byIndex=False):
        implDim = next(iter(self['implementation']))
        if implId == None: # if the name is empty it is assumed that first raster implementation
                           # ( and often the only) is implied
           if len(self[DIMENSIONSLABEL][implDim]) > 0:
                d = self.firstImplDimItem(implDim)
                # might be that the rasterdata is not yet loaded
                if not self.hasData(d):
                    return self.loadRaster(d)
                return d[RASTERDATA]
           return None
        for item in self[DIMENSIONSLABEL][implDim]:
            if byIndex:
                if item[INDEXID] == implId:
                    if not self.hasData(item):
                        self.loadRaster(item)
                    return item['data']
            else:                
                if item[NAMEID] == implId:
                    if not self.hasData(item):
                        self.loadRaster(item)
                    return item['data']
        
        return None

    def projType(self): 
        if 'proj' in self and self['proj'] != None:
            if str(self['proj']).find('+proj') != -1:
                return 'proj4'
            if self['proj'].isnumeric():
                return 'epsg'
        return '?'
    
    def loadRaster(self, d):
        pdir = os.path.dirname(self['dataSource'])
        spath = os.path.join(pdir, self['dataFolder'], d[DATASOURCE] ) 
        ilwRaster = ilwis.RasterCoverage(spath)
        d[RASTERDATA] = ilwRaster
        return ilwRaster
    
    def getRasters(self):
        data = []
        implDim = next(iter(self['implementation']))  
        for item in self[DIMENSIONSLABEL][implDim]:
            if RASTERDATA in item and (not isinstance(item[RASTERDATA], str)):
                data.append(item[RASTERDATA])  
            else:
                ilwRaster = self.loadRaster(item)
                if ilwRaster:
                    data.append(ilwRaster)
        return data

    def matchProjection(self, otherRaster):
        if 'proj' in otherRaster and 'proj' in self:
            return self['proj'] == otherRaster['proj']
        return False               

    def getImplementationDimension(self):
        return next(iter(self['implementation']))

    def getDimension(self, level=0) :
        if level == 0:
            return next(iter(self['dimensions']))
        i = 0
        for item in self['dimensions']:
            if i == level:
                return item
            i = i+1

        return None            

    def setLabels(self, dimension, labels):
        if dimension in self[DIMENSIONSLABEL]:
            startIndex = 0 if dimension != DIMTEMPORALLAYER else 1
            for index in len(startIndex, self[DIMENSIONSLABEL][dimension]):
                self[DIMENSIONSLABEL][dimension][index]['label'] = labels[index]
            
    def getLabels(self, dimension):
            result = []
            if dimension in self[DIMENSIONSLABEL]:
                startIndex = 0 if dimension != DIMTEMPORALLAYER else 1
                for index in len(startIndex, self[DIMENSIONSLABEL][dimension]):
                    if 'label' in self[DIMENSIONSLABEL][dimension][index]:
                        result.append(self[DIMENSIONSLABEL][dimension][index]['label']) 
            return result                          

    def createRasterDatafromBand(self, bands):
        extra = { TEMPORALEXTENT : self[TEMPORALEXTENT], 'bands' : bands, 'epsg' : self['proj']}
        extra['textsublayers'] = self.getLayersTempExtent()
        extra['rasterkeys'] = []
        rasters = []
        for band in bands: 
            ilwRaster = band['data']
            rasters.append(ilwRaster)
     
        extra['basename'] = self['title'] + '_' + str(len(bands))
        rasterData = DataCube()
        rasterData.load(rasters, 'ilwisraster', extra )

        return rasterData
    
    def sourceIsMetadata(self):
        if 'dataSource' in self:
            ext = pathlib.Path(self['dataSource']).suffix
            return ext == '.metadata'
        return False  

    def dataFolder(self):
        if 'dataFolder' in self:
            fpath = self['dataFolder']
            if ( fpath.find('/')) == 0:
                return fpath
            if 'dataSource' in self:
                fds = self['dataSource']
                folder = os.path.dirname(fds)
                return folder 
        return ''   
    
# stores the metadata for a seperate band. Note that the details section can contain
# sensor specific information about this band    
class RasterBand(dict):
    CENTER_WAVELENGTH = 'center_wavelength'
    def toDict(self):
        d = {}
        d['type'] = self['type']
        d['name'] = self['name']
        d['commonbandname'] = self['commonbandname']
        d['details'] = self['details']
        d[BANDINDEX] = self[BANDINDEX]
        d[DATASOURCE] = self[DATASOURCE]
        return d


    def fromEoReader(self, band, index):
        defnames = ['name', 'common_name', 'description', RasterBand.CENTER_WAVELENGTH, 'full_width_half_max', 'solar_illumination','gsd']
        b = band[1]
        if ( b != None):
            details = {}
            name = ''
            for key,value in b.__dict__.items():
                if key == 'name':
                    name = value
                else:
                    if value != None and isPrimitive(value):
                        if key in defnames:
                            details[key] = value
            if name != '':                            
                self['name'] = name
            b = band[0] 
            self['commonbandname'] = b.value
            self['details'] = details
            self[BANDINDEX] = index
            self['type'] = 'float'
            self['label'] = self['commonbandname']
            # we don't load data at this level; might be not needed at all 

    def getDetail(self, property):
        if 'details' in self:
            if property in self['details']:
                return self['details'][property]
        return None  



  
   
class RasterLayer(dict):
    def fromMetadataFile(self, metadata, index):
        self[DATASOURCE] = metadata[index][DATASOURCE]
        self[TEMPORALEXTENT] = metadata[index][TEMPORALEXTENT]
        if 'eo:cloud_cover' in metadata[index]:
            self['eo:cloud_cover'] = metadata[index]['eo:cloud_cover']
        else:
            self['eo:cloud_cover'] = 0            
        self[LAYERINDEX] = index
        self['label'] = str(self[TEMPORALEXTENT][1:-1]).replace("'", "")

    def fromMetadata(self, temporalMetadata, idx ):
        self.temporalExtent = temporalMetadata['extent']
        self.dataSource = temporalMetadata[DATASOURCE]
        self.index = idx

def matchesTemporalExtent(existingLayers: list[RasterLayer], tobeCheckedLayers : list[RasterLayer]):
    if len(existingLayers) != len(tobeCheckedLayers):
        return False
    for lyrKey in existingLayers:
            if not lyrKey in tobeCheckedLayers:
                return False
            d00 = existingLayers[lyrKey][TEMPORALEXTENT][0]
            d01 = existingLayers[lyrKey][TEMPORALEXTENT][1]
            d10 = tobeCheckedLayers[lyrKey][TEMPORALEXTENT][0]
            d11 = tobeCheckedLayers[lyrKey][TEMPORALEXTENT][1]
            if d00 != d10:
                return False
            if d01 != d11:
                return False
    return True  

def matchBands(existingBands : list, toBeCheckedBands : list):
    if len(existingBands) != len(toBeCheckedBands):
        return False
    for bnd in existingBands:
        b1 = existingBands[bnd]
        b2 = toBeCheckedBands[bnd]
        if b1['name'] != b2['name'] or b1['type'] != b2['type']:
            return False

    return True


    
