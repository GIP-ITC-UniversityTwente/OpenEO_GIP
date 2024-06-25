import os
import json
from eoreader.reader import Reader
from eoreader.bands import *
from datetime import datetime, date
from dateutil import parser
import ilwis
from constants.constants import *
import re

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


def createNewRaster(rasters):
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

    for index in range(0, len(rasters)):
        rc.setBandDefinition(index, rasters[index].datadef())

    return rc 
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
class RasterData(dict):
    def load( self, layerDataLink, method = 'eoreader', extra = None ) : 
        self[STRUCTUREDEFDIM] = []
        self[METADATDEFDIM] = {}
        self[DATAIMPLEMENTATION] = {}     
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
            dataDir = os.path.join(head, metadata["data_folder"])  
            self['dataSource'] = layerDataLink
            self['dataFolder'] = dataDir
            self.fromMetadata(metadata)

        if self['type'] == 'file' and extra != None:
            if self['id'] in extra:
                self['description'] =  getMandatoryValue('description', extra[self['id']]), 
                self['license'] = getMandatoryValue('license', extra[self['id']]),                   
                self['keywords'] = getValue('keywords', extra[self['id']], [])
                self['providers'] = getValue('providers', extra[self['id']], 'unknown'),
                self['links'] = getMandatoryValue('links', extra[self['id']])

    def fromEOReader(self, prod):
        self['boundingbox'] = prod.stac.bbox
        time = [str(prod.stac.datetime), str(prod.stac.datetime)]
        self['temporalExtent'] = time
        self['proj:epsg'] = prod.stac.proj.epsg
        self['spatialExtent'] = prod.stac.proj.bbox
        self['summaries']= {}
        self.setSummariesValue('constellation', prod.stac)
        self.setSummariesValue('instrument', prod)
        self['eo:cloud_cover'] = prod.get_cloud_cover()  
        
        self[STRUCTUREDEFDIM] = [DIMSPECTRALBANDS]
        bands = {}
        labels = []       
        bandIndex = 0
        for eoband in prod.bands.items():
            band = RasterBand()
            band.fromEoReader(eoband, bandIndex)
            if eoband[1] != None:
                bands[band['name']] = band
                bandIndex = bandIndex + 1
                labels.append(band['name']) 
            
        self[METADATDEFDIM][DIMSPECTRALBANDS] = {'items' : bands, 'labels' :labels, 'RefSystem' : '', 'unit' : ''}  
        lyrs = {} 
        labels = [] 
        layer = RasterLayer()
        # layer 0 is a special layer as its the extent all the layers
        layer['temporalExtent'] = self['temporalExtent']
        layer['dataSource'] = 'all'
        layer['layerIndex'] = 0
        self['eo:cloud_cover'] = 0
        lyrs['all'] = layer  
        layer = RasterLayer()     
        layer['temporalExtent'] = self['temporalExtent']
        layer['dataSource'] = self['dataSource']
        layer['layerIndex'] = 1
        layer['eo:cloud_cover'] = prod.get_cloud_cover()  
        lyrs[str(layer['temporalExtent'])] = layer
        self['layers'] = lyrs

        self[STRUCTUREDEFDIM].append(DIMXYRASTER)
        self[METADATDEFDIM][DIMXYRASTER] = {'unit' : 'meter', 'RefSystem' : self['proj:epsg']}
        self[DATAIMPLEMENTATION] = {}        

    def fromMetadata(self, metadata):
        self['type'] = 'metadata' 
        self['id'] = metadata["title"]
        self['title'] = metadata["title"] 
        self['description'] = getMandatoryValue("description", metadata) 
        self['license'] = getMandatoryValue("license", metadata)                   
        self['keywords'] = getValue('keywords', metadata, [])
        self['providers'] = getValue('providers', metadata, 'unknown')
        self['links'] = getMandatoryValue("links", metadata) 
        ext = getMandatoryValue("dimensions", metadata)
        self['boundingbox'] = getMandatoryValue("bounding_box", ext)
        self['proj:epsg'] = getValue('epsg' , metadata['projection'], '0')

        bands = getMandatoryValue("bands", ext)
        self[STRUCTUREDEFDIM] = [DIMSPECTRALBANDS]
        labels = []
        bdns = {}
        for idx in range(len(bands)):
            band = RasterBand()
            band['name'] = bands[idx]['name']
            band['commonbandname']= bands[idx]['commonbandname']
            band['details'] = bands[idx]['details']
            band['bandIndex'] = idx
            band['type'] = bands[idx]['type']
            bdns[band['name']] = band
            labels.append(band['name'])
        self[METADATDEFDIM][DIMSPECTRALBANDS] = {'items' : bdns, 'labels' :labels, 'RefSystem' : '', 'unit' : ''}            
        if 'summaries' in metadata:
            self['summaries'] = metadata['summaries']         
        temporal = getMandatoryValue("t", ext)
        if len(temporal) == 0:
            raise Exception("missing mandatory temporal extent value") 
        lyrs = {} 
        labels = []       
        for index in range(0, len(temporal)):
            layer = RasterLayer() 
            layer.fromMetadataFile(temporal, index)
            key = str(layer['temporalExtent'])
            if index == 0:
                key = 'all'
                self['temporalExtent'] = layer['temporalExtent']
            lyrs[key] = layer
            labels.append(layer['temporalExtent'])
        if len(lyrs) > 0:
            self[STRUCTUREDEFDIM].append(DIMTEMPORALLAYER)
            self[METADATDEFDIM][DIMTEMPORALLAYER] =  {'items' :lyrs, 'labels' : labels,'unit' : '' , 'RefSystem': 'Gregorian calendar / UTC'} 
        self['layers'] =lyrs 
        xext = ext['x']['extent']
        yext = ext['y']['extent']
        self['spatialExtent'] = [xext[0], xext[1], yext[0], yext[1]]
        self['eo:cloud_cover'] = DTUNKNOWN
        
        self[STRUCTUREDEFDIM].append(DIMXYRASTER)
        self[METADATDEFDIM][DIMXYRASTER] = {'unit' : 'meter', 'RefSystem' : self['proj:epsg']}
        self[DATAIMPLEMENTATION] = {}
      

    def fromIlwisRaster(self, ilwisRaster, extraParams):
        rasterList = []
        if isinstance(ilwisRaster, list):
            rasterList = ilwisRaster
            ilwisRaster = rasterList[0]
        else:
            rasterList = [ilwisRaster]            
            
        self.lastmodified = datetime.now()
        self['id'] = ilwisRaster.ilwisID()
        self['title'] = ilwisRaster.name()
        if 'basename' in extraParams:
            self['title'] = extraParams['basename'] + '_' + str(self['id'])
        self['description'] = "internally generated"
        self['license'] = "none"            
        self[ 'keywords'] = "raster"
        self['providers'] = "internal"
        self['type'] = 'data'
        self['links'] = ''
        ext = str(ilwisRaster.envelope())
        csyLL = ilwis.CoordinateSystem("epsg:4326")
        env = csyLL.convertEnvelope(ilwisRaster.coordinateSystem(), ilwisRaster.envelope())
        self['boundingbox'] = str(env)
        epsg = extraParams['epsg']
        self['proj:epsg'] = epsg
        self['temporalExtent'] = getValue('temporalExtent', extraParams, [str(date.today()),str(date.today())])
        parts = ext.split()
        self['spatialExtent'] = [float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])]
        url = ilwisRaster.url()
        path = url.split('//')
        head = os.path.dirname(path[1])
        self['dataSource'] = url
        self['dataFolder'] = head
        count = 0
        defineSTRUCTUREDEFDIM = STRUCTUREDEFDIM in extraParams and len(extraParams[STRUCTUREDEFDIM]) > 0
        if defineSTRUCTUREDEFDIM:
            self[STRUCTUREDEFDIM] = extraParams[STRUCTUREDEFDIM]
        else:            
            self[STRUCTUREDEFDIM] = [DIMSPECTRALBANDS]

        bnds ={}
        if DIMSPECTRALBANDS in self[STRUCTUREDEFDIM]:
            labels = []
            for b in extraParams['bands']:
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
                if 'bandIndex' in  b:
                    band['bandIndex'] = b['bandIndex']
                else:
                    band['commonbandname'] = count                
                if 'type' in b:
                    band['type'] = b['type']
                else:
                    band['type'] =  'float'
                bnds[band['name']] = band
                labels.append(band['name'])
                count = count + 1
            self[METADATDEFDIM][DIMSPECTRALBANDS] = {'items' : bnds, 'labels' :labels, 'RefSystem' : '', 'unit' : ''}
     
        if not defineSTRUCTUREDEFDIM and 'textsublayers' in extraParams:
            self[STRUCTUREDEFDIM].append(DIMTEMPORALLAYER)
        impLevel = self[STRUCTUREDEFDIM][-1]
        if DIMTEMPORALLAYER in self[STRUCTUREDEFDIM]:
            self.layerIndex = 0
            labels = [] 
            if 'textsublayers' in extraParams:
                textsublayers = extraParams['textsublayers']
                lyrs = {} 
                # layer 0 is a special layer as its the extent all the layers
                layer = RasterLayer() 
                layer['source'] = 'all'
                layer['temporalExtent'] = self['temporalExtent']
                layer['layerIndex'] = 0 
                lyrs['all'] = layer
          
                for index in range(0, len(textsublayers)):
                    layer = RasterLayer() 
                    layer['source'] = '' # calculated or derived product there is no source
                    layer['temporalExtent'] = textsublayers[index]
                    layer['layerIndex'] = index
                    layer['eo:cloud_cover'] = 0
                    labels.append(textsublayers[index])               
                    lyrs[str(layer['temporalExtent'])] = layer
                self['layers'] =lyrs  

            self[METADATDEFDIM][DIMTEMPORALLAYER] =  {'items' :lyrs, 'labels' : labels,'unit' : '' , 'RefSystem': 'Gregorian calendar / UTC'} 
        if not DIMXYRASTER in self[STRUCTUREDEFDIM]:

            self[STRUCTUREDEFDIM].append(DIMXYRASTER)
            self[METADATDEFDIM][DIMXYRASTER] = {'unit' : 'meter', 'RefSystem' : self['proj:epsg']}

        if not defineSTRUCTUREDEFDIM:
            self.makeKeyIndex(0, "", impLevel, rasterList)
        if 'rasterkeys' in extraParams:
            i = 0
            for item in list(extraParams['rasterkeys']):
                self[DATAIMPLEMENTATION][item] = rasterList[i]
                i = i + 1
     

    def makeKeyIndex(self, currentIndex, keys, implementationLevel, rasterList):
        dimname = self[STRUCTUREDEFDIM][currentIndex]
        if dimname != implementationLevel:
            dimmeta = self[METADATDEFDIM][dimname]['items']

            for idx in range(0, len(dimmeta)):
                cidx = idx
                newkeys = str(cidx) if keys == "" else keys + ":" + str(cidx)
                self.makeKeyIndex(currentIndex + 1,newkeys, implementationLevel, rasterList)
        else: 
            for i in range(0,len(rasterList)):
                key = str(i)               
                self[DATAIMPLEMENTATION][key] = rasterList[i]

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
        if 'summaries' in self:
            dictDef['summaries'] = {"constellation" : self['summaries']["constellation"], "instrument" : self['summaries']['instrument']}
        if 'clouds' in self:
            dictDef['eo:cloud_cover'] = [0, self['eo:cloud_cover']]
        if 'snow' in self:
            dictDef['eo:snow'] = [0, self['snow']]            
        dictDef['proj:epsg'] = { 'min' :self['proj:epsg'], 'max' : self['proj:epsg']} 

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
            bbox['bbox'] = self['boundingbox']
            lyr = self.idx2layer(0)
            if lyr == None:
                 time = self['temporalExtent']
            else:                 
                time = lyr['temporalExtent']
               
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
    
    def toMetadataFile(self, folder):  
        filename = os.path.join(folder, self.id + ".metadata")
        meta = self

        with open(filename, "w") as write_file:
            json.dump(meta, write_file, indent=4) 

        return filename             

    # translates the spatial extent to a json format. Used to translate a rasterdata instance to a dict
    def getJsonExtent(self):
        bbox = self['spatialExtent']
        epsg = self['proj:epsg']
        lyr = self.idx2layer(0)
        if lyr == None:
            time = self['temporalExtent']
        else:                 
            time = lyr['temporalExtent']        
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
                idxs.append(b['bandIndex'])
        else:
            bands = self.getBands()
            for reqBandName in requestedBands:
                idx = 0 
                for b in bands:
                    if b['name'] == reqBandName or b['commonbandname'] == reqBandName:
                        if 'bandIndex' in b:
                            idxs.append(b['bandIndex'])
                            break
                        else:                    
                            idxs.append(idx)
                    idx = idx + 1
        return idxs           

    #translates a list of temporal extents to its corresponding layer indexes

    def getLayerIndexes(self, temporalExtent):
            idxs = []
            layers = self.getLayers()
            if temporalExtent == None:
                for layer in layers:
                    idxs.append(layer['layerIndex'])

            else:
                first = parser.parse(temporalExtent[0])
                last = parser.parse(temporalExtent[1])
                for layer in layers:
                    layerTempFirst = parser.parse(layer['temporalExtent'][0])
                    layerTempLast = parser.parse(layer['temporalExtent'][1])
                    if layerTempFirst >=  first and layerTempLast <= last:
                        idxs.append(layer['layerIndex'])

            return idxs
    # gets all the layers temporal extents of a rasterdata instance except for the first. The
    # first has a special meaning as its the temporal extent of all the layers
    def getLayersTempExtent(self):
        result = []
        if DIMTEMPORALLAYER in self[STRUCTUREDEFDIM]:
            result = []
            first = True
            layers = self[METADATDEFDIM][DIMTEMPORALLAYER]['items']
            for layer in layers.values():
                if not first:
                    result.append(layer['temporalExtent'])
                first = False                
        return result
    
    def getLayers(self):
        result = []
        if DIMTEMPORALLAYER in self[STRUCTUREDEFDIM]:
            result = []
            first = True
            layers = self[METADATDEFDIM][DIMTEMPORALLAYER]['items']
            for layer in layers.values():
                if not first:
                    result.append(layer)
                first = False                
        return result
    
    def getBands(self):
        result = []
        if DIMSPECTRALBANDS in self[STRUCTUREDEFDIM]:
            result = []
            bands = self[METADATDEFDIM][DIMSPECTRALBANDS]['items']
            for band in bands.values():
               result.append(band)
        return result

    # translates an index to an actual band instance
    def index2band(self, idx):
        if DIMSPECTRALBANDS in self[STRUCTUREDEFDIM]:
            meta = self.getBands()
            for b in meta:
                if b['bandIndex'] == int(idx):
                    return b
        return None                    

    #translates an index to an actual layer index
    def idx2layer(self, index):
        if DIMTEMPORALLAYER in self[STRUCTUREDEFDIM]:
            items = self[METADATDEFDIM][DIMTEMPORALLAYER]['items']
            for layer in items.items():
                if layer[1]['layerIndex'] == index:
                    return layer[1]
        return None  
    
    #gets a raster implementation for this RasterData objects. Note that if it contain multiple
    #implementation ( meaning multiple bands) you must include a band name. If its just a layer based
    # implementation there will only be one impl ( ilwisrasters are 3D)
    def getRaster(self, implName=''):
        if implName == '': # if the name is empty it is assumed that first raster implementation
                           # ( and often the only) is implied
           if len(self[DATAIMPLEMENTATION]) > 0:
                implName = next(iter(self[DATAIMPLEMENTATION]))
        if implName in self[DATAIMPLEMENTATION]:
            return self[DATAIMPLEMENTATION][implName]
        
        return None
    def setLabels(self, dimension, labels):
        if dimension in self[STRUCTUREDEFDIM]:
            self[METADATDEFDIM][dimension]['labels'] = labels
            
    def bandName2RasterKey(self, bandName):
        key = next(iter(self[DATAIMPLEMENTATION])) # from the key we can learn at which level the implementation can be found
        parts = key.split(':')
        level = self[STRUCTUREDEFDIM][len(parts)-1]
        meta = self[METADATDEFDIM][level]['items']
        endIdx = -1
        for key in meta:
            if key == bandName:
                endIdx = meta[key]['bandIndex']
                break
        if endIdx != -1:
            for rasterKey in self[DATAIMPLEMENTATION]:
                parts = rasterKey.split(':')
                if parts[-1] == str(endIdx):
                    return rasterKey
        return None

    def createRasterDatafromBand(self, bands):
        extra = { 'temporalExtent' : self['temporalExtent'], 'bands' : bands, 'epsg' : self['proj:epsg']}
        extra[STRUCTUREDEFDIM] = self[STRUCTUREDEFDIM]
        extra['textsublayers'] = self.getLayersTempExtent()
        extra['rasterkeys'] = []
        rasters = []
        for band in bands: 
            ilwRasterKey = self.bandName2RasterKey(band['name'])
            ilwRaster : ilwis.RasterCoverage = self[DATAIMPLEMENTATION][ilwRasterKey]
            rasters.append(ilwRaster)

            # last index of the rasterkey must now become 0 as its the only element
            # bit more complicated as expected the often happening case with only one level
            # of indexes ('0', '1', '2', etc...) has no ':'
            parts = ilwRasterKey.split(':')
            newKey = ''
            for p in range(len(parts)-1):
                newKey = newKey + ':' + parts[p] if p == 0 else parts[0]
            newKey = str(len(extra['rasterkeys'])) if newKey == '' else newKey + ':' + '0'
            extra['rasterkeys'].append(newKey)
       
        extra['basename'] = self['title'] + '_' + str(len(bands))
        rasterData = RasterData()
        rasterData.load(rasters, 'ilwisraster', extra )

        return rasterData
    
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
        d['bandIndex'] = self['bandIndex']
        d['dataSource'] = self['dataSource']
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
            self['bandIndex'] = index
            self['type'] = 'float'

    def getDetail(self, property):
        if 'details' in self:
            if property in self['details']:
                return self['details'][property]
        return None            
   
class RasterLayer(dict):
    def fromMetadataFile(self, metadata, index):
        self['source'] = metadata[index]['source']
        self['temporalExtent'] = metadata[index]['extent']
        if 'eo:cloud_cover' in metadata[index]:
            self['eo:cloud_cover'] = metadata[index]['eo:cloud_cover']
        else:
            self['eo:cloud_cover'] = 0            
        self['layerIndex'] = index

    def fromMetadata(self, temporalMetadata, idx ):
        self.temporalExtent = temporalMetadata['extent']
        self.dataSource = temporalMetadata['source']
        self.index = idx

def matchesTemporalExtent(existingLayers: list[RasterLayer], tobeCheckedLayers : list[RasterLayer]):
    if len(existingLayers) != len(tobeCheckedLayers):
        return False
    for lyrKey in existingLayers:
            if not lyrKey in tobeCheckedLayers:
                return False
            d00 = existingLayers[lyrKey]['temporalExtent'][0]
            d01 = existingLayers[lyrKey]['temporalExtent'][1]
            d10 = tobeCheckedLayers[lyrKey]['temporalExtent'][0]
            d11 = tobeCheckedLayers[lyrKey]['temporalExtent'][1]
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
    