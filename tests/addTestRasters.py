import ilwis
import math
import numpy as np
import common
from rasterdata import RasterData, RasterBand
import os
from pathlib import Path
import constants.constants as cc
from datetime import datetime, timedelta
import logging


def createEmptySmallNumericRaster(alternate):
    if alternate == 3:
        grf = ilwis.GeoReference("epsg:4326", ilwis.Envelope("10 35 30 60") , ilwis.Size(15,12))  
    else:      
        grf = ilwis.GeoReference("epsg:4326", ilwis.Envelope("0 25 30 60") , ilwis.Size(15,12))
    dfNum = ilwis.DataDefinition(ilwis.NumericDomain("code=value"), ilwis.NumericRange(0.0, 1000000.0, 1.0))
    rc = ilwis.RasterCoverage()
    rc.setGeoReference(grf)
    rc.setDataDef(dfNum)

    return rc 
                    
def createSmallNumericRasterNLayers(dims, alternate=0, bndcount=1):
    rc = createEmptySmallNumericRaster(alternate)
    rc.setSize(ilwis.Size(15,12,dims))
    baseSize = 15 * 12

    data =np.empty(baseSize, dtype = np.int64)

    for count in range(0, dims):
        if ( alternate == 0):
            for i in range(len(data)):
                data[i] = (i + count*baseSize) * 10 + (bndcount-1)*10 
                if i % 4 == 0:
                    data[i] = -data[i]
        if ( alternate == 1):                
            for i in range(len(data)):
                data[i] = (i + 2*baseSize + 3 * + 2 * math.sin(math.radians(1 + i*count * 10))) * 10 + (bndcount-1)*10 

            data[5 * 6] = ilwis.Const.iUNDEF # pos 0,2 is undefined 

        if ( alternate == 2 or alternate == 3): # mask map
             for i in range(len(data)):
                if i % 7 == 0:
                    data[i] = 1 
                else:
                    data[i] = 0                        
        rc.array2raster(data, count)                 
             
                            
    return rc 

def convert_to_utm_date_only(starting_date_str, dims):
    starting_date = datetime.strptime(starting_date_str, "%Y-%m-%d")
    utm_dates = []
    for i in range(0,2 * dims):
        new_date = starting_date + timedelta(days=i)
        utm_dates.append(new_date.strftime("%Y-%m-%d"))
    
    return utm_dates

def setTestRaster(dims, bndcount = 1, version = 0):
    
    raster = RasterData()

    common_names = ['Red', 'Green', 'Blue', 'NDVI', 'NIR', 'SWIR', 'Shortwave infrared / Cirrus']
    bdns = []
    bandNames = []
    rcs = []
    for i in range(bndcount):
        band = RasterBand()
        band['name'] = 'TB0' + str(i+1)
        bandNames.append({'name' : band['name']})
        band['commonbandname']=  common_names[i]
        band['details'] = {'center_wavelength' : 0.4 + 2 * i / 10}
        band[cc.BANDINDEX] = i
        band['type'] = 'float'
        band['label'] = band['name']
        bdns.append(band)
        rc = createSmallNumericRasterNLayers(dims, version, i)
        rcs.append(rc)

    url = rc.url()
    url.replace('////', '///') # if at all
    path = url.split('//')
    folder = os.path.dirname("/"+ path[1])
    common.logMessage(logging.INFO, 'new working folder:' + folder + '>>' + str(url)) 
    path = Path(folder).as_uri()
    ilwis.setWorkingCatalog(path)  

    utmdates = convert_to_utm_date_only('2020-11-01', dims)

    extra = {'epsg' : 4326, cc.TEMPORALEXTENT : ['2020-11-01', utmdates[-1]], 'bands' :bdns}
    if dims > 1:
        text = []
        for d in range(0, len(utmdates), 2):    
            d1 = utmdates[d]
            d2 = utmdates[d+1]
            text.append([d1, d2])
        extra['textsublayers'] = text 
    raster.load(rcs, 'ilwisraster', extra)        
    #raster[cc.METADATDEFDIM][cc.DIMSPECTRALBANDS] = bdns

 

    return raster

def setTestRasters(dims):
    if common.testRaster_openeo1 != None:
        return common.testRaster_openeo1
    
    raster1 = setTestRaster(dims, 1)
    raster1['id'] = raster1['name'] = cc.TESTFILENAME1    

    raster2 = setTestRaster(dims, 4)
    raster2['id'] = raster2['name'] = cc.TESTFILENAME2 

    raster3 = setTestRaster(dims, 4, 2) # mask map
    raster3['id'] = raster3['name'] = cc.TESTFILENAME_MASKMAP 

    raster4 = setTestRaster(dims, 4, 3) # mask map
    raster4['id'] = raster4['name'] = cc.TESTFILENAME_MASKMAP_SHIFTED

    raster5 = setTestRaster(1, 4) #no layer map
    raster5['id'] = raster5['name'] = cc.TESTFILENAME_NO_LAYERS  

    raster6 = setTestRaster(20,2) #more layer map
    raster6['id'] = raster6['name'] = cc.TESTFILENAME_MORE_LAYERS

    raster7 = setTestRaster(60,2) #many layer map
    raster7['id'] = raster7['name'] = cc.TESTFILENAME_MANY_LAYERS

    common.testRaster_openeo1 = [raster1, raster2, raster3, raster4, raster5, raster6, raster7]

    return [raster1, raster2, raster3, raster4, raster5, raster6, raster7]
  

   
   