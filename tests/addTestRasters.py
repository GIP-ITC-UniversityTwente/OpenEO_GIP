import ilwis
import math
import numpy as np
import common
from rasterdata import RasterData, RasterBand
import os
from pathlib import Path
import constants.constants as cc

def createEmptySmallNumericRaster():
    grf = ilwis.GeoReference("epsg:4326", ilwis.Envelope("0 25 30 60") , ilwis.Size(15,12))
    dfNum = ilwis.DataDefinition(ilwis.NumericDomain("code=value"), ilwis.NumericRange(0.0, 1000000.0, 1.0))
    rc = ilwis.RasterCoverage()
    rc.setGeoReference(grf)
    rc.setDataDef(dfNum)

    return rc 
                    
def createSmallNumericRasterNLayers(dims, alternate=0):
    rc = createEmptySmallNumericRaster()
    rc.setSize(ilwis.Size(15,12,dims))
    baseSize = 15 * 12

    data =np.empty(baseSize, dtype = np.int64)

    for count in range(0, dims):
        if ( alternate == 0):
            for i in range(len(data)):
                data[i] = (i + count*baseSize) * 10 
        if ( alternate == 1):                
            for i in range(len(data)):
                data[i] = (i + 2*baseSize + 3 * + 2 * math.sin(math.radians(1 + i*count * 10))) * 10

            data[5 * 6] = ilwis.Const.iUNDEF # pos 0,2 is undefined 
        rc.array2raster(data, count)                 
             
                            
    return rc 
    
def setTestRasters(dims):
    if common.testRaster_openeo1 != None:
        return common.testRaster_openeo1
    
    rc = createSmallNumericRasterNLayers(dims)
    raster = RasterData()
    end = '2020-11-' + str((dims+1)*2)
    extra = {'epsg' : 4326, 'temporalExtent' : ['2020-11-01', end], 'bands' : [{'name' : 'band_01'}]}
    text = []
    for i in range(1,dims + 1):
        begin = '2020-11-' + str(i*2 - 1)
        end = '2020-11-' + str((i)*2)
        text.append([begin, end])
    extra['textsublayers'] = text 

    url = rc.url()
    path = url.split('//')
    folder = os.path.dirname("/"+ path[1])
    path = Path(folder).as_uri()
    ilwis.setWorkingCatalog(path)  
    raster.load(rc, 'ilwisraster', extra)
    bdns = {}
    band = RasterBand()
    band['name'] = 'TB01'
    band['normalizedbandname']= 'TB01'
    band['details'] = {}
    band['bandIndex'] = 0
    band['type'] = 'float'
    bdns[band['name']] = band
    raster['eo:bands'] = bdns

    raster['id'] = raster['name'] = cc.TESTFILENAME1

    common.testRaster_openeo1 = [raster]

    return [raster]
  

   
   