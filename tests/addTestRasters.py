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
                    
def createSmallNumericRasterNLayers(dims, alternate=0, bndcount=1):
    rc = createEmptySmallNumericRaster()
    rc.setSize(ilwis.Size(15,12,dims))
    baseSize = 15 * 12

    data =np.empty(baseSize, dtype = np.int64)

    for count in range(0, dims):
        if ( alternate == 0):
            for i in range(len(data)):
                data[i] = (i + count*baseSize) * 10 + (bndcount-1)*10 
        if ( alternate == 1):                
            for i in range(len(data)):
                data[i] = (i + 2*baseSize + 3 * + 2 * math.sin(math.radians(1 + i*count * 10))) * 10 + (bndcount-1)*10 

            data[5 * 6] = ilwis.Const.iUNDEF # pos 0,2 is undefined 
        rc.array2raster(data, count)                 
             
                            
    return rc 

def setTestRaster(dims, bndcount = 1):
    
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
        band['bandIndex'] = i
        band['type'] = 'float'
        bdns.append(band)
        rc = createSmallNumericRasterNLayers(dims, 0, i)
        rcs.append(rc)

    url = rc.url()
    path = url.split('//')
    folder = os.path.dirname("/"+ path[1])
    path = Path(folder).as_uri()
    ilwis.setWorkingCatalog(path)  

    end = '2020-11-' + str((dims+1)*2)

    extra = {'epsg' : 4326, 'temporalExtent' : ['2020-11-01', end], 'bands' :bdns}
    text = []
    for i in range(1,dims + 1):
        begin = '2020-11-' + str(i*2 - 1)
        end = '2020-11-' + str((i)*2)
        text.append([begin, end])
    extra['textsublayers'] = text 
    raster.load(rcs, 'ilwisraster', extra)        
    raster['eo:bands'] = bdns
    #raster[cc.METADATDEFDIM][cc.DIMSPECTRALBANDS] = bdns

 

    return raster

def setTestRasters(dims):
    if common.testRaster_openeo1 != None:
        return common.testRaster_openeo1
    
    raster1 = setTestRaster(dims, 1)
    raster1['id'] = raster1['name'] = cc.TESTFILENAME1    

    raster2 = setTestRaster(dims, 4)
    raster2['id'] = raster2['name'] = cc.TESTFILENAME2        

    common.testRaster_openeo1 = [raster1, raster2]

    return [raster1, raster2]
  

   
   