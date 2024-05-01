from tests import basetests

import openeo
import sys


def runLoadCollection(spat_ext={}, temp_ext=[], dataset="", sbands=[]):

    if spat_ext == {}:
        spat_ext = {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
    if temp_ext == []:
        temp_ext = ["2018-11-18", "2019-10-24"]
    if dataset == "":
        dataset = "Sentinel2TimeSeriesData" 
    if sbands == [None]: # place holder value for an empty list; else default and a truely empty list conflict
        sbands = []
    elif sbands == []:
        sbands = ['B02']        

    conn = openeo.connect("http://127.0.0.1:5000")
    conn.authenticate_basic("tester", "pwd")    
    ##conn = openeo.connect("http://cityregions.roaming.utwente.nl:5000")
  

    cube_s2 = conn.load_collection(
        dataset,
        spatial_extent = spat_ext,
        temporal_extent = temp_ext,
        bands = sbands
    )
    cube_s2 = cube_s2.reduce_dimension(dimension="t", reducer="mean" )
    cube_s2.download("bbb3.tif")


class TestLoadCollection(basetests.BaseTest):
   # def setUp(self):
   #     self.prepare('base')

    def test_01_SpatialExtent(self): 
        self.prepare(sys._getframe().f_code.co_name)
      
        spat_ext = {"eest": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. illegal bounds,eest")

        spat_ext = {"east": 119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. label duplicate")  

        spat_ext = {"west": -119.0861, "south": 35.959, "east":  -119.2201, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. east <-> west switch") 

        spat_ext = {"west": -119.2201, "south": 36.0574, "east":  -119.0861, "north": 35.959}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. north <-> east switch")

        spat_ext = {"west": -50, "south": 35.959, "east": -49, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. no overlap bounds  north <-> south")                   

        spat_ext = {"west": -119.2201, "south": 10, "east":  -119.0861, "north": 11}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection. no overlap bounds  west <-> east")

        spat_ext = {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"load_collection spatial extent, success") 
    

    def test_02_TemporalExtent(self):
        self.prepare(sys._getframe().f_code.co_name)

        temp_ext = ["2000-11-18", "2001-10-24"]
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(temp_ext=r1), temp_ext,"load_collection temporal extent wrong")  

        temp_ext = ["2018-11-18", "2019-10-24"]
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(temp_ext=r1), temp_ext,"load_collection temporal extent, success") 

    def test_03_Bands(self):
        self.prepare(sys._getframe().f_code.co_name)

        sbands = ['B44']
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(sbands=r1), sbands,"load_collection not existing band") 

        sbands = ['B02']
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(sbands=r1), sbands,"load_collection one band, success") 

        sbands = ['B02', 'B04']
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(sbands=r1), sbands,"load_collection two bands, success") 

        sbands = ['B02', 'B44']
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(sbands=r1), sbands,"load_collection two bands, one illegal")

    def test_04_dataset(self): 
        self.prepare(sys._getframe().f_code.co_name)

        dataset = "Sentinel2TimeSeriesData" 
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(dataset=r1), dataset,"load_collection existing data, success")                                            

        dataset = "NotExist" 
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(dataset=r1), dataset,"load_collection not existing data") 