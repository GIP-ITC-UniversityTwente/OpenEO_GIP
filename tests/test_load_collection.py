from tests import basetests

import openeo
import sys

def runLoadCollection(spat_ext={}, temp_ext=[], dataset="", sbands=[], name='test', multi = False):

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

    conn = basetests.openConnection()
  

    cube_s2 = conn.load_collection(
        dataset,
        spatial_extent = spat_ext,
        temporal_extent = temp_ext,
        bands = sbands
    )
    if multi:
        result = cube_s2.save_result("GTiff")
        job = result.create_job()
        job.start_and_wait()
        job.get_results().download_files(name)

        basetests.testCheckSumMulti('load_collection', name)
    else:
        opf = name + ".tif"
        cube_s2.download(opf)

        basetests.testCheckSumSingle('load_collection', name, opf)

class TestLoadCollection(basetests.BaseTest):
   # def setUp(self):
   #     self.prepare('base')

    def test_01_SpatialExtent(self): 
        self.prepare(sys._getframe().f_code.co_name)
      
        spat_ext = {"eest": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc1'), spat_ext,"load_collection. illegal bounds,eest")

        spat_ext = {"east": 119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc2'), spat_ext,"load_collection. label duplicate")  

        spat_ext = {"west": -119.0861, "south": 35.959, "east":  -119.2201, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc3'), spat_ext,"load_collection. east <-> west switch") 

        spat_ext = {"west": -119.2201, "south": 36.0574, "east":  -119.0861, "north": 35.959}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc4'), spat_ext,"load_collection. north <-> east switch")

        spat_ext = {"west": -50, "south": 35.959, "east": -49, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc5'), spat_ext,"load_collection. no overlap bounds  north <-> south")                   

        spat_ext = {"west": -119.2201, "south": 10, "east":  -119.0861, "north": 11}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1, name='lc6'), spat_ext,"load_collection. no overlap bounds  west <-> east")

        spat_ext = {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(spat_ext=r1, name='lc7'), spat_ext,"load_collection spatial extent, success") 
    

    def test_02_TemporalExtent(self):
        self.prepare(sys._getframe().f_code.co_name)

        temp_ext = ["2000-11-18", "2001-10-24"]
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(temp_ext=r1, name='lc8'), temp_ext,"load_collection temporal extent wrong")  

        temp_ext = ["2018-11-18", "2019-10-24"]
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(temp_ext=r1, name='lc9'), temp_ext,"load_collection temporal extent, success") 

    def test_03_Bands(self):
        self.prepare(sys._getframe().f_code.co_name)

        sbands = ['B44']
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(sbands=r1, name='lc10'), sbands,"load_collection not existing band") 

        sbands = ['B02']
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(sbands=r1, name='lc11'), sbands,"load_collection one band, success") 

        sbands = ['B02', 'B04']
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(sbands=r1, multi=True, name='lc12'), sbands,"load_collection two bands, success") 

        sbands = ['B02', 'B44']
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(sbands=r1, multi=True, name='lc13'), sbands,"load_collection two bands, one illegal")

    def test_04_dataset(self): 
        self.prepare(sys._getframe().f_code.co_name)

        dataset = "Sentinel2TimeSeriesData" 
        basetests.testExceptionCondition1(self, True, lambda r1 : runLoadCollection(dataset=r1, name='lc14'), dataset,"load_collection existing data, success")                                            

        dataset = "NotExist" 
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(dataset=r1, name='lc15'), dataset,"load_collection not existing data") 