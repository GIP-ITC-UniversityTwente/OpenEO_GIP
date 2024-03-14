from tests import basetests

from workflow import openeoprocess
from userinfo import UserInfo
from constants import constants
import openeo


def runLoadCollection(spat_ext={}, temp_extent=[], dataset="", bands=[]):

    if spat_ext == {}:
        spat_ext = {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
    if temp_extent == []:
        temp_ext = ["2018-11-18", "2019-10-24"]
    if dataset == "":
        dataset = "Sentinel2TimeSeriesData" 
    if bands == []:
        bands = ['B02']        

    conn = openeo.connect("http://127.0.0.1:5000")
    conn.authenticate_basic("tester", "pwd")    
    ##conn = openeo.connect("http://cityregions.roaming.utwente.nl:5000")
  

    cube_s2 = conn.load_collection(
        dataset,
        spatial_extent = spat_ext,
        temporal_extent = temp_ext,
        bands = ["B02"]
    )
    cube_s2 = cube_s2.reduce_dimension(dimension="t", reducer="mean" )
    result = cube_s2.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()


class TestLoadCollection(basetests.BaseTest):
    def setUp(self):
        self.prepare('base')

    def test_01_SpatialExtent(self): 
        spat_ext = {"eest": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"aborted load_collection. illegal bounds,")  

        spat_ext = {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574}
        basetests.testExceptionCondition1(self, False, lambda r1 : runLoadCollection(spat_ext=r1), spat_ext,"aborted load_collection. illegal bounds,")  