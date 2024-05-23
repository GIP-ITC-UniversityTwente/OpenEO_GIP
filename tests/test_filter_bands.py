from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, filter_bands
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])

def execFilterBands(operation, bands):
    name = operation + ".tif"
    b2 = filter_bands(cube_s2, bands)
    b2.download(name)
    basetests.testCheckSumMulti('filter_bands', operation, name)


class TestFilterBands(basetests.BaseTest):

    def test_01_array_element(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, bands=['TB01']), 'basic',"filter on one bands")
