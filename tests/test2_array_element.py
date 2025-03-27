import openeo.processes
from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, sum
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01', 'TB02'])

def execArrayElement(operation, band):
    name = operation + ".tif"
    b2 = cube_s2.band(band)
    b2.download(name)
    basetests.testCheckSumSingle('array_element', operation, name)


class TestArrayElement(basetests.BaseTest):

    def test_01_array_element(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execArrayElement(operation=r1, band='TB02'), 'single',"split single band")
        basetests.testExceptionCondition1(self, False, lambda r1 : execArrayElement(operation=r1, band='XXX'), 'singleF',"Fail for not existing band")        
 