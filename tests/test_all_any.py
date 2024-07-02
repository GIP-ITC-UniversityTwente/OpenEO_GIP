from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execAll(operation, vals):
    name = operation + ".tif"
    c3 = cube_s2.process('all', data=vals)
    c3.download(name)

    basetests.removeOutput(name) # there is no real ooutput

def execAny(operation, vals):
    name = operation + ".tif"
    b = [True, False, True]
    c3 = cube_s2.process('any', data=vals) 
    c3.download(name)    
   


class TestAll(basetests.BaseTest):

    def test_01_all(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execAll(operation=r1, vals=[True, True, True]), 'allbasic1',"list of trues")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAll(operation=r1, vals=[True, False, True]), 'allbasic2',"list with false")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAll(operation=r1, vals=[True, 22, True]), 'nobool',"other types in list, fail")        
        basetests.testExceptionCondition1(self, False, lambda r1 : execAll(operation=r1, vals='wrong'), 'nolist',"not a list as input")        

    def test_01_any(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execAll(operation=r1, vals=[True, True, True]), 'allbasic1',"list of trues")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAll(operation=r1, vals=[False, False, True]), 'allbasic2',"list with false")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAll(operation=r1, vals=[True, 22, True]), 'nobool',"other types in list, fail")        
        basetests.testExceptionCondition1(self, False, lambda r1 : execAll(operation=r1, vals='wrong'), 'nolist',"not a list as input")        
         
 