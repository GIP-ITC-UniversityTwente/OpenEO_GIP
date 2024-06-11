from tests import basetests
import openeo
import sys
import openeo.rest.datacube
from openeo.processes import array_element, normalized_difference
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])


def execReduceDimension(name, reduc):
    name = name + ".tif"
    b2 = cube_s2.reduce_dimension(dimension="t", reducer=reduc )
    result = b2.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()
    job.get_results().download_files(name) 
    basetests.testCheckSumMulti('reduce_dimension', name)       
   
def ndif(data):
	TB01 = array_element(data, index = 1)
	TB03 = array_element(data, label = "TB03") # or a label

	ndvi = normalized_difference(TB01, TB03)

	return ndvi

class TestReduceDimension(basetests.BaseTest):

    def test_01_add_dimension(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1: execReduceDimension(name='mean1', reduc='mean'), 'reducedimensiona',"reduce a dimension using aggregate method")
        basetests.testExceptionCondition1(self, True, lambda r1: execReduceDimension(name='udf1', reduc=ndif), 'reducedimensionb',"reduce a dimension using udf method") 
        basetests.testExceptionCondition1(self, False, lambda r1: execReduceDimension(name='mean1', reduc='dummy123'), 'reducedimensionc',"reduce a dimension using not existing method, fail")       
