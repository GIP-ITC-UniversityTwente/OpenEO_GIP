from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, filter_bands, apply
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])

   
def execFilterBands(operation, bnds=[], wvl=[]):
    name = operation + ".tif"
    b2 = cube_s2.apply(lambda : filter_bands(cube_s2, bnds, wvl))
    if len(bnds) == 1 and len(wvl) == 0:
        b2.download(name)
        basetests.testCheckSumSingle('filter_bands', operation, name)
    else:
        result = b2.save_result("GTiff")
        job = result.create_job()
        job.start_and_wait()
        job.get_results().download_files(operation) 
        basetests.testCheckSumMulti('filter_bands', operation)       
   


class TestFilterBands(basetests.BaseTest):

    def test_01_filter_bands_element(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, bnds=['TB01']), 'filterbasicname',"filter on one bands")
        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, bnds=['TB01','TB02']), 'filtertwobands',"filter on two bands")        
        basetests.testExceptionCondition1(self, False, lambda r1 : execFilterBands(operation=r1, bnds=['TB11']), 'basic',"filterwrongname")        
        basetests.testExceptionCondition1(self, False, lambda r1 : execFilterBands(operation=r1, bnds=['TB01, TB11']), 'filter2wrongname',"filter more bands, one wrong name")                

        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, wvl=[[0.8, 1.0]]), 'filterbasicwavel',"filter wavelengths")
        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, bnds=['TB01'], wvl=[[0.8, 1.0]]), 'filternameweavel',"filter wavelengths and band name")  
        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, bnds=['TB03'], wvl=[[0.8, 1.0]]), 'filterdupnamewvl',"filter wavelengths, band name already present")          
        basetests.testExceptionCondition1(self, False, lambda r1 : execFilterBands(operation=r1, wvl=[['no number', 1.0]]), '"filerillegalwvl','illegal wavelength')   
        basetests.testExceptionCondition1(self, False, lambda r1 : execFilterBands(operation=r1, wvl=[[1.0, 0.8]]), 'filterwrongminmax',"filter wavelengths wrong order min/max")
        basetests.testExceptionCondition1(self, False, lambda r1 : execFilterBands(operation=r1, wvl=[[0.8]]), 'filterwrongminmax2',"filter wavelengthsrange needs 2 values")
        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, wvl=[[0.8, 1.0], [0.4, 0.6]]), 'filterbasicwavel2sets',"filter wavelengths 2 sets ")             
