import unittest
import configglobals
from zlib import adler32
import shutil
import os
import json
import openeo


CUSTOM_EX = 'CUSTOM_EX@'
CMESS = '\033[92m'
CERR = '\033[93m'
CENDMESS = '\033[0m'

def openConnection() :
     conn = openeo.connect("http://127.0.0.1:5000")
     conn.authenticate_basic("tester", "pwd") 
     ##conn = openeo.connect("http://cityregions.roaming.utwente.nl:5000")
     return conn
 
def customError(regular_message, custom_message):
     if not custom_message.find(CUSTOM_EX) == -1:
          parts = custom_message.split('@')
          regular_message = regular_message + "," + parts[1]
     return regular_message              
    
def testExceptionCondition6(p, expected,func, parm1, parm2, parm3, parm4, parm5, parm6, message):
      try:
           obj = func(parm1, parm2, parm3, parm4, parm5, parm6)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)

def testExceptionCondition5(p, expected, func, parm1, parm2, parm3, parm4, parm5, message):
      try:
           obj = func(parm1, parm2, parm3, parm4, parm5)
           p.isTrue(True, expected)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(True, not expected)
           return False

def testExceptionCondition4(p, expected, func, parm1, parm2, parm3, parm4, message):
      try:
           obj = func(parm1, parm2, parm3, parm4)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)
           return None

def testExceptionCondition3(p, expected, func, parm1, parm2, parm3, message):
      try:
           obj = func(parm1, parm2, parm3)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)
           return None

def testExceptionCondition2(p, expected, func, parm1, parm2, message):
      try:
           obj = func(parm1, parm2)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)

def testExceptionCondition1(p, expected, func, parm1, message):
      try:
           obj = func(parm1)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           msg = str(ex)
           message = customError(message, msg)
           p.isTrue(not expected, message, msg)
           return None 

def checksum(p): 
     BLOCKSIZE=256*1024*1024
     asum = 1
     with open(p,mode="rb") as f:
          while True:
               data = f.read(BLOCKSIZE)
               if not data:
                    break
               asum = adler32(data, asum)
               if asum < 0:
                    asum += 2**32

               return hex(asum)[2:10].zfill(8).lower()   

def testCheckSumMulti(group, outputDir):
     testdir = configglobals.testdir
     testdiragg = testdir + "/" + group
     testFolder = os.path.join(testdiragg, outputDir)
     if os.path.exists(outputDir):
          if not os.path.exists(testdiragg):
               os.makedirs(testdiragg)
          checksumfile = testdir + "/checksums.txt"
          data = {}
          if os.path.exists(checksumfile):
               file_stats = os.stat(checksumfile)
               if file_stats.st_size > 0:
                    with open(checksumfile, 'r') as fp:
                         data = json.load(fp)
               else: # probably rest of an aborted test
                    os.remove(checksumfile) 

          if os.path.exists(testFolder):        
              shutil.rmtree(testFolder)
          shutil.move(outputDir, testFolder)
          dir_list = os.listdir(testFolder)
          for f in dir_list:
               if f.find('.json') != -1:
                    continue
               fn = os.path.join(testFolder, f)
               cc = checksum(fn)

               key = group + outputDir + f
               if key in data:
                    if not data[key]['checksum'] == cc:
                         raise Exception(CUSTOM_EX + 'checksum error')
               else:
                    entry = {'group' : group, 'operation' : outputDir, 'checksum' : cc}
                    data[key] = entry
          with open(checksumfile, 'w') as fp:
               json.dump(data, fp)     
     else:
          raise Exception('file not found')


def removeOutput(outputFile):
     if os.path.exists(outputFile):        
          os.remove(outputFile)          

def testCheckSumSingle(group, operation, outputFile):
     testdir = configglobals.testdir
     testdiragg = testdir + "/" + group
     testfile = os.path.join(testdiragg, outputFile)
     if os.path.exists(outputFile):
          file_stats = os.stat(outputFile) # sometimes if an error occurs openeo still creates an empty result file
          if file_stats.st_size == 0:
               raise Exception('file not found')

          if not os.path.exists(testdiragg):
               os.makedirs(testdiragg)
          checksumfile = testdir + "/checksums.txt"
          data = {}
          if os.path.exists(checksumfile):
               file_stats = os.stat(checksumfile)
               if file_stats.st_size > 0:
                    with open(checksumfile, 'r') as fp:
                         data = json.load(fp)
               else: # probably rest of an aborted test
                    os.remove(checksumfile)               
          if os.path.exists(testfile):        
               os.remove(testfile)
          shutil.move(outputFile, testdiragg)
          cc = checksum(testfile)

          key = group + operation
          if key in data:
               if not data[key]['checksum'] == cc:
                    raise Exception(CUSTOM_EX + 'checksum error')
          else:
               entry = {'group' : group, 'operation' : operation, 'checksum' : cc}
               data[key] = entry
               with open(checksumfile, 'w') as fp:
                    json.dump(data, fp)     
     else:
          raise Exception('file not found')

class BaseTest(unittest.TestCase):
    
     def prepare(self, testdir):
         print('\033[96m' + 'started:' + testdir + CENDMESS)
         

     def decorateFunction(self, mod, fn) :
        self.decoration = mod + " ==> " + fn 
        print('\033[94m' + "\n" + self.decoration +  CENDMESS + "\n")


     def isEqual(self, str1, str2, msg):
        cls = configglobals.TestManager()
        cls.incTestCount()
        color = CMESS
        result = 'SUCCESS'
        if (str1 != str2):
           color = CERR
           cls.addErrorNumber(cls.testCount())
           result = 'FAIL'

        print(color + f'{cls.testCount():5} {msg:65}  {result}' + CENDMESS)

     def isAlmostEqualNum(self, num1, num2, delta, msg) :
        cls = configglobals.TestManager()
        cls.incTestCount()
        color = CMESS
        result = 'SUCCESS'
        if (abs(num1 - num2) > delta):
           cls.addErrorNumber(cls.testCount())
           color = CERR
           result = 'FAIL'

        print(color + f'{cls.testCount():5} {msg:65}  {result}'  + CENDMESS)

     
     def isTrue(self, b, msg, error=''):
        cls = configglobals.TestManager()
        cls.incTestCount() 
        result = 'FAIL'
        color = CMESS
        if (b):
           result = 'SUCCESS'
        else:
            color = CERR
            cls.addErrorNumber(cls.testCount(), error)         
        print(color + f'{cls.testCount():5} {msg:65}  {result}' + CENDMESS)

     def isFalse(self, b, msg):
        cls = configglobals.TestManager()
        cls.incTestCount() 
        result = 'SUCCESS'
        color = CMESS
        if (b):
            cls.addErrorNumber(cls.testCount())  
            color = CERR
            result = 'FAIL'
        print(color + f'{cls.testCount():5} {msg:65}  {result}' + CENDMESS)


        

   


    


           

