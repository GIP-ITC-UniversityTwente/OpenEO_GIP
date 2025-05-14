import unittest
import sys
import pathlib
import json
import os

from unittest.mock import ANY
import logging

pp = pathlib.Path(__file__).parent.resolve()
pp = '/home/mschouwen/projects/openeo/openeo/'

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')
sys.path.append(pp + '/tests')
sys.path.append(pp)

from workflow.openeoprocess import OpenEOParameter
from unittest.mock import MagicMock, patch
from workflow.openeoprocess import OpenEOProcess
from workflow.processGraph import ProcessGraph
from constants.constants import STATUSCREATED
import constants.constants as cc
from customexception import CustomException


# import common
# import datacube
import addTestRasters

testRasters = addTestRasters.setTestRasters(5)
homepath = os.environ['HOME']
if not os.path.exists(homepath + '/temp'): 
    os.makedirs(homepath + '/temp') 
if not os.path.exists(homepath + '/temp/openeotest'): 
    os.makedirs(homepath + '/temp/openeotest') 
testdir = homepath + '/temp/openeotest'       

class TestOpenEOParameter(unittest.TestCase):
    maxDiff = None
    def expectedParm(self):
        # Call toDict and verify the output
        expected_parm = OpenEOParameter()
        expected_parm.schema = {'type': 'array', 
                       'subtype' : 'string',
						"deprecated": False,
						"$schema": "http://json-schema.org/draft-07/schema#",
						"$id": "http://example.com",
						"type": "array",
			            "pattern": "/regex/",
						"enum": [],
						"minimum": 0,
						"maximum": 0,
						"minItems": 0,
						"maxItems": 0,
						"items": [],
						"property1": None,
						"property2": None
                       }
        expected_parm.name = 'a name'
        expected_parm.description = 'more text'
        expected_parm.optional = False
        expected_parm.deprecated =  False
        expected_parm.experimental = False
        expected_parm.default = 'def 101'

        return expected_parm
         
    def setUp(self):
        currentFile = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(currentFile, "test_process1.json") 
        if os.path.exists(path):
            with open(path, "r") as fp:
                    eoprocess = json.load(fp) 
  
        self.sample_param = eoprocess['tests']['base_process_full']['process']['parameters'][0]

    def test_toDict_with_default(self):
        param = OpenEOParameter(self.sample_param)
        expected_parm = self.expectedParm()
        self.assertEqual(param, expected_parm)

class TestOpenEOProcess(unittest.TestCase):
    def setUp(self):
        # Mocking ProcessGraph and its validateGraph method
        self.mock_process_graph = MagicMock()
        self.mock_process_graph.validateGraph.return_value = []

        # Mocking constants
        self.mock_constants = MagicMock()
        self.mock_constants.STATUSCREATED = cc.STATUSCREATED

        # Creating an instance of OpenEOProcess with mocked dependencies

    def test_validate_no_errors(self):
        # Simulate no errors in validateGraph
        self.mock_process_graph.validateGraph.return_value = []

        process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {	"firstMult": {
						"process_id": "dummylongfunc",
						"arguments": {
							"a": 1000
						},
						"result": True
					}
                },
                "id": "test_id",
                "description": "test_description"
            },
            id=0
        )        
        process.job_id = "test_job_id"
        errors = process.validate()
        self.assertEqual(errors, [])

    def test_validate_with_errors(self):
        # Simulate errors in validateGraph
        self.mock_process_graph.validateGraph.return_value = ["Error 1"]
        process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {	"firstMult": {
						"process_id": "dummylongfunc2",
						"arguments": {
							"a": 1000
						},
						"result": True
					}
                },
                "id": "test_id",
                "description": "test_description"
            },
            id=0
        )  

        process.job_id = "test_job_id"
        errors = process.validate()
        expected_errors = [
            {"id": "test_job_id", "code": "missing operation", "message": "missing 'operation' dummylongfunc2"}
        ]
        self.assertEqual(errors, expected_errors)

class TestSetItem(unittest.TestCase):
    def setUp(self):
        # Create a mock OpenEOProcess instance
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.some_attribute = "test_value"

    def test_setItem_existing_attribute(self):
        # Test when the attribute exists
        target_dict = {}
        result = self.process.setItem("some_attribute", target_dict)
        self.assertIn("some_attribute", result)
        self.assertEqual(result["some_attribute"], "test_value")

    def test_setItem_non_existing_attribute(self):
        # Test when the attribute does not exist
        target_dict = {}
        result = self.process.setItem("non_existing_attribute", target_dict)
        self.assertNotIn("non_existing_attribute", result)
        self.assertEqual(result, target_dict)

class TestSetItem2(unittest.TestCase):
    def setUp(self):
        # Create a mock OpenEOProcess instance
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.some_attribute = "test_value"

    def test_setItem2_existing_alt_attribute(self):
        # Test when the alternative attribute exists
        target_dict = {}
        result = self.process.setItem2("key_name", target_dict, "some_attribute")
        self.assertIn("key_name", result)
        self.assertEqual(result["key_name"], "test_value")

    def test_setItem2_non_existing_alt_attribute(self):
        # Test when the alternative attribute does not exist
        target_dict = {}
        result = self.process.setItem2("key_name", target_dict, "non_existing_attribute")
        self.assertNotIn("key_name", result)
        self.assertEqual(result, target_dict)

class TestEstimate(unittest.TestCase):
    def setUp(self):
        # Mocking ProcessGraph and its estimate method
        self.mock_process_graph = MagicMock()
        self.mock_process_graph.estimate.return_value = {"cost": 100}

        # Mocking constants
        self.mock_constants = MagicMock()
        self.mock_constants.STATUSCREATED = cc.STATUSCREATED

        # Creating an instance of OpenEOProcess with mocked dependencies
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.processGraph = self.mock_process_graph

    def test_estimate(self):
        # Call the estimate method
        result = self.process.estimate(user=MagicMock(username="test_user"))
        # Verify the result
        self.assertEqual(result, {"cost": 100})

class TestToDict(unittest.TestCase):
    def setUp(self):

        # Creating an instance of OpenEOProcess with mocked dependencies
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.job_id = "test_job_id"
        self.process.title = "Test Title"
        self.process.description = "Test Description"
        self.process.deprecated = False
        self.process.experimental = True
        self.process.submitted = "2023-01-01T00:00:00Z"
        self.process.plan = "basic"
        self.process.budget = 100
        self.process.parameters = []
        self.process.returns = {}
        self.process.categories = ["category1", "category2"]
        self.process.examples = [{"example": "example1"}]
        self.process.links = [{"link": "link1"}]
        self.process.log_level = "INFO"
        self.process.spatialextent = {"extent": "mocked_extent"}


    def test_toDict_short(self):
        # Test the short version of toDict
        result = self.process.toDict(short=True)
        expected_result = {
            "id": "test_job_id",
            "title": "Test Title",
            "description": "Test Description",
            "deprecated": False,
            "experimental": True,
            "submitted": "2023-01-01T00:00:00Z",
            "plan": "basic",
            "budget": 100,
        }
        self.assertEqual(result, expected_result)

    def test_toDict_full(self):
   
        result = self.process.toDict(short=False)
        expected_result = {
            "id": "test_job_id",
            "title": "Test Title",
            "description": "Test Description",
            "deprecated": False,
            "experimental": True,
            "submitted": "2023-01-01T00:00:00Z",
            "plan": "basic",
            "budget": 100,
            "process": {
                "summary": '',
                "id": "Test Title",
                "returns" : {},
                "categories": ["category1", "category2"],
                "examples": [{"example": "example1"}],
                "links": [{"example": "example1"}],
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
            },
            "log_level": "INFO",
            "spatialextent": {"extent": "mocked_extent"},
        }
        self.assertEqual(result, expected_result)

class TestCleanup(unittest.TestCase):
    def setUp(self):
        # Mocking common.openeoip_config
        self.mock_common = MagicMock()
        self.mock_common.openeoip_config = {
            'data_locations': {
                'root_user_data_location': {'location': '/mocked/path'}
            }
        }

        # Patching common in the OpenEOProcess module
        patcher = patch('workflow.openeoprocess.common', self.mock_common)
        self.addCleanup(patcher.stop)
        patcher.start()

        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.job_id = "test_job_id"

    @patch('os.path.isdir')
    @patch('shutil.rmtree')
    def test_cleanup_directory_exists(self, mock_rmtree, mock_isdir):
        # Simulate the directory exists
        mock_isdir.return_value = True

        # Call the cleanup method
        self.process.cleanup()

        # Verify the directory path
        expected_path = '/mocked/path/test_job_id'
        mock_isdir.assert_called_once_with(expected_path)
        mock_rmtree.assert_called_once_with(expected_path)

    @patch('os.path.isdir')
    @patch('shutil.rmtree')
    def test_cleanup_directory_does_not_exist(self, mock_rmtree, mock_isdir):
        # Simulate the directory does not exist
        mock_isdir.return_value = False

        # Call the cleanup method
        self.process.cleanup()

        # Verify the directory path
        expected_path = '/mocked/path/test_job_id'
        mock_isdir.assert_called_once_with(expected_path)
        mock_rmtree.assert_not_called()

class TestStop(unittest.TestCase):
    def setUp(self):
        # Mocking common.openeoip_config
        self.mock_common = MagicMock()
        self.mock_common.openeoip_config = {
            'data_locations': {
                'root_user_data_location': {'location': '/mocked/path'}
            }
        }

        # Patching common in the OpenEOProcess module
        patcher = patch('workflow.openeoprocess.common', self.mock_common)
        self.addCleanup(patcher.stop)
        patcher.start()

        # Patching Pipe
        self.mock_pipe = patch('workflow.openeoprocess.Pipe', MagicMock(return_value=(MagicMock(), MagicMock())))
        self.addCleanup(self.mock_pipe.stop)
        self.mock_pipe.start()

        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.job_id = "test_job_id"

    @patch('workflow.openeoprocess.json.dumps')
    @patch('workflow.openeoprocess.OpenEOProcess.cleanup')
    def test_stop(self, mock_cleanup, mock_json_dumps):
        # Mocking json.dumps
        mock_json_dumps.return_value = '{"job_id": "test_job_id", "status": "stop"}'

        # Call the stop method
        self.process.stop()

        # Verify json.dumps was called with the correct data
        mock_json_dumps.assert_called_once_with({"job_id": "test_job_id", "status": "stop"})

        # Verify sendTo.send was called with the correct message
        self.process.sendTo.send.assert_called_once_with('{"job_id": "test_job_id", "status": "stop"}')

        # Verify cleanup was called
        mock_cleanup.assert_called_once()

class TestSaveResult(unittest.TestCase):
    def setUp(self):
        # Mocking ilwis.Envelope
        self.addCleanup(patch.stopall)

     
        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )

    def test_saveResult_with_valid_data(self):
        # Mocking data and dependencies
     
        data = testRasters[0]

        # Call saveResult
        result = self.process.saveResult(testdir, [data], "GTiff")
      
        # Verify the result
        
        self.assertEqual(result, ['0.000000', '25.000000', '30.000000', '60.000000'])

   

    def test_saveResult_with_empty_data(self):
        # Call saveResult with None data
        result = self.process.saveResult(testdir, None, "GTiff")

        # Verify the result is None
        self.assertIsNone(result)

    def test_saveResult_with_non_raster_data(self):
        data = 21

        # Call saveResult
        result = self.process.saveResult("/mocked/path", data, "mocked_format")

        # Verify the result is None
        self.assertIsNone(result)

    def test_saveResult_with_multiple_rasters(self):
        data1 = testRasters[0]
        data2 = testRasters[1]

        # Call saveResult
        result = self.process.saveResult(testdir, [data1,data2], "GTiff")

        # Verify the result
        self.assertEqual(result,  ['0.000000', '25.000000', '30.000000', '60.000000'])

class TestRun(unittest.TestCase):
    def setUp(self):
        # Mocking dependencies
        self.mock_process_graph = MagicMock()
        self.mock_process_graph.run.return_value = {"status": cc.STATUSJOBDONE, "spatialextent": "mocked_extent"}
        self.mock_constants = MagicMock()
        self.mock_constants.STATUSJOBDONE = cc.STATUSCREATED
        self.mock_constants.STATUSSTOPPED = cc.STATUSSTOPPED
        self.mock_constants.STATUSERROR = cc.STATUSERROR

        # Patching dependencies
        patcher_common = patch('workflow.openeoprocess.common', MagicMock())
        self.addCleanup(patcher_common.stop)
        self.mock_common = patcher_common.start()

        patcher_customexception = patch('workflow.openeoprocess.customexception', MagicMock())
        self.addCleanup(patcher_customexception.stop)
        self.mock_customexception = patcher_customexception.start()

        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.processGraph = self.mock_process_graph
        self.process.job_id = "test_job_id"

        # Mocking pipes
        self.process.fromServer = MagicMock()
        self.process.sendTo = MagicMock()

        # Mocking private methods
        self.process._logJobStart = MagicMock()
        self.process._handleProcessGraphOutput = MagicMock()
        self.process._handleRunException = MagicMock()

    def test_run_successful_execution(self):
        # Mocking successful process graph execution
        self.mock_process_graph.run.return_value = {"status": cc.STATUSJOBDONE, "spatialextent": "mocked_extent"}

        # Call the run method
        self.process.run(toServer=MagicMock())

        # Verify that the private methods were called
        self.process._logJobStart.assert_called_once()
        self.process._handleProcessGraphOutput.assert_called_once_with(
            {"status": cc.STATUSJOBDONE, "spatialextent": "mocked_extent"},
            ANY,
            ANY,
            ANY
        )
        self.process._handleRunException.assert_not_called()

    def test_run_with_exception(self):
        # Mocking an exception during process graph execution
        self.mock_process_graph.run.side_effect = Exception("Testing Exception")

        # Call the run method
        self.process.run(toServer=MagicMock())

        # Verify that the private methods were called
        self.process._logJobStart.assert_called_once()
        self.process._handleProcessGraphOutput.assert_not_called()
        self.process._handleRunException.assert_called_once_with(ANY, ANY)

    def test_run_with_custom_exception(self):
        # Mocking a custom exception during process graph execution
        self.mock_process_graph.run.side_effect = CustomException("test_job_id", cc.STATUSERROR,"dummy", "Custom Exception")

        # Call the run method
        self.process.run(toServer=MagicMock())

        # Verify that the private methods were called
        self.process._logJobStart.assert_called_once()
        self.process._handleProcessGraphOutput.assert_not_called()
        self.process._handleRunException.assert_called_once_with(ANY, ANY)

class TestLogJobStart(unittest.TestCase):
    def setUp(self):
        # Mocking openeologging.logMessage
        self.mock_common = MagicMock()
        patcher = patch('workflow.openeoprocess.common', self.mock_common)
        self.addCleanup(patcher.stop)
        patcher.start()

        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "process" : {},
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.job_id = "test_job_id"
        self.process.title = "Test Title"

    def test_logJobStart(self):
        # Call _logJobStart
        time_start = "2023-01-01T00:00:00Z"
        self.process._logJobStart(time_start)

        # Verify openeologging.logMessage was called with the correct arguments
        self.mock_openeologging.logMessage.assert_called_once_with(
            logging.INFO,
            "started job_id: test_job_id with name: Test Title",
            self.process.user.username
        )

class TestHandleProcessGraphOutput(unittest.TestCase):
    def setUp(self):
        # Mocking dependencies
        self.mock_constants = MagicMock()
        self.mock_constants.STATUSJOBDONE = cc.STATUSJOBDONE
        self.mock_constants.STATUSSTOPPED = cc.STATUSSTOPPED

        # Patching dependencies
        patcher_common = patch('workflow.openeoprocess.common', MagicMock())
        self.addCleanup(patcher_common.stop)
        self.mock_common = patcher_common.start()

        # Creating an instance of OpenEOProcess
        self.process = OpenEOProcess(
            user=MagicMock(username="test_user"),
            request_json={
                "process_graph": {
                    "firstMult": {
                        "process_id": "dummylongfunc",
                        "arguments": {"a": 1000},
                        "result": True,
                    }
                },
                "id": "test_id",
                "description": "test_description",
            },
            id=0,
        )
        self.process.job_id = "test_job_id"
        self.process.spatialextent = None
        self.process.status = None
        self.process._saveMetadata = MagicMock()
        self.process.cleanup = MagicMock()

    def test_handleProcessGraphOutput_with_spatialextent(self):
        output_info = {
            "status": cc.STATUSJOBDONE,
            "spatialextent": "mocked_extent"
        }
        toServer = MagicMock()
        time_start = "2023-01-01T00:00:00Z"
        time_end = "2023-01-01T01:00:00Z"

        self.process._handleProcessGraphOutput(output_info, toServer, time_start, time_end)

        self.assertEqual(self.process.spatialextent, "mocked_extent")
        self.assertEqual(self.process.returns, output_info)
        toServer.put.assert_called_once_with({
            'type': 'progressevent',
            'job_id': "test_job_id",
            'progress': 'job finished',
            'last_updated': time_end,
            'status': cc.STATUSJOBDONE,
            'current_operation': '?'
        })
        self.assertEqual(self.process.status, cc.STATUSJOBDONE)
        self.process._saveMetadata.assert_called_once_with(output_info, time_start, time_end)
        self.process.cleanup.assert_not_called()

    def test_handleProcessGraphOutput_with_status_stopped(self):
        output_info = {
            "status": cc.STATUSSTOPPED
        }
        toServer = MagicMock()
        time_start = "2023-01-01T00:00:00Z"
        time_end = "2023-01-01T01:00:00Z"

        self.process._handleProcessGraphOutput(output_info, toServer, time_start, time_end)

        self.assertEqual(self.process.returns, output_info)
        toServer.put.assert_called_once_with({
            'type': 'progressevent',
            'job_id': "test_job_id",
            'progress': 'job finished',
            'last_updated': time_end,
            'status': cc.STATUSJOBDONE,
            'current_operation': '?'
        })
        self.process.cleanup.assert_called_once()
        self.process._saveMetadata.assert_called_once_with(output_info, time_start, time_end)

    def test_handleProcessGraphOutput_with_none_output_info(self):
        output_info = None
        toServer = MagicMock()
        time_start = "2023-01-01T00:00:00Z"
        time_end = "2023-01-01T01:00:00Z"

        self.process._handleProcessGraphOutput(output_info, toServer, time_start, time_end)

        self.assertEqual(self.process.returns, None)
        toServer.put.assert_called_once_with({
            'type': 'progressevent',
            'job_id': "test_job_id",
            'progress': 'job finished',
            'last_updated': time_end,
            'status': cc.STATUSJOBDONE,
            'current_operation': '?'
        })
        self.assertEqual(self.process.status, cc.STATUSJOBDONE)
        self.process._saveMetadata.assert_called_once_with(output_info, time_start, time_end)
        self.process.cleanup.assert_not_called()



      
if __name__ == '__main__':
    unittest.main()