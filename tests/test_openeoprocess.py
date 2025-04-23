import unittest
import sys
import pathlib
import json
import os
import ilwis
import datacube
import common
import constants.constants as cc

pp = pathlib.Path(__file__).parent.resolve()
pp = '/home/mschouwen/projects/openeo/openeo/'

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')
sys.path.append(pp)

from workflow.openeoprocess import OpenEOParameter
from unittest.mock import MagicMock, patch
from workflow.openeoprocess import OpenEOProcess
from workflow.processGraph import ProcessGraph
from constants.constants import STATUSCREATED

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
        self.mock_constants.STATUSCREATED = STATUSCREATED

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
        self.mock_constants.STATUSCREATED = STATUSCREATED

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
        self.mock_envelope = patch('workflow.openeoprocess.ilwis.Envelope', MagicMock()).start()
        self.addCleanup(patch.stopall)

        # Mocking rasterdata.RasterData
        self.mock_raster_data = patch('workflow.openeoprocess.rasterdata.RasterData', MagicMock()).start()

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

    @patch('workflow.openeoprocess.re.split')
    def test_saveResult_with_valid_data(self, mock_re_split):
        # Mocking data and dependencies
        mock_re_split.return_value = ["part1", "part2"]

        mock_raster = MagicMock()
        mock_raster.envelope.return_value = "mocked_env"
        mock_store = MagicMock()
        mock_raster.store = mock_store

        mock_raster_data_instance = MagicMock()
        mock_raster_data_instance.getRasters.return_value = [mock_raster]
        mock_raster_data_instance.__getitem__.return_value = "mocked_title"


        # Set the side_effect for isinstance
        data = [mock_raster_data_instance]
        mock_isinstance = patch('builtins.isinstance')
            # Define a custom side_effect function for isinstance
        def isinstance_side_effect(obj, cls):
            if cls == datacube.DataCube:
                return obj is mock_raster_data_instance  # Directly compare with the mock object
            elif cls == list:
                return type(obj) is list  # Use type() instead of isinstance
            elif cls == str:
                return type(obj) is str  # Use type() instead of isinstance
            return False  # Default to False for other types

        # Set the side_effect for isinstance
        mock_isinstance.side_effect = isinstance_side_effect

        # Call saveResult
        result = self.process.saveResult("/mocked/path", data, "mocked_format")

        # Verify raster.store was called
        mock_raster.store.assert_called_once_with("file:///mocked/path/mocked_title_0", "mocked_format", "gdal")

        # Verify the result
        self.assertEqual(result, ["part1", "part2"])

        # Verify raster.store was called
        mock_raster.store.assert_called_once_with("file:///mocked/path/mocked_title_0", "mocked_format", "gdal")

        # Verify the result
        self.assertEqual(result, ["part1", "part2"])

    def test_saveResult_with_empty_data(self):
        # Call saveResult with None data
        result = self.process.saveResult("/mocked/path", None, "mocked_format")

        # Verify the result is None
        self.assertIsNone(result)

    def test_saveResult_with_non_raster_data(self):
        # Mocking non-raster data
        data = ["non_raster_data"]

        # Call saveResult
        result = self.process.saveResult("/mocked/path", data, "mocked_format")

        # Verify the result is None
        self.assertIsNone(result)

    @patch('workflow.openeoprocess.re.split')
    def test_saveResult_with_multiple_rasters(self, mock_re_split):
        # Mocking multiple raster data
        mock_raster1 = MagicMock()
        mock_raster1.envelope.return_value = "env1"
        mock_raster1.store = MagicMock()

        mock_raster2 = MagicMock()
        mock_raster2.envelope.return_value = "env2"
        mock_raster2.store = MagicMock()

        mock_raster_data_instance = MagicMock()
        mock_raster_data_instance.getRasters.return_value = [mock_raster1, mock_raster2]
        mock_raster_data_instance.__getitem__.return_value = "mocked_title"

        data = [mock_raster_data_instance]
        mock_re_split.return_value = ["part1", "part2"]

        # Call saveResult
        result = self.process.saveResult("/mocked/path", data, "mocked_format")

        # Verify raster.store was called for both rasters
        mock_raster1.store.assert_called_once_with("file:///mocked/path/mocked_title_0", "mocked_format", "gdal")
        mock_raster2.store.assert_called_once_with("file:///mocked/path/mocked_title_1", "mocked_format", "gdal")

        # Verify the result
        self.assertEqual(result, ["part1", "part2"])



      
if __name__ == '__main__':
    unittest.main()