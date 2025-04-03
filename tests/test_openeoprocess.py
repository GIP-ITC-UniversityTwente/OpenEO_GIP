import unittest
import sys
import pathlib
import json
import os
import ilwis
import rasterdata

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

      
if __name__ == '__main__':
    unittest.main()