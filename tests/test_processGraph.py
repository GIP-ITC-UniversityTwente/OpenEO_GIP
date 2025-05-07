import unittest
import sys
from unittest.mock import MagicMock

pp = '/home/mschouwen/projects/openeo/openeo/'

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')
sys.path.append(pp)

from workflow.openeoprocess import OpenEOProcess
from workflow.processGraph import ProcessGraph, ProcessNode

class TestProcessGraph(unittest.TestCase):

    def setUp(self):
        # Mocking the ProcessNode and ProcessGraph setup
        self.mock_node = MagicMock(spec=ProcessNode)
        self.mock_node.localArguments = {'key1': 'value1', 'key2': 'value2'}
        self.mock_output_node = ('node_id', self.mock_node)

        self.process_graph = ProcessGraph(source_graph={}, arguments={}, getOperation=MagicMock())
        self.process_graph.outputNodes = [self.mock_output_node]

    def test_clearLocalArgument_with_valid_index(self):
        # Ensure localArguments is not empty before clearing
        self.assertNotEqual(self.process_graph.outputNodes[0][1].localArguments, {})
        
        # Call the method
        self.process_graph.clearLocalArgument(index=0)
        
        # Assert localArguments is cleared
        self.assertEqual(self.process_graph.outputNodes[0][1].localArguments, {})

    def test_clearLocalArgument_with_invalid_index(self):
        # Call the method with an invalid index
        self.process_graph.clearLocalArgument(index=1)
        
        # Assert no changes were made to the existing node
        self.assertNotEqual(self.process_graph.outputNodes[0][1].localArguments, {})

    def test_clearLocalArgument_with_empty_outputNodes(self):
        # Clear outputNodes and call the method
        self.process_graph.outputNodes = []
        self.process_graph.clearLocalArgument(index=0)
        
        # Assert no exceptions and no changes occurred
        self.assertEqual(len(self.process_graph.outputNodes), 0)

    def test_addLocalArgument_with_valid_index(self):
        # Ensure the key does not exist before adding
        self.assertNotIn('new_key', self.process_graph.outputNodes[0][1].localArguments)
        
        # Call the method
        self.process_graph.addLocalArgument('new_key', 'new_value', index=0)
        
        # Assert the key-value pair is added
        self.assertIn('new_key', self.process_graph.outputNodes[0][1].localArguments)
        self.assertEqual(self.process_graph.outputNodes[0][1].localArguments['new_key'], 'new_value')

    def test_addLocalArgument_with_invalid_index(self):
        # Call the method with an invalid index
        with self.assertRaises(IndexError):
            self.process_graph.addLocalArgument('new_key', 'new_value', index=1)

    def test_addLocalArgument_with_empty_outputNodes(self):
        # Clear outputNodes and call the method
        self.process_graph.outputNodes = []
        with self.assertRaises(IndexError):
            self.process_graph.addLocalArgument('new_key', 'new_value', index=0)

    def test_validateNode_with_valid_node(self):
        # Mock a valid node
        self.mock_node.localArguments = {
            'arg1': {'base': 'value1', 'resolved': 'resolved_value1'},
            'arg2': {'base': 'value2', 'resolved': 'resolved_value2'}
        }
        self.mock_node.process_id = 'valid_process'
        self.process_graph.getOperation.return_value = MagicMock()

        # Call the method
        errors = self.process_graph.validateNode(self.mock_node)

        # Assert no errors are returned
        self.assertEqual(errors, [])

    def test_validateNode_with_missing_operation(self):
        # Mock a node with a missing operation
        self.mock_node.localArguments = {
            'arg1': {'base': 'value1', 'resolved': 'resolved_value1'}
        }
        self.mock_node.process_id = 'missing_process'
        self.process_graph.getOperation.return_value = None

        # Call the method
        errors = self.process_graph.validateNode(self.mock_node)

        # Assert the correct error is returned
        self.assertIn("missing 'operation' missing_process", errors)

    def test_validateNode_with_unresolved_argument(self):
        # Mock a node with an unresolved argument
        self.mock_node.localArguments = {
            'arg1': {'base': {'from_node': 'node_id'}, 'resolved': None}
        }
        self.mock_node.process_id = 'valid_process'
        self.process_graph.getOperation.return_value = MagicMock()

        # Mock id2node to return a valid node
        back_node = MagicMock()
        back_node.localArguments = {
            'arg2': {'base': 'value2', 'resolved': 'resolved_value2'}
        }
        self.process_graph.id2node = MagicMock(return_value=('node_id', back_node))

        # Call the method
        errors = self.process_graph.validateNode(self.mock_node)

        # Assert no errors are returned
        self.assertEqual(errors, [])

    def test_validateNode_with_unresolved_argument_and_missing_operation(self):
        # Mock a node with an unresolved argument and missing operation
        self.mock_node.localArguments = {
            'arg1': {'base': {'from_node': 'node_id'}, 'resolved': None}
        }
        self.mock_node.process_id = 'missing_process'
        self.process_graph.getOperation.return_value = None

        # Mock id2node to return a valid node
        back_node = MagicMock()
        back_node.localArguments = {
            'arg2': {'base': 'value2', 'resolved': 'resolved_value2'}
        }
        self.process_graph.id2node = MagicMock(return_value=('node_id', back_node))

        # Call the method
        errors = self.process_graph.validateNode(self.mock_node)

        # Assert the correct error is returned
        self.assertIn("missing 'operation' missing_process", errors)

    def test_validateNode_with_recursive_unresolved_argument(self):
        # Mock a node with a recursive unresolved argument
        self.mock_node.localArguments = {
            'arg1': {'base': {'from_node': 'valid_process'}, 'resolved': None}
        }
        self.mock_node.process_id = 'valid_process'
        self.process_graph.getOperation.return_value = MagicMock()

        # Mock id2node to return the same node (recursive case)
        self.process_graph.id2node = MagicMock(return_value=('valid_process', self.mock_node))

        # Call the method
        errors = self.process_graph.validateNode(self.mock_node)

        # Assert no infinite recursion occurs and no errors are returned
        self.assertEqual(errors, [])

    def test_validateGraph_with_no_errors(self):
        # Mock a valid node
        self.mock_node.localArguments = {
            'arg1': {'base': 'value1', 'resolved': 'resolved_value1'}
        }
        self.mock_node.process_id = 'valid_process'
        self.process_graph.getOperation.return_value = MagicMock()

        # Mock validateNode to return no errors
        self.process_graph.validateNode = MagicMock(return_value=[])

        # Call the method
        errors = self.process_graph.validateGraph()

        # Assert no errors are returned
        self.assertEqual(errors, [])
        self.process_graph.validateNode.assert_called_once_with(self.mock_node)

    def test_validateGraph_with_errors(self):
        # Mock a node with errors
        self.mock_node.localArguments = {
            'arg1': {'base': 'value1', 'resolved': None}
        }
        self.mock_node.process_id = 'invalid_process'
        self.process_graph.getOperation.return_value = None

        # Mock validateNode to return errors
        self.process_graph.validateNode = MagicMock(return_value=["missing 'operation' invalid_process"])

        # Call the method
        errors = self.process_graph.validateGraph()

        # Assert the correct errors are returned
        self.assertEqual(errors, ["missing 'operation' invalid_process"])
        self.process_graph.validateNode.assert_called_once_with(self.mock_node)

    def test_validateGraph_with_multiple_output_nodes(self):
        # Mock multiple output nodes
        mock_node_2 = MagicMock(spec=ProcessNode)
        mock_node_2.localArguments = {
            'arg2': {'base': 'value2', 'resolved': 'resolved_value2'}
        }
        mock_node_2.process_id = 'valid_process_2'

        self.process_graph.outputNodes = [
            ('node_id_1', self.mock_node),
            ('node_id_2', mock_node_2)
        ]

        # Mock validateNode to return errors for one node and no errors for the other
        self.process_graph.validateNode = MagicMock(side_effect=[
            ["missing 'operation' invalid_process"],  # Errors for the first node
            []  # No errors for the second node
        ])

        # Call the method
        errors = self.process_graph.validateGraph()

        # Assert the combined errors are returned
        self.assertEqual(errors, ["missing 'operation' invalid_process"])
        self.process_graph.validateNode.assert_any_call(self.mock_node)
        self.process_graph.validateNode.assert_any_call(mock_node_2)

    def test_validateGraph_with_empty_outputNodes(self):
        # Clear outputNodes
        self.process_graph.outputNodes = []

        # Call the method
        errors = self.process_graph.validateGraph()

        # Assert no errors are returned
        self.assertEqual(errors, [])

    def test_estimate_with_valid_output_node(self):
        # Mock a valid output node
        mock_estimation_node = MagicMock()
        mock_estimation_node.estimate.return_value = "estimated_value"
        self.process_graph.outputNodes = [self.mock_output_node]

        # Mock EstimationNode to return the mocked instance
        with unittest.mock.patch('workflow.processGraph.EstimationNode', return_value=mock_estimation_node):
            result = self.process_graph.estimate()

        # Assert the estimate method is called and the correct value is returned
        self.assertEqual(result, "estimated_value")
        mock_estimation_node.estimate.assert_called_once()

    def test_estimate_with_no_output_nodes(self):
        # Clear outputNodes
        self.process_graph.outputNodes = []

        # Call the method
        result = self.process_graph.estimate()

        # Assert no estimation is performed and None is returned
        self.assertIsNone(result)

    def test_estimate_with_exception(self):
        # Mock an output node that raises an exception
        self.process_graph.outputNodes = [self.mock_output_node]

        def mock_estimate(*args, **kwargs):
            raise Exception("Estimation error")

        with unittest.mock.patch('workflow.processGraph.EstimationNode', side_effect=mock_estimate):
            result = self.process_graph.estimate()

        # Assert the exception is caught and the error output is returned
        self.assertIsInstance(result, dict)
        self.assertFalse(result['status'] == True)
        self.assertIn("Estimation error", result['value'])

    def test_run_with_valid_output_node(self):
        # Mock a valid output node
        mock_node_execution = MagicMock()
        mock_node_execution.run.return_value = None
        mock_node_execution.outputInfo = "output_info"

        self.process_graph.outputNodes = [self.mock_output_node]

        # Mock NodeExecution to return the mocked instance
        with unittest.mock.patch('workflow.processGraph.NodeExecution', return_value=mock_node_execution):
            result = self.process_graph.run(openeojob="job", toServer="to_server", fromServer="from_server")

        # Assert the run method is called and the correct output is returned
        self.assertEqual(result, "output_info")
        mock_node_execution.run.assert_called_once_with("job", "to_server", "from_server")

    def test_run_with_no_output_nodes(self):
        # Clear outputNodes
        self.process_graph.outputNodes = []

        # Call the method
        result = self.process_graph.run(openeojob="job", toServer="to_server", fromServer="from_server")

        # Assert no execution is performed and None is returned
        self.assertIsNone(result)

    def test_run_with_exception_in_node_execution(self):
        # Mock an output node that raises an exception
        self.process_graph.outputNodes = [self.mock_output_node]

        def mock_run(*args, **kwargs):
            raise Exception("Execution error")

        with unittest.mock.patch('workflow.processGraph.NodeExecution', side_effect=mock_run):
            with self.assertRaises(Exception) as context:
                self.process_graph.run(openeojob="job", toServer="to_server", fromServer="from_server")

        # Assert the exception is raised with the correct message
        self.assertIn("Execution error", str(context.exception))

    def test_stop_with_active_startNode(self):
        # Mock a startNode with a stop method
        mock_start_node = MagicMock()
        self.process_graph.startNode = mock_start_node

        # Call the method
        self.process_graph.stop()

        # Assert the stop method of startNode is called
        mock_start_node.stop.assert_called_once()

    def test_stop_with_no_startNode(self):
        # Ensure startNode is None
        self.process_graph.startNode = None

        # Call the method
        self.process_graph.stop()

        # Assert no exceptions occur and no stop method is called
        # (no assertion needed as there's no startNode to mock)

    def test_id2node_with_valid_id(self):
        # Mock a process graph with a valid node
        self.process_graph.processGraph = {
            'node1': self.mock_node,
            'node2': MagicMock(spec=ProcessNode)
        }

        # Call the method with a valid id
        result = self.process_graph.id2node('node1')

        # Assert the correct node is returned
        self.assertEqual(result, ('node1', self.mock_node))

    def test_id2node_with_invalid_id(self):
        # Mock a process graph with no matching node
        self.process_graph.processGraph = {
            'node1': self.mock_node,
            'node2': MagicMock(spec=ProcessNode)
        }

        # Call the method with an invalid id
        result = self.process_graph.id2node('node3')

        # Assert None is returned
        self.assertIsNone(result)

    def test_id2node_with_list_of_ids(self):
        # Mock a process graph with multiple nodes
        self.process_graph.processGraph = {
            'node1': self.mock_node,
            'node2': MagicMock(spec=ProcessNode)
        }

        # Call the method with a list of ids
        result = self.process_graph.id2node(['node1', 'node2'])

        # Assert the correct nodes are returned('node2', MagicMock(spec=ProcessNode)
        p = list(self.process_graph.processGraph.items())[1]
        self.assertEqual(result, p)

    def test_id2node_with_empty_list(self):
        # Mock a process graph with nodes
        self.process_graph.processGraph = {
            'node1': self.mock_node,
            'node2': MagicMock(spec=ProcessNode)
        }

        # Call the method with an empty list
        result = self.process_graph.id2node([])

        # Assert None is returned
        self.assertIsNone(result)

    def test_determineOutputNodes_with_valid_nodes(self):
        # Mock nodes with one having a 'result' attribute
        mock_node_with_result = MagicMock()
        mock_node_with_result.result = True
        mock_node_without_result = MagicMock()

        nodes = {
            'node1': mock_node_without_result,
            'node2': mock_node_with_result
        }

        # Call the method
        self.process_graph.determineOutputNodes(nodes)

        # Assert only the node with 'result' is added to outputNodes
        self.assertEqual(len(self.process_graph.outputNodes), 1)
        self.assertEqual(self.process_graph.outputNodes[0], ('node2', mock_node_with_result))

    def test_determineOutputNodes_with_no_result_nodes(self):
        # Mock nodes with none having a 'result' attribute
        mock_node_without_result1 = MagicMock()
        mock_node_without_result2 = MagicMock()

        nodes = {
            'node1': mock_node_without_result1,
            'node2': mock_node_without_result2
        }

        # Call the method
        self.process_graph.determineOutputNodes(nodes)

        # Assert outputNodes remains empty
        self.assertEqual(len(self.process_graph.outputNodes), 0)

    def test_determineOutputNodes_with_empty_nodes(self):
        # Call the method with an empty dictionary
        self.process_graph.determineOutputNodes({})

        # Assert outputNodes remains empty
        self.assertEqual(len(self.process_graph.outputNodes), 0)

    def test_determineOutputNodes_with_multiple_result_nodes(self):
        # Mock nodes with multiple having a 'result' attribute
        mock_node_1 = MagicMock()
        mock_node_1.result = True
        mock_node_2 = MagicMock()
        mock_node_2.result = True

        nodes = {
            'node1': mock_node_1,
            'node2': mock_node_2
        }

        # Call the method
        ret = self.process_graph.determineOutputNodes(nodes)

        # Assert both nodes are added to outputNodes
        self.assertEqual(ret, False)


if __name__ == '__main__':
    unittest.main()