import unittest
from unittest.mock import MagicMock, patch
import sys
import pathlib

pp = pathlib.Path(__file__).parent.resolve()
pp = '/home/mschouwen/projects/openeo/openeo/'

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')
sys.path.append(pp + '/tests')
sys.path.append(pp)

from workflow.nodeexecution import NodeExecution

class TestNodeExecution(unittest.TestCase):

    @patch('workflow.nodeexecution.NodeExecution._resolveParameters')
    @patch('workflow.nodeexecution.NodeExecution._executeNode')
    def test_run_success(self, mock_executeNode, mock_resolveParameters):
        """
        Test the `run` method when all parameters are resolved successfully.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act
        node_execution.run(openeojob, toServer, fromServer)

        # Assert
        mock_resolveParameters.assert_called_once_with(openeojob, toServer, fromServer)
        mock_executeNode.assert_called_once_with(openeojob, toServer, fromServer)

    @patch('workflow.nodeexecution.NodeExecution._resolveParameters')
    @patch('workflow.nodeexecution.NodeExecution._executeNode')
    def test_run_resolve_parameters_failure(self, mock_executeNode, mock_resolveParameters):
        """
        Test the `run` method when resolving parameters raises an exception.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_resolveParameters.side_effect = Exception("Parameter resolution failed")

        # Act & Assert
        with self.assertRaises(Exception) as context:
            node_execution.run(openeojob, toServer, fromServer)

        self.assertEqual(str(context.exception), "Parameter resolution failed")
        mock_resolveParameters.assert_called_once_with(openeojob, toServer, fromServer)
        mock_executeNode.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution._resolveParameters')
    @patch('workflow.nodeexecution.NodeExecution._executeNode')
    def test_run_execute_node_failure(self, mock_executeNode, mock_resolveParameters):
        """
        Test the `run` method when executing the node raises an exception.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_executeNode.side_effect = Exception("Node execution failed")

        # Act & Assert
        with self.assertRaises(Exception) as context:
            node_execution.run(openeojob, toServer, fromServer)

        self.assertEqual(str(context.exception), "Node execution failed")
        mock_resolveParameters.assert_called_once_with(openeojob, toServer, fromServer)
        mock_executeNode.assert_called_once_with(openeojob, toServer, fromServer)

    @patch('workflow.nodeexecution.NodeExecution._resolveParameter')
    def test_resolve_parameters_all_resolved(self, mock_resolveParameter):
        """
        Test `_resolveParameters` when all parameters are already resolved.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {
            'param1': {'resolved': 'value1'},
            'param2': {'resolved': 'value2'}
        }
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act
        node_execution._resolveParameters(openeojob, toServer, fromServer)

        # Assert
        mock_resolveParameter.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution._resolveParameter')
    def test_resolve_parameters_some_unresolved(self, mock_resolveParameter):
        """
        Test `_resolveParameters` when some parameters are unresolved.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {
            'param1': {'resolved': 'value1'},
            'param2': {'resolved': None, 'base': 'base_value2'}
        }
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_resolveParameter.return_value = 'resolved_value2'

        # Act
        node_execution._resolveParameters(openeojob, toServer, fromServer)

        # Assert
        mock_resolveParameter.assert_called_once_with(openeojob, toServer, fromServer, 'param2', 'base_value2')
        self.assertEqual(processNode.localArguments['param2']['resolved'], 'resolved_value2')

    @patch('workflow.nodeexecution.NodeExecution._resolveParameter')
    def test_resolve_parameters_all_unresolved(self, mock_resolveParameter):
        """
        Test `_resolveParameters` when all parameters are unresolved.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {
            'param1': {'resolved': None, 'base': 'base_value1'},
            'param2': {'resolved': None, 'base': 'base_value2'}
        }
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_resolveParameter.side_effect = ['resolved_value1', 'resolved_value2']

        # Act
        node_execution._resolveParameters(openeojob, toServer, fromServer)

        # Assert
        self.assertEqual(mock_resolveParameter.call_count, 2)
        mock_resolveParameter.assert_any_call(openeojob, toServer, fromServer, 'param1', 'base_value1')
        mock_resolveParameter.assert_any_call(openeojob, toServer, fromServer, 'param2', 'base_value2')
        self.assertEqual(processNode.localArguments['param1']['resolved'], 'resolved_value1')
        self.assertEqual(processNode.localArguments['param2']['resolved'], 'resolved_value2')

    @patch('workflow.nodeexecution.NodeExecution._resolveParameter')
    def test_resolve_parameters_ignored_non_dict(self, mock_resolveParameter):
        """
        Test `_resolveParameters` when some parameters are not dictionaries and should be ignored.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {
            'param1': 'value1',  # Non-dict, should be ignored
            'param2': {'resolved': None, 'base': 'base_value2'}
        }
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_resolveParameter.return_value = 'resolved_value2'

        # Act
        node_execution._resolveParameters(openeojob, toServer, fromServer)

        # Assert
        mock_resolveParameter.assert_called_once_with(openeojob, toServer, fromServer, 'param2', 'base_value2')
        self.assertEqual(processNode.localArguments['param2']['resolved'], 'resolved_value2')

    @patch('workflow.nodeexecution.NodeExecution._resolveComplexParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveListParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_parameter_with_dict(self, mock_resolveNode, mock_resolveListParameter, mock_resolveComplexParameter):
        """
        Test `_resolveParameter` when the parameter definition is a dictionary.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = {'key': 'value'}

        mock_resolveComplexParameter.return_value = 'resolved_complex_value'

        # Act
        result = node_execution._resolveParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        mock_resolveComplexParameter.assert_called_once_with(openeojob, toServer, fromServer, key, parmDefinition)
        mock_resolveListParameter.assert_not_called()
        mock_resolveNode.assert_not_called()
        self.assertEqual(result, 'resolved_complex_value')

    @patch('workflow.nodeexecution.NodeExecution._resolveComplexParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveListParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_parameter_with_list(self, mock_resolveNode, mock_resolveListParameter, mock_resolveComplexParameter):
        """
        Test `_resolveParameter` when the parameter definition is a list.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = ['value1', 'value2']

        mock_resolveListParameter.return_value = ['resolved_value1', 'resolved_value2']

        # Act
        result = node_execution._resolveParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        mock_resolveComplexParameter.assert_not_called()
        mock_resolveListParameter.assert_called_once_with(openeojob, toServer, fromServer, key, parmDefinition)
        mock_resolveNode.assert_not_called()
        self.assertEqual(result, ['resolved_value1', 'resolved_value2'])

    @patch('workflow.nodeexecution.NodeExecution._resolveComplexParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveListParameter')
    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_parameter_with_other(self, mock_resolveNode, mock_resolveListParameter, mock_resolveComplexParameter):
        """
        Test `_resolveParameter` when the parameter definition is neither a dictionary nor a list.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = 'direct_value'

        mock_resolveNode.return_value = 'resolved_direct_value'

        # Act
        result = node_execution._resolveParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        mock_resolveComplexParameter.assert_not_called()
        mock_resolveListParameter.assert_not_called()
        mock_resolveNode.assert_called_once_with(openeojob, toServer, fromServer, (key, parmDefinition))
        self.assertEqual(result, 'resolved_direct_value')

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_complex_parameter_with_indirect_key(self, mock_resolveNode):
        """
        Test `_resolveComplexParameter` when the parameter definition contains an indirect key.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        itemkey = 'param1'
        parmDefinition = {'from_node': 'node1'}

        mock_resolveNode.return_value = 'resolved_from_node'

        # Act
        result = node_execution._resolveComplexParameter(openeojob, toServer, fromServer, itemkey, parmDefinition)

        # Assert
        mock_resolveNode.assert_called_once_with(openeojob, toServer, fromServer, ('from_node', 'node1'))
        self.assertEqual(result, 'resolved_from_node')

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_complex_parameter_with_direct_value(self, mock_resolveNode):
        """
        Test `_resolveComplexParameter` when the parameter definition contains a direct value.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        itemkey = 'param1'
        parmDefinition = {'key': 'value'}

        mock_resolveNode.return_value = 'resolved_direct_value'

        # Act
        result = node_execution._resolveComplexParameter(openeojob, toServer, fromServer, itemkey, parmDefinition)

        # Assert
        mock_resolveNode.assert_called_once_with(openeojob, toServer, fromServer, (itemkey, parmDefinition))
        self.assertEqual(result, 'resolved_direct_value')

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_complex_parameter_with_multiple_items(self, mock_resolveNode):
        """
        Test `_resolveComplexParameter` when the parameter definition contains multiple items.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        itemkey = 'param1'
        parmDefinition = {'from_node': 'node1', 'key': 'value'}

        mock_resolveNode.side_effect = ['resolved_from_node', 'resolved_direct_value']

        # Act
        result = node_execution._resolveComplexParameter(openeojob, toServer, fromServer, itemkey, parmDefinition)

        # Assert
        mock_resolveNode.assert_called_once_with(openeojob, toServer, fromServer, ('from_node', 'node1'))
        self.assertEqual(result, 'resolved_from_node')

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_list_parameter_with_dict_elements(self, mock_resolveNode):
        """
        Test `_resolveListParameter` when the parameter definition contains dictionary elements.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = [{'from_node': 'node1'}, {'key': 'value'}]

        mock_resolveNode.side_effect = ['resolved_from_node', 'resolved_direct_value']

        # Act
        result = node_execution._resolveListParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        self.assertEqual(mock_resolveNode.call_count, 2)
        mock_resolveNode.assert_any_call(openeojob, toServer, fromServer, ('from_node', 'node1'))
        mock_resolveNode.assert_any_call(openeojob, toServer, fromServer, (key, parmDefinition))
        self.assertEqual(result, ['resolved_from_node', 'resolved_direct_value'])

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_list_parameter_with_direct_values(self, mock_resolveNode):
        """
        Test `_resolveListParameter` when the parameter definition contains direct values.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = ['value1', 'value2']

        mock_resolveNode.side_effect = ['resolved_value1', 'resolved_value2']

        # Act
        result = node_execution._resolveListParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        self.assertEqual(mock_resolveNode.call_count, 2)
        mock_resolveNode.assert_any_call(openeojob, toServer, fromServer, (key, parmDefinition))
        self.assertEqual(result, 'resolved_value2')  # Last resolved value is returned

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_list_parameter_with_mixed_elements(self, mock_resolveNode):
        """
        Test `_resolveListParameter` when the parameter definition contains mixed elements (dict and direct values).
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = [{'from_node': 'node1'}, 'value2']

        mock_resolveNode.side_effect = ['resolved_from_node', 'resolved_value2']

        # Act
        result = node_execution._resolveListParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        self.assertEqual(mock_resolveNode.call_count, 2)
        mock_resolveNode.assert_any_call(openeojob, toServer, fromServer, ('from_node', 'node1'))
        mock_resolveNode.assert_any_call(openeojob, toServer, fromServer, (key, parmDefinition))
        self.assertEqual(result, 'resolved_value2')  # Last resolved value is returned

    @patch('workflow.nodeexecution.NodeExecution._resolveNode')
    def test_resolve_list_parameter_with_empty_list(self, mock_resolveNode):
        """
        Test `_resolveListParameter` when the parameter definition is an empty list.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        key = 'param1'
        parmDefinition = []

        # Act
        result = node_execution._resolveListParameter(openeojob, toServer, fromServer, key, parmDefinition)

        # Assert
        mock_resolveNode.assert_not_called()
        self.assertEqual(result, [])

    @patch('workflow.nodeexecution.NodeExecution._prepareAndRunNode')
    @patch('workflow.nodeexecution.NodeExecution.handleError')
    @patch('copy.deepcopy')
    def test_execute_node_success(self, mock_deepcopy, mock_handleError, mock_prepareAndRunNode):
        """
        Test `_executeNode` when the process object is found and executed successfully.
        """
        # Arrange
        processNode = MagicMock()
        processNode.process_id = 'test_process'
        processGraph = MagicMock()
        processObj = MagicMock()
        processGraph.getOperation.return_value = processObj
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        mock_deepcopy.return_value = processObj

        # Act
        node_execution._executeNode(openeojob, toServer, fromServer)

        # Assert
        processGraph.getOperation.assert_called_once_with('test_process')
        mock_deepcopy.assert_called_once_with(processObj)
        mock_prepareAndRunNode.assert_called_once_with(processObj, openeojob, toServer, fromServer)
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution._prepareAndRunNode')
    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_execute_node_process_not_found(self, mock_handleError, mock_prepareAndRunNode):
        """
        Test `_executeNode` when the process object is not found.
        """
        # Arrange
        processNode = MagicMock()
        processNode.process_id = 'unknown_process'
        processGraph = MagicMock()
        processGraph.getOperation.return_value = None
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act
        node_execution._executeNode(openeojob, toServer, fromServer)

        # Assert
        processGraph.getOperation.assert_called_once_with('unknown_process')
        mock_prepareAndRunNode.assert_not_called()
        mock_handleError.assert_called_once_with(
            openeojob,
            "Unknown operation unknown_process. This operation is not implemented on the server.",
            'operation'
        )

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_prepare_and_run_node_success(self, mock_handleError):
        """
        Test `_prepareAndRunNode` when the process object is prepared and runnable.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {'param1': 'value1'}
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        executeObj = MagicMock()
        executeObj.runnable = True
        executeObj.run.return_value = 'output_info'

        openeojob = MagicMock()
        openeojob.job_id = 'test_job_id'
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act
        node_execution._prepareAndRunNode(executeObj, openeojob, toServer, fromServer)

        # Assert
        executeObj.prepare.assert_called_once_with({
            'param1': 'value1',
            'serverChannel': toServer,
            'job_id': 'test_job_id'
        })
        executeObj.run.assert_called_once_with(openeojob, toServer, fromServer)
        self.assertEqual(node_execution.outputInfo, 'output_info')
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_prepare_and_run_node_not_runnable(self, mock_handleError):
        """
        Test `_prepareAndRunNode` when the process object is not runnable after preparation.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {'param1': 'value1'}
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        executeObj = MagicMock()
        executeObj.runnable = False

        openeojob = MagicMock()
        openeojob.job_id = 'test_job_id'
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act
        node_execution._prepareAndRunNode(executeObj, openeojob, toServer, fromServer)

        # Assert
        executeObj.prepare.assert_called_once_with({
            'param1': 'value1',
            'serverChannel': toServer,
            'job_id': 'test_job_id'
        })
        executeObj.run.assert_not_called()
        self.assertIsNone(node_execution.outputInfo)
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_prepare_and_run_node_prepare_failure(self, mock_handleError):
        """
        Test `_prepareAndRunNode` when the preparation of the process object raises an exception.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {'param1': 'value1'}
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        executeObj = MagicMock()
        executeObj.prepare.side_effect = Exception("Preparation failed")

        openeojob = MagicMock()
        openeojob.job_id = 'test_job_id'
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act & Assert
        with self.assertRaises(Exception) as context:
            node_execution._prepareAndRunNode(executeObj, openeojob, toServer, fromServer)

        self.assertEqual(str(context.exception), "Preparation failed")
        executeObj.prepare.assert_called_once_with({
            'param1': 'value1',
            'serverChannel': toServer,
            'job_id': 'test_job_id'
        })
        executeObj.run.assert_not_called()
        self.assertIsNone(node_execution.outputInfo)
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_prepare_and_run_node_run_failure(self, mock_handleError):
        """
        Test `_prepareAndRunNode` when the execution of the process object raises an exception.
        """
        # Arrange
        processNode = MagicMock()
        processNode.localArguments = {'param1': 'value1'}
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        executeObj = MagicMock()
        executeObj.runnable = True
        executeObj.run.side_effect = Exception("Execution failed")

        openeojob = MagicMock()
        openeojob.job_id = 'test_job_id'
        toServer = MagicMock()
        fromServer = MagicMock()

        # Act & Assert
        with self.assertRaises(Exception) as context:
            node_execution._prepareAndRunNode(executeObj, openeojob, toServer, fromServer)

        self.assertEqual(str(context.exception), "Execution failed")
        executeObj.prepare.assert_called_once_with({
            'param1': 'value1',
            'serverChannel': toServer,
            'job_id': 'test_job_id'
        })
        executeObj.run.assert_called_once_with(openeojob, toServer, fromServer)
        self.assertIsNone(node_execution.outputInfo)
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution._resolveFromNode')
    @patch('workflow.nodeexecution.NodeExecution._resolveFromParameter')
    def test_resolve_node_from_node(self, mock_resolveFromParameter, mock_resolveFromNode):
        """
        Test `_resolveNode` when the parameter key contains 'from_node'.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        parmKeyValue = ('from_node', 'node1')

        mock_resolveFromNode.return_value = 'resolved_from_node'

        # Act
        result = node_execution._resolveNode(openeojob, toServer, fromServer, parmKeyValue)

        # Assert
        mock_resolveFromNode.assert_called_once_with(openeojob, toServer, fromServer, 'node1')
        mock_resolveFromParameter.assert_not_called()
        self.assertEqual(result, 'resolved_from_node')

    @patch('workflow.nodeexecution.NodeExecution._resolveFromNode')
    @patch('workflow.nodeexecution.NodeExecution._resolveFromParameter')
    def test_resolve_node_from_parameter(self, mock_resolveFromParameter, mock_resolveFromNode):
        """
        Test `_resolveNode` when the parameter key contains 'from_parameter'.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        parmKeyValue = ('from_parameter', 'param1')

        mock_resolveFromParameter.return_value = 'resolved_from_parameter'

        # Act
        result = node_execution._resolveNode(openeojob, toServer, fromServer, parmKeyValue)

        # Assert
        mock_resolveFromParameter.assert_called_once_with('param1')
        mock_resolveFromNode.assert_not_called()
        self.assertEqual(result, 'resolved_from_parameter')

    @patch('workflow.nodeexecution.NodeExecution._resolveFromNode')
    @patch('workflow.nodeexecution.NodeExecution._resolveFromParameter')
    def test_resolve_node_direct_value(self, mock_resolveFromParameter, mock_resolveFromNode):
        """
        Test `_resolveNode` when the parameter key does not contain 'from_node' or 'from_parameter'.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        parmKeyValue = ('param1', 'direct_value')

        # Act
        result = node_execution._resolveNode(openeojob, toServer, fromServer, parmKeyValue)

        # Assert
        mock_resolveFromNode.assert_not_called()
        mock_resolveFromParameter.assert_not_called()
        self.assertEqual(result, 'direct_value')

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    @patch('workflow.nodeexecution.NodeExecution.run')
    def test_resolve_from_node_single_node_resolved(self, mock_run, mock_handleError):
        """
        Test `_resolveFromNode` when a single referred node is resolved successfully.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        referredNode = MagicMock()
        referredNode.nodeValue = {'value': 'resolved_value'}
        processGraph.id2node.return_value = (None, referredNode)
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        referredNodeName = 'node1'

        # Act
        result = node_execution._resolveFromNode(openeojob, toServer, fromServer, referredNodeName)

        # Assert
        processGraph.id2node.assert_called_once_with('node1')
        mock_run.assert_not_called()
        mock_handleError.assert_not_called()
        self.assertEqual(result, 'resolved_value')

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    @patch('workflow.nodeexecution.NodeExecution.run')
    def test_resolve_from_node_single_node_unresolved(self, mock_run, mock_handleError):
        """
        Test `_resolveFromNode` when a single referred node is unresolved and needs execution.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        referredNode = MagicMock()
        referredNode.nodeValue = None
        processGraph.id2node.return_value = None
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        referredNodeName = 'node1'

        mock_run.return_value = None
        refExecutionNode = MagicMock()
        refExecutionNode.outputInfo = {'value': 'resolved_value'}
        with patch('workflow.nodeexecution.NodeExecution', return_value=refExecutionNode):
            # Act
            result = node_execution._resolveFromNode(openeojob, toServer, fromServer, referredNodeName)

        # Assert
        processGraph.id2node.assert_called_once_with('node1')
        mock_run.assert_not_called()
        self.assertEqual(result, {})

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_resolve_from_node_node_not_found(self, mock_handleError):
        """
        Test `_resolveFromNode` when the referred node is not found in the process graph.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        processGraph.id2node.return_value = None
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        referredNodeName = 'node1'

        # Act
        result = node_execution._resolveFromNode(openeojob, toServer, fromServer, referredNodeName)

        # Assert
        processGraph.id2node.assert_called_once_with('node1')
        mock_handleError.assert_called_once_with(openeojob, "Node cannot be found", 'resolved node')
        self.assertEqual(result,{})

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    @patch('workflow.nodeexecution.NodeExecution.run')
    def test_resolve_from_node_multiple_nodes(self, mock_run, mock_handleError):
        """
        Test `_resolveFromNode` when multiple referred nodes are resolved.
        """
        # Arrange
        processNode = MagicMock()
        processGraph = MagicMock()
        referredNode1 = MagicMock()
        referredNode1.nodeValue = {'value': 'resolved_value1'}
        referredNode2 = MagicMock()
        referredNode2.nodeValue = {'value': 'resolved_value2'}
        processGraph.id2node.side_effect = [
            (None, referredNode1),
            (None, referredNode2)
        ]
        node_execution = NodeExecution(processNode, processGraph)

        openeojob = MagicMock()
        toServer = MagicMock()
        fromServer = MagicMock()
        referredNodeName = ['node1', 'node2']

        # Act
        result = node_execution._resolveFromNode(openeojob, toServer, fromServer, referredNodeName)

        # Assert
        processGraph.id2node.assert_any_call('node1')
        processGraph.id2node.assert_any_call('node2')
        mock_run.assert_not_called()
        mock_handleError.assert_not_called()
        self.assertEqual(result, {'node1': 'resolved_value1', 'node2': 'resolved_value2'})

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_resolve_from_parameter_resolved(self, mock_handleError):
        """
        Test `_resolveFromParameter` when the parameter is resolved.
        """
        # Arrange
        processNode = MagicMock()
        parentProcessGraph = MagicMock()
        processNode.parentProcessGraph = parentProcessGraph
        parentProcessGraph.resolveParameter.return_value = {'resolved': 'resolved_value'}
        node_execution = NodeExecution(processNode, MagicMock())

        # Act
        result = node_execution._resolveFromParameter('param1')

        # Assert
        parentProcessGraph.resolveParameter.assert_called_once_with('param1')
        self.assertEqual(result, 'resolved_value')
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_resolve_from_parameter_unresolved(self, mock_handleError):
        """
        Test `_resolveFromParameter` when the parameter is unresolved.
        """
        # Arrange
        processNode = MagicMock()
        parentProcessGraph = MagicMock()
        processNode.parentProcessGraph = parentProcessGraph
        parentProcessGraph.resolveParameter.return_value = {'resolved': None}
        node_execution = NodeExecution(processNode, MagicMock())

        # Act
        result = node_execution._resolveFromParameter('param1')

        # Assert
        parentProcessGraph.resolveParameter.assert_called_once_with('param1')
        self.assertIsNone(result)
        mock_handleError.assert_not_called()

    @patch('workflow.nodeexecution.NodeExecution.handleError')
    def test_resolve_from_parameter_no_ref_node(self, mock_handleError):
        """
        Test `_resolveFromParameter` when the parameter is not found in the parent process graph.
        """
        # Arrange
        processNode = MagicMock()
        parentProcessGraph = MagicMock()
        processNode.parentProcessGraph = parentProcessGraph
        parentProcessGraph.resolveParameter.return_value = None
        node_execution = NodeExecution(processNode, MagicMock())

        # Act
        result = node_execution._resolveFromParameter('param1')

        # Assert
        parentProcessGraph.resolveParameter.assert_called_once_with('param1')
        self.assertIsNone(result)
        mock_handleError.assert_not_called()

if __name__ == '__main__':
    unittest.main()