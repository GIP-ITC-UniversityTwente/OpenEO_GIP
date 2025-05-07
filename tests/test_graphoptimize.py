import unittest
from workflow.graphoptimize import argValue
from workflow.graphoptimize import analyzeProcessGraph
from workflow.graphoptimize import analyzeGraph

class TestArgValue(unittest.TestCase):
    def test_arg_is_dict_with_from_node(self):
        arg = {'from_node': 'node1'}
        result = argValue(arg)
        self.assertEqual(result, 'node1')

    def test_arg_is_dict_without_from_node(self):
        arg = {'other_key': 'value'}
        result = argValue(arg)
        self.assertIsNone(result)

    def test_arg_is_not_dict(self):
        arg = 'some_value'
        result = argValue(arg)
        self.assertEqual(result, 'some_value')

    def test_arg_is_empty_dict(self):
        arg = {}
        with self.assertRaises(StopIteration):
            argValue(arg)

class TestAnalyzeProcessGraph(unittest.TestCase):
    def test_single_node_with_no_arguments(self):
        node = {'process_id': 'add', 'arguments': {}}
        processGraph = {'node1': node}
        expr, usedNodes = analyzeProcessGraph(node, processGraph, 'node1')
        self.assertEqual(expr, '')
        self.assertEqual(usedNodes, ['node1'])

    def test_single_node_with_arguments(self):
        node = {'process_id': 'add', 'arguments': {'x': 1, 'y': 2}}
        processGraph = {'node1': node}
        expr, usedNodes = analyzeProcessGraph(node, processGraph, 'node1')
        self.assertEqual(expr, '(1+2)')
        self.assertEqual(usedNodes, ['node1'])

    def test_nested_nodes(self):
        node2 = {'process_id': 'multiply', 'arguments': {'x': 3, 'y': 4}}
        node1 = {'process_id': 'add', 'arguments': {'x': {'from_node': 'node2'}, 'y': 5}}
        processGraph = {'node1': node1, 'node2': node2}
        expr, usedNodes = analyzeProcessGraph(node1, processGraph, 'node1')
        self.assertEqual(expr, '((3*4)+5)')
        self.assertEqual(usedNodes, ['node1', 'node2'])

    def test_node_with_unknown_process_id(self):
        node = {'process_id': 'unknown', 'arguments': {}}
        processGraph = {'node1': node}
        expr, usedNodes = analyzeProcessGraph(node, processGraph, 'node1')
        self.assertEqual(expr, '@')
        self.assertEqual(usedNodes, [])

    def test_node_with_empty_arguments(self):
        node = {'process_id': 'add', 'arguments': {}}
        processGraph = {'node1': node}
        expr, usedNodes = analyzeProcessGraph(node, processGraph, 'node1')
        self.assertEqual(expr, '')
        self.assertEqual(usedNodes, ['node1'])

    def test_node_with_invalid_from_node_reference(self):
        node = {'process_id': 'add', 'arguments': {'x': {'from_node': 'invalid_node'}}}
        processGraph = {'node1': node}
        with self.assertRaises(KeyError):
            analyzeProcessGraph(node, processGraph, 'node1')

class TestAnalyzeGraph(unittest.TestCase):
    def test_no_subgraph(self):
        sourceGraph = {'node1': {'process_id': 'add', 'arguments': {'x': 1, 'y': 2}}}
        subgraph = False
        result = analyzeGraph(sourceGraph, subgraph)
        self.assertEqual(result, sourceGraph)

    def test_single_node_graph(self):
        sourceGraph = {'node1': {'process_id': 'add', 'arguments': {'x': 1, 'y': 2}}}
        subgraph = True
        result = analyzeGraph(sourceGraph, subgraph)
        expected = {
            'rastercalc1': {
                'process_id': 'rastercalc',
                'arguments': {
                    'expression': '(1+2)',
                    'v': {'from_node': []}
                },
                'result': True
            }
        }
        self.assertEqual(result, expected)

    def test_nested_graph(self):
        sourceGraph = {
            'node1': {'process_id': 'multiply', 'arguments': {'x': 3, 'y': 4}},
            'node2': {'process_id': 'add', 'arguments': {'x': {'from_node': 'node1'}, 'y': 5}}
 
        }
        subgraph = True
        result = analyzeGraph(sourceGraph, subgraph)
        expected = {
            'rastercalc1': {
                'process_id': 'rastercalc',
                'arguments': {
                    'expression': '((3*4)+5)',
                    'v': {'from_node': []}
                },
                'result': True
            }
        }
        self.assertEqual(result, expected)

    def test_unused_nodes_in_graph(self):
        sourceGraph = {
            
            'node1': {'process_id': 'multiply', 'arguments': {'x': 3, 'y': 4}},
            'node2': {'process_id': 'subtract', 'arguments': {'x': 10, 'y': 2}},
            'node3': {'process_id': 'add', 'arguments': {'x': {'from_node': 'node1'}, 'y': 5}},            
        }
        subgraph = True
        result = analyzeGraph(sourceGraph, subgraph)
        expected = {
            'node2': {'process_id': 'subtract', 'arguments': {'x': 10, 'y': 2}},
            'rastercalc1': {
                'process_id': 'rastercalc',
                'arguments': {
                    'expression': '((3*4)+5)',
                    'v': {'from_node': []}
                },
                'result': True
            }
        }
        self.assertEqual(result, expected)

    def test_empty_source_graph(self):
        sourceGraph = {}
        subgraph = True
        result = analyzeGraph(sourceGraph, subgraph)
        self.assertEqual(result, sourceGraph)

    def test_invalid_from_node_reference(self):
        sourceGraph = {
            'node1': {'process_id': 'add', 'arguments': {'x': {'from_node': 'invalid_node'}, 'y': 5}}
        }
        subgraph = True
        with self.assertRaises(KeyError):
            analyzeGraph(sourceGraph, subgraph)

if __name__ == '__main__':
    unittest.main()