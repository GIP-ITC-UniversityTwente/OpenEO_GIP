import os
import json
import unittest
from unittest.mock import patch, mock_open, MagicMock
from pathlib import Path
from multiprocessing import Lock
import sys

pp = '/home/mschouwen/projects/openeo/openeo/'

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')
sys.path.append(pp)

from operations.ilwispy.load_collection import getRasterDataSets
from operations.ilwispy.load_collection import LoadCollectionOperation
from unittest.mock import patch, MagicMock
import unittest
import os
import logging

import addTestRasters

testRasters = addTestRasters.setTestRasters(5)
  

class TestGetRasterDataSets(unittest.TestCase):

    @patch("operations.ilwispy.load_collection.Path.home")
    @patch("operations.ilwispy.load_collection.openeoip_config")
    @patch("operations.ilwispy.load_collection.os.path.exists")
    @patch("operations.ilwispy.load_collection.open", new_callable=mock_open, read_data='{"id1": {"data": "value1"}}')
    @patch("operations.ilwispy.load_collection.tr.setTestRasters")
    def test_get_raster_data_sets_with_synthetic_data(self, mock_set_test_rasters, mock_open_file, mock_path_exists, mock_openeoip_config, mock_home):
        # Mocking the home directory and configuration
        mock_home.return_value = Path("/mock/home")
        mock_openeoip_config.__getitem__.return_value = {'system_files': {'location': 'mock_system_files'}}
        mock_path_exists.side_effect = lambda path: path.endswith("id2filename.table") or path.endswith("mock_system_files")

        # Mocking synthetic raster data
        mock_set_test_rasters.return_value = [{"id": "synthetic1", "data": "synthetic_value1"}]

        # Call the function
        result = getRasterDataSets(includeSynteticData=True)

        # Assertions
        self.assertIn("id1", result)
        self.assertEqual(result["id1"]["data"], "value1")
        self.assertIn("synthetic1", result)
        self.assertEqual(result["synthetic1"]["data"], "synthetic_value1")

    @patch("operations.ilwispy.load_collection.Path.home")
    @patch("operations.ilwispy.load_collection.openeoip_config")
    @patch("operations.ilwispy.load_collection.os.path.exists")
    @patch("operations.ilwispy.load_collection.open", new_callable=mock_open, read_data='{"id1": {"data": "value1"}}')
    def test_get_raster_data_sets_without_synthetic_data(self, mock_open_file, mock_path_exists, mock_openeoip_config, mock_home):
        # Mocking the home directory and configuration
        mock_home.return_value = Path("/mock/home")
        mock_openeoip_config.__getitem__.return_value = {'system_files': {'location': 'mock_system_files'}}
        mock_path_exists.side_effect = lambda path: path.endswith("id2filename.table") or path.endswith("mock_system_files")

        # Call the function
        result = getRasterDataSets(includeSynteticData=False)

        # Assertions
        self.assertIn("id1", result)
        self.assertEqual(result["id1"]["data"], "value1")
        self.assertNotIn("synthetic1", result)

    @patch("operations.ilwispy.load_collection.Path.home")
    @patch("operations.ilwispy.load_collection.openeoip_config")
    @patch("operations.ilwispy.load_collection.os.path.exists")
    def test_get_raster_data_sets_no_properties_file(self, mock_path_exists, mock_openeoip_config, mock_home):
        # Mocking the home directory and configuration
        mock_home.return_value = Path("/mock/home")
        mock_openeoip_config.__getitem__.return_value = {'system_files': {'location': 'mock_system_files'}}
        mock_path_exists.return_value = False

        # Call the function
        result = getRasterDataSets(includeSynteticData=False)

        # Assertions
        self.assertEqual(result, {})

class TestUnpackOriginalData(unittest.TestCase):

    @patch("operations.ilwispy.load_collection.Reader")
    @patch("operations.ilwispy.load_collection.openeologging.logMessage")
    @patch("operations.ilwispy.load_collection.os.path.join")
    @patch("operations.ilwispy.load_collection.os.path.isdir")
    @patch("operations.ilwispy.load_collection.os.scandir")
    @patch("operations.ilwispy.load_collection.shutil.move")
    @patch("operations.ilwispy.load_collection.shutil.copy")
    def test_unpack_original_data(self, mock_shutil_copy, mock_shutil_move, mock_scandir, mock_isdir, mock_path_join, mock_log_message, mock_reader):
        # Mocking dependencies
        mock_reader_instance = MagicMock()
        mock_reader.return_value = mock_reader_instance
        mock_reader_instance.open.return_value = MagicMock(output="mock_folder", condensed_name="mock_name")
        mock_isdir.return_value = True
        mock_scandir.return_value.__enter__.return_value = []
        mock_path_join.side_effect = lambda *args: "/".join(args)

        # Mocking the class and its attributes
        operation = LoadCollectionOperation()
        operation.name = "test_operation"
        operation.inputRaster = {"id": "mock_id"}
        operation._cleanTemporaryFolder = MagicMock()
        operation._processBands = MagicMock(return_value={"band1": "file1.tif"})
        operation._moveUnpackedData = MagicMock()

        # Call the method
        source_list, unpack_folder_name = operation.unpackOriginalData("mock_data", "mock_folder")

        # Assertions
        self.assertEqual(source_list, {"band1": "file1.tif"})
        self.assertEqual(unpack_folder_name, "unpacked_mock_id")
        operation._cleanTemporaryFolder.assert_called_once_with("mock_folder/tmp_mock_name")
        operation._processBands.assert_called_once()
        mock_log_message.assert_any_call(logging.INFO, "test_operation unpacking original data using eoreader")
        mock_log_message.assert_any_call(logging.INFO, "test_operation done unpacking original data")

class TestCleanTemporaryFolder(unittest.TestCase):

    @patch("operations.ilwispy.load_collection.os.path.isdir")
    @patch("operations.ilwispy.load_collection.os.scandir")
    @patch("operations.ilwispy.load_collection.os.unlink")
    def test_clean_temporary_folder_with_files(self, mock_unlink, mock_scandir, mock_isdir):
        # Mocking os.path.isdir to return True
        mock_isdir.return_value = True

        # Mocking os.scandir to return a list of mock entries
        mock_entry_file = MagicMock()
        mock_entry_file.is_file.return_value = True
        mock_entry_file.path = "/mock/tmp/file1"
        mock_scandir.return_value.__enter__.return_value = [mock_entry_file]

        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Call the method
        operation._cleanTemporaryFolder("/mock/tmp")

        # Assertions
        mock_isdir.assert_called_once_with("/mock/tmp")
        mock_scandir.assert_called_once_with("/mock/tmp")
        mock_unlink.assert_called_once_with("/mock/tmp/file1")

    @patch("operations.ilwispy.load_collection.os.path.isdir")
    @patch("operations.ilwispy.load_collection.os.scandir")
    @patch("operations.ilwispy.load_collection.os.unlink")
    def test_clean_temporary_folder_without_files(self, mock_unlink, mock_scandir, mock_isdir):
        # Mocking os.path.isdir to return True
        mock_isdir.return_value = True

        # Mocking os.scandir to return an empty list
        mock_scandir.return_value.__enter__.return_value = []

        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Call the method
        operation._cleanTemporaryFolder("/mock/tmp")

        # Assertions
        mock_isdir.assert_called_once_with("/mock/tmp")
        mock_scandir.assert_called_once_with("/mock/tmp")
        mock_unlink.assert_not_called()

    @patch("operations.ilwispy.load_collection.os.path.isdir")
    @patch("operations.ilwispy.load_collection.os.scandir")
    @patch("operations.ilwispy.load_collection.os.unlink")
    def test_clean_temporary_folder_not_a_directory(self, mock_unlink, mock_scandir, mock_isdir):
        # Mocking os.path.isdir to return False
        mock_isdir.return_value = False

        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Call the method
        operation._cleanTemporaryFolder("/mock/tmp")

        # Assertions
        mock_isdir.assert_called_once_with("/mock/tmp")
        mock_scandir.assert_not_called()
        mock_unlink.assert_not_called()

class TestProcessBands(unittest.TestCase):

    @patch("operations.ilwispy.load_collection.to_band")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._getNewFiles")
    @patch("operations.ilwispy.load_collection.Path.glob")
    def test_process_bands_with_valid_bands(self, mock_glob, mock_get_new_files, mock_to_band):
        # Mocking the product and its attributes
        mock_product = MagicMock()
        mock_obj = type('BandPlaceHolder', (object,), {'name': 'file1.tif'})
        mock_product.output.glob.side_effect = lambda pattern: [mock_obj]

        # Mocking the input raster and its methods
        operation = LoadCollectionOperation()
        operation.inputRaster = MagicMock()
        operation.inputRaster.getBands.return_value = [
            {"commonbandname": "band1", "name": "Band1"},
            {"commonbandname": "band2", "name": "Band2"}
        ]

        # Mocking helper methods
        mock_to_band.side_effect = lambda band_name: f"enum_{band_name}"
        mock_get_new_files.side_effect = lambda old, new: new

        # Call the method
        result = operation._processBands(mock_product, "tmp_folder")

        # Assertions
        self.assertEqual(result, {"Band1": "file1.tif", "Band2": "file1.tif"})
        operation.inputRaster.getBands.assert_called_once()
        mock_to_band.assert_any_call("band1")
        mock_to_band.assert_any_call("band2")
        mock_get_new_files.assert_called()

    @patch("operations.ilwispy.load_collection.to_band")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._getNewFiles")
    @patch("operations.ilwispy.load_collection.Path.glob")
    def test_process_bands_with_no_new_files(self, mock_glob, mock_get_new_files, mock_to_band):
        # Mocking the product and its attributes
        mock_product = MagicMock()
        mock_product.output.glob.side_effect = lambda pattern: []

        # Mocking the input raster and its methods
        operation = LoadCollectionOperation()
        operation.inputRaster = MagicMock()
        operation.inputRaster.getBands.return_value = [
            {"commonbandname": "band1", "name": "Band1"}
        ]

        # Mocking helper methods
        mock_to_band.side_effect = lambda band_name: f"enum_{band_name}"
        mock_get_new_files.side_effect = lambda old, new: []

        # Call the method
        result = operation._processBands(mock_product, "tmp_folder")

        # Assertions
        self.assertEqual(result, {})
        operation.inputRaster.getBands.assert_called_once()
        mock_to_band.assert_called_once_with("band1")
        mock_get_new_files.assert_called_once()

    @patch("operations.ilwispy.load_collection.to_band")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._getNewFiles")
    @patch("operations.ilwispy.load_collection.Path.glob")
    def test_process_bands_with_multiple_new_files(self, mock_glob, mock_get_new_files, mock_to_band):
        # Mocking the product and its attributes
        mock_product = MagicMock()
        mock_obj1 = type('BandPlaceHolder', (object,), {'name': 'file1.tif'})
        mock_obj2 = type('BandPlaceHolder', (object,), {'name': 'file2.tif'})        
        mock_product.output.glob.side_effect = lambda pattern: [mock_obj1, mock_obj2]

        # Mocking the input raster and its methods
        operation = LoadCollectionOperation()
        operation.inputRaster = MagicMock()
        operation.inputRaster.getBands.return_value = [
            {"commonbandname": "band1", "name": "Band1"}
        ]

        # Mocking helper methods
        mock_to_band.side_effect = lambda band_name: f"enum_{band_name}"
        mock_get_new_files.side_effect = lambda old, new: new

        # Call the method
        result = operation._processBands(mock_product, "tmp_folder")

        # Assertions
        self.assertEqual(result, {})
        operation.inputRaster.getBands.assert_called_once()
        mock_to_band.assert_called_once_with("band1")
        mock_get_new_files.assert_called_once()

class TestGetNewFiles(unittest.TestCase):

    def test_get_new_files_with_new_files(self):
        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Define old and new outputs
        old_outputs = ["file1.tif", "file2.tif"]
        new_outputs = ["file1.tif", "file2.tif", "file3.tif", "file4.tif"]

        # Call the method
        result = operation._getNewFiles(old_outputs, new_outputs)

        # Assertions
        self.assertEqual(result, ["file3.tif", "file4.tif"])

    def test_get_new_files_with_no_new_files(self):
        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Define old and new outputs
        old_outputs = ["file1.tif", "file2.tif"]
        new_outputs = ["file1.tif", "file2.tif"]

        # Call the method
        result = operation._getNewFiles(old_outputs, new_outputs)

        # Assertions
        self.assertEqual(result, [])

    def test_get_new_files_with_empty_old_outputs(self):
        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Define old and new outputs
        old_outputs = []
        new_outputs = ["file1.tif", "file2.tif"]

        # Call the method
        result = operation._getNewFiles(old_outputs, new_outputs)

        # Assertions
        self.assertEqual(result, ['file1.tif', 'file2.tif'])

    def test_get_new_files_with_empty_new_outputs(self):
        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Define old and new outputs
        old_outputs = ["file1.tif", "file2.tif"]
        new_outputs = []

        # Call the method
        result = operation._getNewFiles(old_outputs, new_outputs)

        # Assertions
        self.assertEqual(result, [])

    def test_get_new_files_with_both_empty_outputs(self):
        # Create an instance of the operation
        operation = LoadCollectionOperation()

        # Define old and new outputs
        old_outputs = []
        new_outputs = []

        # Call the method
        result = operation._getNewFiles(old_outputs, new_outputs)

        # Assertions
        self.assertEqual(result, [])

class TestPrepare(unittest.TestCase):

    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._initializePreparation")
    @patch("operations.ilwispy.load_collection.getRasterDataSets")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._loadInputRaster")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._transformDataIfNeeded")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._processBandsArgument")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._processTemporalExtent")
    @patch("operations.ilwispy.load_collection.setWorkingCatalog")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._processSpatialExtent")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._processProperties")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation.logEndPrepareOperation")
    def test_prepare_success(
        self, mock_log_end, mock_process_properties, mock_process_spatial_extent,
        mock_set_working_catalog, mock_process_temporal_extent, mock_process_bands_argument,
        mock_transform_data, mock_load_input_raster, mock_get_raster_datasets, mock_initialize_preparation
    ):
        # Mocking dependencies
        mock_initialize_preparation.return_value = ("mock_server", "mock_job_id")
        mock_get_raster_datasets.return_value = {"mock_id": {"data": "mock_data"}}
        mock_load_input_raster.return_value = {"dataFolder": "mock_folder"}
        mock_transform_data.return_value = "mock_folder"
        mock_set_working_catalog.return_value = "mock_path"

        # Create an instance of the operation
        operation = LoadCollectionOperation()
        operation.name = "test_operation"

        # Call the method
        operation.prepare({"mock_argument": "value"})

        # Assertions
        mock_initialize_preparation.assert_called_once_with({"mock_argument": "value"})
        mock_get_raster_datasets.assert_called_once()
        mock_load_input_raster.assert_called_once_with(
            {"mock_id": {"data": "mock_data"}}, {"mock_argument": "value"}, "mock_server", "mock_job_id"
        )
        mock_transform_data.assert_called_once_with({"mock_id": {"data": "mock_data"}}, "mock_server", "mock_job_id")
        mock_process_bands_argument.assert_called_once_with({"mock_argument": "value"})
        mock_process_temporal_extent.assert_called_once_with({"mock_argument": "value"}, "mock_server", "mock_job_id")
        mock_set_working_catalog.assert_called_once_with({"dataFolder": "mock_folder"}, "test_operation")
        mock_process_spatial_extent.assert_called_once_with({"mock_argument": "value"}, "mock_server", "mock_job_id", "mock_path")
        mock_process_properties.assert_called_once_with({"mock_argument": "value"})
        mock_log_end.assert_called_once_with("mock_job_id")
        self.assertTrue(operation.runnable)
        self.assertTrue(operation.rasterSizesEqual)

    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._initializePreparation")
    @patch("operations.ilwispy.load_collection.getRasterDataSets")
    @patch("operations.ilwispy.load_collection.LoadCollectionOperation._loadInputRaster")
    def test_prepare_failure_on_load_input_raster(
        self, mock_load_input_raster, mock_get_raster_datasets, mock_initialize_preparation
    ):
        # Mocking dependencies
        mock_initialize_preparation.return_value = ("mock_server", "mock_job_id")
        mock_get_raster_datasets.return_value = {"mock_id": {"data": "mock_data"}}
        mock_load_input_raster.side_effect = Exception("Failed to load input raster")

        # Create an instance of the operation
        operation = LoadCollectionOperation()
        operation.name = "test_operation"

        # Call the method and assert exception
        with self.assertRaises(Exception) as context:
            operation.prepare({"mock_argument": "value"})

        self.assertEqual(str(context.exception), "Failed to load input raster")
        mock_initialize_preparation.assert_called_once_with({"mock_argument": "value"})
        mock_get_raster_datasets.assert_called_once()
        mock_load_input_raster.assert_called_once_with(
            {"mock_id": {"data": "mock_data"}}, {"mock_argument": "value"}, "mock_server", "mock_job_id"
        )
        self.assertFalse(operation.runnable)








if __name__ == "__main__":
    unittest.main()