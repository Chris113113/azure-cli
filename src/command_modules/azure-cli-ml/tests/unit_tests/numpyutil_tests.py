import numpy as np
import json
from .schema_tests_common import SchemaUnitTests
from azure.cli.command_modules.ml.service.schema.common import Schema
from azure.cli.command_modules.ml.service.schema.numpyutil import NumpyUtil


class NumpyUtilTests(SchemaUnitTests):
    int_array = np.array(range(10), dtype=np.int32)
    _structured_dt = np.dtype([('name', np.str_, 16), ('grades', np.float64, (2,))])
    struct_array = np.array([('Sarah', (8.0, 7.0)), ('John', (6.0, 7.0))], dtype=_structured_dt)
    _nested_array_dt = np.dtype("i4, (2,2,3)f8, f4")
    multi_level_array = np.array([(1, (((4.6, 3.2, 1.5), (1.1, 1.2, 1.3)), ((1.6, 1.2, 1.5), (3.1, -1.2, 8.3))), 3.6)],
                                 dtype=_nested_array_dt)
    expected_swagger = None

    def test_single_array_schema_extract(self):
        # Simple types array
        self._run_schema_extract_test(self.int_array, 'int_array')
        # Structured data type array
        self._run_schema_extract_test(self.struct_array, 'struct_data_array')
        # Multi level array
        self._run_schema_extract_test(self.multi_level_array, 'multi_level_array')

    def test_schema_save_load(self):
        self._run_schema_save_load_test(self.struct_array, 'struct_array', 'struct_data_array')
        self._run_schema_save_load_test(self.int_array, 'ints', 'int_array')

    def test_input_parsing(self):
        input_json = '[{"name": "Steven", "grades": [3.3, 5.5]}, {"name": "Jenny", "grades": [8.8, 7.5]}, {"name": "Dan", "grades": [9.3, 6.7]}]'
        file_prefix = 'grades'
        try:
            # First generate the input schema
            NumpyUtil.save_data_schema(self.struct_array, file_prefix)

            # Then load it back to a schema object
            schema = NumpyUtil.load_data_schema(SchemaUnitTests._get_internal_schema_file_name(file_prefix))

            # Now parse the input json into a numpy array with the same schema
            parsed_input = NumpyUtil.parse_json_to_array(input_json, schema)

            # Validate the result
            self.assertIsNotNone(parsed_input, "Parsed input must have a value here")
            self.assertTrue(type(parsed_input) == np.ndarray, "Parsed input must be a numpy array.")
            self.assertEqual(3, len(parsed_input),
                             "Parsed array holds {0} elements instead of expected {1}".format(len(parsed_input), 3))
            self.assertEqual(parsed_input.dtype, self.struct_array.dtype,
                             "Parsed array dtype={0} is different than expected {1}".format(parsed_input.dtype,
                                                                                            self.struct_array.dtype))
        finally:
            SchemaUnitTests._delete_test_schema_files(file_prefix)

    def _run_schema_extract_test(self, array, swagger_key):
        expected_swagger = self.expected_swagger[swagger_key]
        schema = NumpyUtil.extract_schema(array)
        self._validate_single_array_schema(array, schema, expected_swagger)

    def _run_schema_save_load_test(self, array, file_prefix, expected_swagger_key):
        try:
            # Extract & persist array schemas to disk
            NumpyUtil.save_data_schema(array, file_prefix)
            self._validate_schema_files_existence(file_prefix)

            # Load & validate array schemas from disk
            internal_schema = NumpyUtil.load_data_schema(SchemaUnitTests._get_internal_schema_file_name(file_prefix))
            with open(SchemaUnitTests._get_swagger_schema_file_name(file_prefix), 'r') as f:
                swagger_schema = json.load(f)
            schema = Schema(internal_schema, swagger_schema)
            expected_swag = self.expected_swagger[expected_swagger_key]
            self._validate_single_array_schema(array, schema, expected_swag)
        finally:
            SchemaUnitTests._delete_test_schema_files(file_prefix)

    def _validate_single_array_schema(self, array, array_schema, expected_swagger):
        self._validate_extracted_schema_structure(array_schema)

        self.assertTrue('dtype' in array_schema.internal, "Internal schema must contain a 'dtype' property")
        extracted_dtype = array_schema.internal['dtype']
        self.assertEqual(extracted_dtype, array.dtype,
                         "Internal schema has different dtype than expected: {0}".format(extracted_dtype))
        self.assertTrue('shape' in array_schema.internal, "Internal schema must contain a 'shape' property")
        extracted_shape = array_schema.internal['shape']
        self.assertEqual(extracted_shape, array.shape,
                         "Internal schema has different shape than expected: {0}".format(extracted_shape))

        self.assertDictEqual(expected_swagger, array_schema.swagger)
