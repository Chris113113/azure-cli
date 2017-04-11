import pandas as pd
import json
from .schema_tests_common import SchemaUnitTests
from azure.cli.command_modules.ml.service.schema.common import Schema
from azure.cli.command_modules.ml.service.schema.pandasutil import PandasUtil


class PandasUtilTests(SchemaUnitTests):
    _birthsPerName = list(zip(['Bob', 'Jessica', 'Mary', 'John', 'Mel'], [968, 155, 77, 578, 973]))
    births_df = pd.DataFrame(data=_birthsPerName, columns=['Names', 'Births'])
    test_df = pd.DataFrame(data=[[1, 3.3, "bla"], [2, -4.6, "blah"]], columns=['Idx', 'aFloat', 'aString'])

    def test_schema_extract(self):
        expected_swagger = self.expected_swagger['pandas_births']
        schema = PandasUtil.extract_schema(self.births_df)
        self._validate_single_df_schema(self.births_df, schema, expected_swagger)

    def test_schema_save_load(self):
        self._run_schema_save_load_test(self.test_df, "./test_data", "test_dataframe")
        self._run_schema_save_load_test(self.births_df, "./births", "pandas_births")

    def test_input_parsing(self):
        input_json = '[{"Names": "Jim", "Births": 345}, {"Names": "Andrew", "Births": 121}, {"Names": "Molly", "Births": 563}]'
        file_prefix = './births'
        try:
            # First generate the input schema
            PandasUtil.save_data_schema(self.births_df, file_prefix)

            # Then load it back to a schema object
            schema = PandasUtil.load_data_schema(SchemaUnitTests._get_internal_schema_file_name(file_prefix))

            # Now parse the input json into a Pandas data frame with the same schema
            parsed_input = PandasUtil.parse_json_to_data_frame(input_json, schema)

            # Validate the result
            expected_shape = (3,2)
            expected_columns = self.births_df.columns
            expected_dtypes = self.births_df.dtypes
            self.assertIsNotNone(parsed_input, "Parsed input must have a value here")
            self.assertTrue(isinstance(parsed_input, pd.core.frame.DataFrame), "Parsed input must be a pandas data frame.")
            self.assertEqual(expected_shape, parsed_input.shape,
                             "Parsed data frame shape {0} different from expected {1}".format(
                                 parsed_input.shape, expected_shape))
            self.assertCountEqual(parsed_input.columns.values, expected_columns.values,
                                  "Parsed data frame columns={0} are different than expected {1}".format(
                                      parsed_input.columns, expected_columns))
            self.assertCountEqual(parsed_input.dtypes, expected_dtypes,
                                  "Parsed data frame dtypes={0} are different than expected {1}".format(
                                      parsed_input.dtypes, expected_dtypes))
        finally:
            SchemaUnitTests._delete_test_schema_files(file_prefix)

    def _run_schema_save_load_test(self, df, file_prefix, expected_swagger_key):
        try:
            # Extract & persist simple service input/output schemas to disk
            PandasUtil.save_data_schema(df, file_prefix)
            self._validate_schema_files_existence(file_prefix)

            # Load & validate simple service schemas from disk
            internal_schema = PandasUtil.load_data_schema(SchemaUnitTests._get_internal_schema_file_name(file_prefix))
            with open(SchemaUnitTests._get_swagger_schema_file_name(file_prefix), 'r') as f:
                swagger_schema = json.load(f)
            schema = Schema(internal_schema, swagger_schema)
            expected_swag = self.expected_swagger[expected_swagger_key]
            self._validate_single_df_schema(df, schema, expected_swag)
        finally:
            SchemaUnitTests._delete_test_schema_files(file_prefix)

    def _validate_single_df_schema(self, df, schema, expected_swagger):
        self._validate_extracted_schema_structure(schema)

        expected_columns = df.columns.values.tolist()
        expected_dtypes = df.dtypes.tolist()
        self.assertTrue('columns' in schema.internal, "Internal schema must contain a 'columns' property")
        extracted_columns = schema.internal['columns']
        self.assertSequenceEqual(extracted_columns, expected_columns,
                                 "Internal schema has different columns than expected: {0}".format(extracted_columns))
        self.assertTrue('dtypes' in schema.internal, "Internal schema must contain a 'dtype' property")
        extracted_dtypes = schema.internal['dtypes']
        self.assertSequenceEqual(extracted_dtypes, expected_dtypes,
                                 "Internal schema has different dtypes than expected: {0}".format(extracted_dtypes))
        self.assertTrue('shape' in schema.internal, "Internal schema must contain a 'shape' property")
        extracted_shape = schema.internal['shape']
        self.assertEqual(extracted_shape, df.shape,
                         "Internal schema has different shape than expected: {0}".format(extracted_shape))

        self.assertDictEqual(expected_swagger, schema.swagger)
