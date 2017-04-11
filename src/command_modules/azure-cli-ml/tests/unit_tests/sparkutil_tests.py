import json
from .schema_tests_common import SchemaUnitTests
from azure.cli.command_modules.ml.service.schema.common import Schema
from azure.cli.command_modules.ml.service.schema.sparkutil import SparkUtil
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


class SparkUtilTests(SchemaUnitTests):

    test_data_schema = StructType([StructField('string', StringType(), True),
                         StructField('array', ArrayType(StructType(
                             [StructField('f1', LongType(), True),
                              StructField('f2', BooleanType(), True),
                              StructField('f3', StringType(), True)]), False)),
                         StructField('map', MapType(StringType(), FloatType(), False), True)])

    def setUp(self):
        super().setUp()
        self.sqlContext = SQLContext(SparkUtil.spark_session.sparkContext)
        self.contacts_df = self.sqlContext.read.json('resources/contact_data.json')
        self.test_df = self.sqlContext.read.json('resources/test_data.json', self.test_data_schema)

    def test_schema_extract(self):
        # Test against a contacts data frame
        self._run_schema_extract_test(self.contacts_df, 'spark_contacts_df')
        # Test against a DataFrame with more diverse fields
        self._run_schema_extract_test(self.test_df, 'spark_test_df')

    def test_schema_save_load(self):
        self._run_schema_save_load_test(self.contacts_df, './contacts', 'spark_contacts_df')
        self._run_schema_save_load_test(self.test_df, './test_data', 'spark_test_df')

    def test_input_parsing(self):
        input_json = '['\
                       '{"string" : "test1", "map": {"AA": 5.3, "BB": -343.32}, "array": [{"f1": 121, "f2": false, "f3": "yada"}]},' \
                       '{"string" : "test2", "map": {"AA": 2.42, "BB": -0.25, "CC": 22.0}, "array": [{"f1": 10, "f2": true, "f3": "blah"}]},' \
                       '{"string" : "test3", "map": {"AA": 10.0, "BB": 99.12, "FF": 14.39}, "array": [{"f1": 3, "f2": false, "f3": "more blah"}]},' \
                       '{"string" : "test4", "map": {"AA": 1.3}, "array": [{"f1": 121, "f2": false, "f3": "yada"}, {"f1": 10, "f2": true, "f3": "blah"}]}' \
                     ']'
        self._run_input_parsing_test(input_json, self.test_df.schema, 4)

        input_json = '['\
                       '{"fname": "Michael", "lname": "Brown", "age": 58, "address": { "street": "112 Main Ave", "city": "Redmond", "state": "WA" }, "phone": "425 123 4567"},' \
                       '{"fname": "Mary", "lname": "Smith", "age": 45, "address": { "street": "34 Blue St.", "city": "Portland", "state": "OR" }, "phone": "425 111 2222"}' \
                     ']'
        self._run_input_parsing_test(input_json, self.contacts_df.schema, 2)

    def _run_schema_extract_test(self, df, expected_swagger_key):
        schema = SparkUtil.extract_schema(df)
        expected_swagger = self.expected_swagger[expected_swagger_key]

        self._validate_extracted_schema(df, schema, expected_swagger)

    def _run_schema_save_load_test(self, df, file_prefix, expected_swagger_key):
        try:
            # Extract & persist the data frame's schema on disk
            SparkUtil.save_data_schema(df, file_prefix)
            self._validate_schema_files_existence(file_prefix)

            # Load & validate the saved schema
            internal_schema = SparkUtil.load_data_schema(file_prefix + ".schema")
            with open(file_prefix + ".swagger.json", 'r') as f:
                swagger_schema = json.load(f)
            schema = Schema(internal_schema, swagger_schema)
            expected_swag = self.expected_swagger[expected_swagger_key]
            self._validate_extracted_schema(df, schema, expected_swag)
        finally:
            SchemaUnitTests._delete_test_schema_files(file_prefix)

    def _run_input_parsing_test(self, input_json_string, input_schema, expected_rows_count):
        input_df = SparkUtil.parse_json_to_data_frame(input_json_string, input_schema)

        self.assertIsNotNone(input_df, "Parsed input must have a value here")
        self.assertTrue(isinstance(input_df, DataFrame), "Parsed input must be a Spark data frame.")
        self.assertEqual(input_df.schema, input_schema,
                         "Parsed input schema {0} different from expected {1}".format(input_df.schema, input_schema))
        rows_count = input_df.count()
        self.assertEqual(rows_count, expected_rows_count,
                         "Parsed input row count of {0} is different than expected {1}".format(
                             rows_count, expected_rows_count))

    def _validate_extracted_schema(self, data_frame, schema, expected_swagger):
        self._validate_extracted_schema_structure(schema)
        self.assertEqual(schema.internal, data_frame.schema,
                         "Extracted internal schema {0} does not match expected one {1}".format(
                             schema.internal, data_frame.schema))
        self.assertDictEqual(expected_swagger, schema.swagger)
