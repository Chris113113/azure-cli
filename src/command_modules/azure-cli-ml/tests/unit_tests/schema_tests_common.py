import unittest
import os.path
import json


class SchemaUnitTests(unittest.TestCase):

    def setUp(self):
        with open('./resources/test_swagger_data.json', 'r') as f:
            self.expected_swagger = json.load(f)

    def _validate_extracted_schema_structure(self, schema):
        self.assertIsNotNone(schema, "Extracted array schema cannot be None")
        self.assertIsNotNone(schema.internal, "Expected internal schema is None")
        self.assertIsNotNone(schema.swagger, "Expected swagger schema is None")

    def _validate_schema_files_existence(self, file_prefix):
        self.assertTrue(os.path.exists(file_prefix + '.schema'), "Expected input schema was not found")
        self.assertTrue(os.path.exists(file_prefix + '.swagger.json'), "Expected input swagger schema was not found")

    @staticmethod
    def _delete_test_schema_files(file_prefix):
        internal_schema_file = file_prefix + '.schema'
        if os.path.exists(internal_schema_file):
            os.remove(internal_schema_file)
        swagger_file = file_prefix + '.swagger.json'
        if os.path.exists(swagger_file):
            os.remove(swagger_file)

    @staticmethod
    def _get_internal_schema_file_name(file_prefix):
        return file_prefix + '.schema'

    @staticmethod
    def _get_swagger_schema_file_name(file_prefix):
        return file_prefix + '.swagger.json'
