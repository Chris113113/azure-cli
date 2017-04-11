# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""
Utilities to save and load schema information for Numpy Arrays.
"""

import numpy as np
import json
import pickle
from azure.cli.command_modules.ml.service.schema.common import BaseUtil, Schema
from azure.cli.command_modules.ml.service.schema.dtypeToSwagger import Dtype2Swagger


class NumpyUtil(BaseUtil):

    @classmethod
    def extract_schema(cls, array):
        if type(array) is not np.ndarray:
            raise TypeError('Only valid numpy array can be passed in to extract schema from.')
        schema = dict()
        schema['shape'] = array.shape
        schema['dtype'] = array.dtype

        swagger_type = Dtype2Swagger.convert_dtype_to_swagger(array.dtype)
        swagger_schema = Dtype2Swagger.handle_swagger_array(swagger_type, array.shape)
        return Schema(schema, swagger_schema)

    @staticmethod
    def parse_json_to_array(service_input, schema):
        parsed_input = json.loads(service_input)
        if type(parsed_input) != list:
            raise ValueError("Invalid input format: expected an array.")

        for i in range(len(parsed_input)):
            parsed_input[i] = NumpyUtil._numpify_json_object(parsed_input[i], schema['dtype'])

        numpy_array = np.array(object=parsed_input, dtype=schema['dtype'], copy=False)

        # Validate the schema of the parsed data against the known one
        expected_shape = schema['shape']
        parsed_dims = len(numpy_array.shape)
        expected_dims = len(expected_shape)
        if parsed_dims != expected_dims:
            raise ValueError(
                "Invalid input array: an array with {0} dimensions is expected; "
                "input has {1} [shape {2}]".format(expected_dims, parsed_dims, numpy_array.shape))
        for dim in range(1, len(expected_shape)):
            if numpy_array.shape[dim] != expected_shape[dim]:
                raise ValueError(
                    'Invalid input array: array has size {0} on dimension #{1}, '
                    'while expected value is {2}'.format(numpy_array.shape[dim], dim, expected_shape[dim]))

        return numpy_array

    @classmethod
    def _persist_internal_schema(cls, internal_schema, schema_filename):
        with open(schema_filename, 'wb') as f:
            pickle.dump(internal_schema, f)

    @classmethod
    def _load_internal_schema_object(cls, filename):
        super(NumpyUtil, cls)._load_internal_schema_object(filename)
        with open(filename, 'rb') as f:
            schema = pickle.load(f)
        return schema

    @classmethod
    def _validate_schema_object_on_load(cls, schema):
        if type(schema) == dict:
            BaseUtil._validate_schema_object_property(schema, 'dtype', 'numpy array')
            BaseUtil._validate_schema_object_property(schema, 'shape', 'numpy array')
            return True
        return False

    @staticmethod
    def _numpify_json_object(item, item_dtype):
        if len(item_dtype) > 0:
            converted_item = []
            for field_name in item_dtype.names:
                new_item_field = NumpyUtil._numpify_json_object(item[field_name], item_dtype[field_name])
                converted_item.append(new_item_field)
            return tuple(converted_item)
        else:
            return item
