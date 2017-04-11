# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""
Utilities to save and load schema information for Pandas DataFrames.
"""

import pandas as pd
import numpy as np
import pickle
from azure.cli.command_modules.ml.service.schema.common import BaseUtil, Schema
from azure.cli.command_modules.ml.service.schema.dtypeToSwagger import Dtype2Swagger


class PandasUtil(BaseUtil):

    @classmethod
    def extract_schema(cls, data_frame):
        if not isinstance(data_frame, pd.core.frame.DataFrame):
            raise TypeError('Only valid pandas data frames can be passed in to extract schema from.')

        # Construct internal schema
        internal_schema = dict()
        internal_schema['columns'] = data_frame.columns.values.tolist()
        internal_schema['dtypes'] = data_frame.dtypes.tolist()
        internal_schema['shape'] = data_frame.shape

        # Construct swagger schema as array of records
        df_record_swagger = Dtype2Swagger.get_swagger_object_schema()
        for i in range(len(internal_schema['columns'])):
            col_name = internal_schema['columns'][i]
            """
            For string columns, Pandas tries to keep a uniform item size
            for the support ndarray, and such it stores references to strings
            instead of the string's bytes themselves, which have variable size.
            Because of this, even if the data is a string, the column's dtype is
            marked as 'object' since the reference is an object.

            We try to be smart about this here and if the column type is reported as
            object, we will also check the actual data in the column to see if its not
            actually a string, such that we can generate a better swagger schema.
            """
            col_dtype = internal_schema['dtypes'][i]
            if internal_schema['dtypes'][i].name == 'object' and type(data_frame[col_name][0]) is str:
                col_dtype = np.dtype('str')
            col_swagger_type = Dtype2Swagger.convert_dtype_to_swagger(col_dtype)
            df_record_swagger['properties'][col_name] = col_swagger_type
        swagger_schema = {'type': 'array', 'items': df_record_swagger}

        return Schema(internal_schema, swagger_schema)

    @staticmethod
    def parse_json_to_data_frame(service_input, schema):
        data_frame = pd.read_json(service_input, orient='records', dtype=schema['dtypes'])

        # Validate the schema of the parsed data against the known one
        df_cols = data_frame.columns.values.tolist()
        schema_cols = schema['columns']
        if len(df_cols) != len(schema_cols) or len(schema_cols) != len(list(set(df_cols) & set(schema_cols))):
            raise ValueError(
                "Column mismatch between input data frame and expected schema\n\t"
                "Passed in columns: {0}\n\tExpected columns: {1}".format(df_cols, schema_cols))

        expected_shape = schema['shape']
        parsed_data_dims = len(data_frame.shape)
        expected_dims = len(expected_shape)
        if parsed_data_dims != expected_dims:
            raise ValueError(
                "Invalid input data frame: a data frame with {0} dimensions is expected; "
                "input has {1} [shape {2}]".format(expected_dims, parsed_data_dims, data_frame.shape))

        for dim in range(1, len(expected_shape)):
            if data_frame.shape[dim] != expected_shape[dim]:
                raise ValueError(
                    "Invalid input data frame: data frame has size {0} on dimension #{1}, "
                    "while expected value is {2}".format(
                        data_frame.shape[dim], dim, expected_shape[dim]))

        return data_frame

    @classmethod
    def _persist_internal_schema(cls, internal_schema, schema_filename):
        with open(schema_filename, 'wb') as f:
            pickle.dump(internal_schema, f)

    @classmethod
    def _load_internal_schema_object(cls, filename):
        super(PandasUtil, cls)._load_internal_schema_object(filename)
        with open(filename, 'rb') as f:
            schema = pickle.load(f)
        return schema

    @classmethod
    def _validate_schema_object_on_load(cls, schema):
        if type(schema) == dict:
            BaseUtil._validate_schema_object_property(schema, 'dtypes', 'pandas data frame')
            BaseUtil._validate_schema_object_property(schema, 'columns', 'pandas data frame')
            BaseUtil._validate_schema_object_property(schema, 'shape', 'pandas data frame')
            return True
        return False
