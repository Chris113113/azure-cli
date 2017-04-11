# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------
"""
Utilities to save and load schema information for Spark DataFrames.
"""

import json
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from azure.cli.command_modules.ml.service.schema.common import BaseUtil, Schema
from azure.cli.command_modules.ml.service.schema.sparkToSwagger import Spark2Swagger


class SparkUtil(BaseUtil):

    spark_session = SparkSession.builder.getOrCreate()
    sqlContext = SQLContext(spark_session.sparkContext)

    @classmethod
    def extract_schema(cls, data_frame):
        if not isinstance(data_frame, DataFrame):
            raise TypeError('Invalid data type: expected a Spark data frame.')

        internal_schema = data_frame.schema
        swagger_schema = Spark2Swagger.convert_spark_dataframe_schema_to_swagger(data_frame.schema)
        return Schema(internal_schema, swagger_schema)

    @staticmethod
    def parse_json_to_data_frame(service_input, schema):
        input_data = json.loads(service_input)
        data_frame = SparkUtil.sqlContext.createDataFrame(data=input_data, schema=schema, verifySchema=True)
        return data_frame

    @classmethod
    def _persist_internal_schema(cls, internal_schema, schema_filename):
        with open(schema_filename, 'w') as f:
            json.dump(internal_schema.jsonValue(), f)

    @classmethod
    def _load_internal_schema_object(cls, filename):
        super(SparkUtil, cls)._load_internal_schema_object(filename)
        with open(filename, 'r') as f:
            schema_json = json.load(f)
        schema = StructType.fromJson(schema_json)
        return schema

    @classmethod
    def _validate_schema_object_on_load(cls, schema):
        return type(schema) == StructType
