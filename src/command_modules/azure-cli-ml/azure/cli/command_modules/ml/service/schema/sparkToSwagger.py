# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

from pyspark.sql.types import StructType, StructField, DataType, ArrayType, MapType


class Spark2Swagger(object):

    _switcher = {
        'byte': {'type': 'integer', 'format': 'int8'},
        'short': {'type': 'integer', 'format': 'int16'},
        'integer': {'type': 'integer', 'format': 'int32'},
        'long': {'type': 'integer', 'format': 'int64'},
        'boolean': {'type': 'boolean'},
        'float': {'type': 'number', 'format': 'float'},
        'double': {'type': 'number', 'format': 'double'},
        'decimal': {'type': 'number', 'format': 'double'},
        'string': {'type': 'string'},
        'binary': {'type': 'string', 'format': 'binary'},
        'date': {'type': 'string', 'format': 'date'},
        'timestamp': {'type': 'string', 'format': 'date-time'},
        'null': {'type': 'object'}
    }

    @staticmethod
    def convert_spark_dataframe_schema_to_swagger(spark_schema):
        # First get the schema for the structured type making up a dataframe row
        data_type_swagger = Spark2Swagger._convert_spark_schema_to_swagger(spark_schema)

        # Final schema is an array of such types
        return {'type': 'array', 'items': data_type_swagger}

    @staticmethod
    def convert_data_type_to_swagger(basic_type):
        if not isinstance(basic_type, DataType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.DataType types")

        type_name = basic_type.typeName()
        if type_name in Spark2Swagger._switcher.keys():
            return Spark2Swagger._switcher.get(type_name)
        elif type_name == 'array':
            return Spark2Swagger.convert_ArrayType_to_swagger(basic_type)
        elif type_name == 'map':
            return Spark2Swagger.convert_MapType_to_swagger(basic_type)
        else:
            return {'type': 'object'}

    @staticmethod
    def convert_ArrayType_to_swagger(array_type):
        if not isinstance(array_type, ArrayType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.ArrayType instances")
        element_schema = Spark2Swagger._convert_spark_schema_to_swagger(array_type.elementType)
        schema = {'type': 'array', 'items': element_schema}
        return schema


    @staticmethod
    def convert_MapType_to_swagger(map_type):
        if not isinstance(map_type, MapType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.MapType instances")
        value_schema = Spark2Swagger._convert_spark_schema_to_swagger(map_type.valueType)
        schema = {'type': 'object', 'additionalProperties': value_schema}
        return schema


    @staticmethod
    def get_swagger_object_schema():
        return {'type': 'object', 'properties': {}}

    @staticmethod
    def _convert_spark_schema_to_swagger(datatype):
        if not isinstance(datatype, DataType):
            raise TypeError("Invalid data type to convert: expected only valid pyspark.sql.types.DataType types")

        if type(datatype) is StructType:
            schema = {"type": "object", "properties": {}}
            for field_name in datatype.names:
                field_swagger = Spark2Swagger._convert_spark_schema_to_swagger(datatype[field_name].dataType)
                schema["properties"][field_name] = field_swagger
            return schema
        elif type(datatype) is StructField:
            schema = Spark2Swagger.convert_data_type_to_swagger(datatype.dataType)
        else:
            schema = Spark2Swagger.convert_data_type_to_swagger(datatype)
        return schema
