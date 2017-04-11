# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------

import json
import os.path
import abc


class Schema:
    def __init__(self, internal_form, swagger_form):
        self.internal = internal_form
        self.swagger = swagger_form


class BaseUtil(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractclassmethod
    def extract_schema(cls, data):
        pass

    @classmethod
    def save_data_schema(cls, data_sample, filename_prefix):
        if filename_prefix is None or len(filename_prefix) == 0:
            raise ValueError('A filename prefix for the schema files must be specified')
        target_dir = os.path.dirname(filename_prefix)
        if len(target_dir) > 0 and not os.path.exists(target_dir):
            raise ValueError('Please specify a valid path to save the schema files to')

        if data_sample is not None:
            schema = cls.extract_schema(data_sample)
            print("Internal schema is:\n{0}".format(schema.internal))
            cls._persist_schema_files(schema, filename_prefix)

    @classmethod
    def load_data_schema(cls, schema_filename):
        schema_object = cls._load_internal_schema_object(schema_filename)
        if cls._validate_schema_object_on_load(schema_object):
            return schema_object
        else:
            err = 'The contents of {0} are not a valid schema format.'.format(schema_filename)
            raise ValueError(err)

    @abc.abstractclassmethod
    def _validate_schema_object_on_load(cls, schema_filename):
        pass

    @abc.abstractclassmethod
    def _persist_internal_schema(cls, internal_schema, schema_filename):
        pass

    @abc.abstractclassmethod
    def _load_internal_schema_object(cls, filename):
        if filename is None:
            raise TypeError('A filename must be specified.')
        if not os.path.exists(filename):
            raise ValueError('Specified schema file cannot be found: {}.'.format(filename))
        pass

    @classmethod
    def _persist_schema_files(cls, data_schema, file_prefix):
        schema_filename = file_prefix + '.schema'
        cls._persist_internal_schema(data_schema.internal, schema_filename)
        print('Platform specific schema written to: ', schema_filename)

        swagger_filename = file_prefix + '.swagger.json'
        with open(swagger_filename, 'w') as f:
            json.dump(data_schema.swagger, f)
        print('Swagger schema written to: ', swagger_filename)

    @staticmethod
    def _validate_schema_object_property(schema, property_name, data_structure):
        if property_name not in schema or schema[property_name] is None:
            err = "Invalid {0} schema. The {1} property is not specified.".format(data_structure, property_name)
            raise ValueError(err)
