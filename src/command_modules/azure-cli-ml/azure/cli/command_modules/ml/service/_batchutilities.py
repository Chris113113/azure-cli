# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for license information.
# --------------------------------------------------------------------------------------------


"""
batch_cli_util.py - Defines utilities, constants for batch portion of azureml CLI
"""

from __future__ import print_function

import json
import sys
from time import sleep
from re import match
import uuid
import os
from collections import OrderedDict

import requests

from .._util import ConditionalListTraversalFunction
from .._util import TraversalFunction
from .._util import ValueFunction
from .._util import get_json
from .._util import get_success_and_resp_str
from .._util import ice_connection_timeout
from pkg_resources import resource_filename
from pkg_resources import resource_string
from ._realtimeutilities import upload_dependency
from ...ml import __version__
from azure.storage.blob import (BlockBlobService, ContentSettings, BlobPermissions)
from ._realtimeutilities import try_add_sample_file
from datetime import datetime, timedelta
from ._realtimeutilities import test_acs_k8s
from .._k8s_util import KubernetesOperations
from kubernetes.client.rest import ApiException

# CONSTANTS
BATCH_URL_BASE_FMT = '{}'
BATCH_HEALTH_FMT = '{}/v1/health'
BATCH_DEPLOYMENT_INFO_FMT = '{}/v1/deploymentinfo'
BATCH_ALL_WS_FMT = '{}/v1/webservices'
BATCH_SINGLE_WS_FMT = '{}/{{{{}}}}'.format(BATCH_ALL_WS_FMT)
BATCH_ALL_JOBS_FMT = '{}/jobs'.format(BATCH_SINGLE_WS_FMT)
BATCH_SINGLE_JOB_FMT = '{}/{{{{}}}}'.format(BATCH_ALL_JOBS_FMT)
BATCH_CANCEL_JOB_FMT = '{}/cancel'.format(BATCH_SINGLE_JOB_FMT)
BATCH_PYTHON_ASSET = 'PythonAssets'
BATCH_JAR_ASSET = 'JarAssets'
BATCH_FILE_ASSET = 'FileAssets'

BATCH_EXTENSION_TO_ASSET_DICT = {'.py': BATCH_PYTHON_ASSET,
                                 '.jar': BATCH_JAR_ASSET}


# EXCEPTION CLASSES
class InvalidStorageException(Exception):
    """
    Exception raised when determining valid storage failsf
    """


# UTILITY FUNCTIONS
def batch_get_url(context, fmt, *args):
    """
    function to construct target url depending on whether in local mode or not
    :param context: CommandLineInterfaceContext object
    :param fmt: str format string to build url from
    :param args: list arguments to populate format string with
    :return:
    """
    base = 'http://localhost:8080'
    if context.env_is_k8s:
        base = 'http://{}'.format(batch_get_k8s_frontend_url())
    else:
        base = 'https://{}-aml.apps.azurehdinsight.net'.format(context.hdi_domain)
    return fmt.format(base).format(*args)


def batch_get_asset_type(asset_id):
    """

    :param asset_id: str id of asset, expected form <name>.<extension>
    :return: str type of resource the asset's extension indicates
    """
    extension = os.path.splitext(asset_id)[1]
    if extension in BATCH_EXTENSION_TO_ASSET_DICT:
        return BATCH_EXTENSION_TO_ASSET_DICT[extension]

    return BATCH_FILE_ASSET


def batch_get_parameter_str(param_dict):
    """

    :param param_dict: dictionary of Parameter descriptions
    :return: formatted string for Usage associated with this parameter
    """
    letter = '--out' if param_dict['Direction'] == 'Output' else \
        ('--in' if param_dict['Kind'] == 'Reference' else '--param')
    ret_val = '{}={}:<value>'.format(letter, param_dict['Id'])
    return '[{}]'.format(ret_val) if 'Value' in param_dict else ret_val


def batch_get_job_description(context, http_content):
    """

    :param http_content: requests.content object with json encoded job
    :return: str value to print as job description
    """
    json_obj = get_json(http_content)
    return_str = 'Name: {}\n'.format(json_obj['WebServiceId'])
    return_str += 'JobId: {}\n'.format(json_obj['JobId'])
    if 'YarnAppId' in json_obj:
        return_str += 'YarnAppId: {}\n'.format(json_obj['YarnAppId'])
        return_str += 'Logs available at: https://{}.azurehdinsight.net/' \
                      'yarnui/hn/cluster/app/{}\n'.format(context.hdi_domain, json_obj['YarnAppId'])
    elif 'DriverLogFile' in json_obj:
        return_str += 'Logs available at: {}\n'.format(json_obj['DriverLogFile'])
    return_str += 'State: {}'.format(json_obj['State'])
    return return_str


def batch_create_parameter_entry(name, kind, direction):
    """

    :param name: str name of the parameter, in the form "<name>[=<default_value>]"
    :param kind: str kind of parameter (Reference|Value)
    :param direction: str direction of parameter (Input|Output)
    :return: dict encoding of the parameter for transmission to SparkBatch
    """
    return_value = {"Id": name,
                    "IsRuntime": True,
                    "IsOptional": False,
                    "Kind": kind,
                    "Direction": direction}
    if ':' in name:
        # need default value
        return_value['Id'] = name.split(':')[0]
        return_value['Value'] = ':'.join(name.split(':')[1:])

    return return_value


def batch_create_parameter_list(arg_list):
    """

    :param arg_list: list of tuples of the form [(name, direction, kind)]
            name: str name of the parameter, in the form "<name>[=<default_value>]"
            direction: str direction of the parameter (Input|Output)
            kind: str kind of the parameter (Reference|Value)
    :return: list of dicts encoding the parameters for transmission to SparkBatch
    """
    return [batch_create_parameter_entry(name, kind, direction)
            for (name, direction, kind) in arg_list]


def batch_app_is_installed(context):
    """

    :return: int response code, None if connection error
    """
    url = batch_get_url(context, BATCH_HEALTH_FMT)
    try:
        resp = context.http_call('get', url, auth=get_auth(context))
        return resp.status_code
    except requests.exceptions.ConnectionError:
        return None


def batch_get_acceptable_storage(context):
    """

    :return: list of str - names of acceptable storage returned from the
    """
    url = batch_get_url(context, BATCH_DEPLOYMENT_INFO_FMT)
    try:
        success, content = get_success_and_resp_str(context, context.http_call('get', url,
                                                                               auth=get_auth(context)))
    except requests.ConnectionError:
        raise InvalidStorageException(
            "Error connecting to {}. Please confirm SparkBatch app is healthy.".format(
                url))

    if not success:
        raise InvalidStorageException(content)
    deployment_info = get_json(content)
    if 'Storage' not in deployment_info:
        raise InvalidStorageException('No storage found in deployment info.')

    return [info['Value'].strip() for info in deployment_info['Storage']]


def batch_env_is_valid(context):
    """

    :return: bool True if all of the following are true:
        1. environment specifies a SparkBatch location
        2. the app at that location is healthy
    """
    hdi_exists = False
    app_present = False
    if not context.in_local_mode() and not context.env_is_k8s and (not context.hdi_domain or not context.hdi_user or not context.hdi_pw):
        print("")
        print("Environment is missing the following variables:")
        if not context.hdi_domain:
            print("  AML_HDI_CLUSTER")
        if not context.hdi_user:
            print("  AML_HDI_USER")
        if not context.hdi_pw:
            print("  AML_HDI_PW")
        print("For help setting up environment, run")
        print("  az ml env about")
        print("")
    else:
        hdi_exists = True

    # check if the app is installed via health api
    if hdi_exists:
        app_ping_return_code = batch_app_is_installed(context)
        if app_ping_return_code is None or app_ping_return_code == 404:
            print("AML Batch is not currently installed on {0}. "
                  "Please install the app.".format(batch_get_url(context,
                                                                 BATCH_URL_BASE_FMT,
                                                                 context.hdi_domain)))
        elif app_ping_return_code == 200:
            app_present = True
        elif app_ping_return_code == 403:
            print('Authentication failed on {}. Check your AML_HDI_USER and '
                  'AML_HDI_PW environment variables.'.format(
                      batch_get_url(context, BATCH_URL_BASE_FMT, context.hdi_domain)))
            print("For help setting up environment, run")
            print("  az ml env about")
            print("")
        else:
            print('Unexpected return code {} when querying AzureBatch '
                  'at {}.'.format(app_ping_return_code,
                                  batch_get_url(context, BATCH_URL_BASE_FMT, context.hdi_domain)))
            print("If this error persists, contact the SparkBatch team for more "
                  "information.")
    return hdi_exists and app_present


def batch_env_and_storage_are_valid(context):
    """

    :return: bool True if all of the following are true:
        1. environment specifies a SparkBatch location
        2. the app at that location is healthy
        3. storage is defined in the environment
        4. the storage matches the storage associated with the SparkBatch app (for HDI)
    """
    if not batch_env_is_valid(context):
        return False

    if context.in_local_mode():
        return True

    if not context.az_account_name or not context.az_account_key:
        print("")
        print("Environment is missing the following variables:")
        if not context.az_account_name:
            print("  AML_STORAGE_ACCT_NAME")
        if not context.az_account_key:
            print("  AML_STORAGE_ACCT_KEY.")
        print("For help setting up environment, run")
        print("  az ml env about")
        print("")
        return False

    try:
        acceptable_storage = batch_get_acceptable_storage(context)
    except InvalidStorageException as exc:
        print("Error retrieving acceptable storage from SparkBatch: {}".format(exc))
        return False

    if context.az_account_name not in acceptable_storage:
        print("Environment storage account {0} not found when querying server "
              "for acceptable storage. Available accounts are: "
              "{1}".format(context.az_account_name, ', '.join(acceptable_storage)))
        return False

    return True


def batch_get_job(context, job_name, service_name, verbose=False):
    """

    :param context: CommandLineInterfaceContext object
    :param job_name: str name of job to get
    :param service_name: str name of service that job belongs to
    :param verbose: bool verbosity flag
    :return:
    """
    url = batch_get_url(context, BATCH_SINGLE_JOB_FMT, service_name, job_name)
    if verbose:
        print("Getting resource at {}".format(url))
    try:
        return context.http_call('get', url, auth=get_auth(context))
    except requests.ConnectionError:
        print("Error connecting to {}. Please confirm SparkBatch app is healthy.".format(url))
        return


class BatchEnvironmentFunction(ValueFunction):
    """
    ValueFunction object for use displaying the current environment
    """
    def evaluate(self, context):
        return batch_get_url(context, BATCH_URL_BASE_FMT)


class ScoringUrlFunction(ValueFunction):
    """
    ValueFunction object for use displaying API endpoint of a service
    """
    def evaluate(self, context):
        return batch_get_url(context,
                             BATCH_SINGLE_JOB_FMT, self.json_obj['Id'],
                             '<job_id>')

batch_create_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Id',))), ('Environment', BatchEnvironmentFunction())])


batch_list_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Name',))),
     ('Last_Modified_at', TraversalFunction(('ModificationTimeUtc',))),
     ('Environment', BatchEnvironmentFunction())
    ])

batch_list_jobs_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Name',))),
     ('Last_Modified_at', TraversalFunction(('ModificationTimeUtc',))),
     ('Environment', BatchEnvironmentFunction())])

batch_view_service_header_to_fn_dict = OrderedDict(
    [('Name', TraversalFunction(('Id',))),
     ('Environment', BatchEnvironmentFunction())])

batch_view_service_usage_header_to_fn_dict = OrderedDict(
    [('Scoring_url', ScoringUrlFunction()),
     ('Inputs',
      ConditionalListTraversalFunction(
          ('Parameters',),
          condition=lambda param: (param['Kind'] == 'Reference' and
                                   param['Direction'] == 'Input'),
          action=lambda param: param['Id'])),
     ('Outputs', ConditionalListTraversalFunction(
         ('Parameters',),
         condition=lambda param: (param['Kind'] == 'Reference' and
                                  param['Direction'] == 'Output'),
         action=lambda param: param['Id'])),
     ('Parameters', ConditionalListTraversalFunction(
         ('Parameters',),
         condition=lambda param: (param['Kind'] == 'Value' and
                                  param['Direction'] == 'Input'),
         action=lambda param: param['Id']))
    ])


def validate_and_split_run_param(raw_param):
    """

    :param raw_param: str parameter in form <key>:<value>
    :return: (bool, str, str) - (valid, key, value)
    """
    if ':' not in raw_param:
        print("Must provide value for service parameter {0}".format(raw_param))
        return False, None, None
    else:
        return True, raw_param.split(':')[0], ':'.join(raw_param.split(':')[1:])


def get_auth(context):
    """
    Get correct authorization headers
    :param context:
    :return:
    """
    if context.env_is_k8s:
        # Currently we have no Authorization around Kubernetes services
        return None
    return context.hdi_user, context.hdi_pw


def create_batch_docker_image(score_file, dependencies, service_name, verb, context):
    """Create a new realtime web service."""

    verbose = verb
    storage_exists = False
    acr_exists = False

    if context.az_account_name is None or context.az_account_key is None:
        print("")
        print("Please set up your storage account for AML:")
        print("  export AML_STORAGE_ACCT_NAME=<yourstorageaccountname>")
        print("  export AML_STORAGE_ACCT_KEY=<yourstorageaccountkey>")
        print("")
    else:
        storage_exists = True

    acs_exists = test_acs_k8s()
    if not acs_exists:
        print('')
        print('Your Kubernetes cluster is not responding as expected.')
        print('Please verify it is healthy. If you set it up via `az ml env setup,` '
              'please contact deployml@microsoft.com to troubleshoot.')
        print('')

    if context.acr_home is None or context.acr_user is None or context.acr_pw is None:
        print("")
        print("Please set up your ACR registry for AML:")
        print("  export AML_ACR_HOME=<youracrdomain>")
        print("  export AML_ACR_USER=<youracrusername>")
        print("  export AML_ACR_PW=<youracrpassword>")
        print("")
    else:
        acr_exists = True

    if not match(r"[a-zA-Z0-9\.-]+", service_name):
        print("Kubernetes Service names may only contain alphanumeric characters, '.', and '-'")
        return

    if not storage_exists or not acs_exists or not acr_exists:
        return

    # modify json payload to update assets and driver location
    payload = resource_string(__name__, 'data/testrequest.json')
    json_payload = json.loads(payload.decode('ascii'))

    # update target runtime in payload
    json_payload['properties']['deploymentPackage']['targetRuntime'] = 'spark-py'

    # Add dependencies

    # Always inject azuremlutilities.py as a dependency from the CLI
    # It contains helper methods for serializing and deserializing schema
    utilities_filename = resource_filename(__name__, 'azuremlutilities.py')
    dependencies.append(utilities_filename)

    dependency_injection_code = '\nimport tarfile\n'
    dependency_count = 0
    if dependencies is not None:
        print('Uploading dependencies.')
        for dependency in dependencies:
            (status, location, filename) = \
                upload_dependency(context, dependency, verbose)
            if status < 0:
                print('Error resolving dependency: no such file or directory {}'.format(dependency))
                return
            else:
                dependency_count += 1
                # Add the new asset to the payload
                new_asset = {'mimeType': 'application/octet-stream',
                             'id': str(dependency),
                             'location': location}
                json_payload['properties']['assets'].append(new_asset)
                if verbose:
                    print("Added dependency {} to assets.".format(dependency))

                # If the asset was a directory, also add code to unzip and layout directory
                if status == 1:
                    dependency_injection_code = dependency_injection_code + \
                                                'amlbdws_dependency_{} = tarfile.open("{}")\n'\
                                                .format(dependency_count, filename)
                    dependency_injection_code = dependency_injection_code + \
                                                'amlbdws_dependency_{}.extractall()\n'.format(dependency_count)

    if verbose:
        print("Code injected to unzip directories:\n{}".format(dependency_injection_code))
        print(json.dumps(json_payload))

    # read in code file
    if os.path.isfile(score_file):
        with open(score_file, 'r') as scorefile:
            code = scorefile.read()
    else:
        print("Error: No such file {}".format(score_file))
        return

    # Spark specific operations
    # read in fixed preamble code
    preamble = resource_string(__name__, 'data/preamble').decode('ascii')

    # wasb configuration: add the configured storage account in the as a wasb location
    wasb_config = "spark.sparkContext._jsc.hadoopConfiguration().set('fs.azure.account.key." + \
                   context.az_account_name + ".blob.core.windows.net','" + context.az_account_key + "')"

    # create blob with preamble code and user function definitions from cell
    code = "{}\n{}\n{}\n{}\n\n\n".format(preamble, wasb_config, dependency_injection_code, code)

    if verbose:
        print(code)

    az_container_name = 'amlbdpackages'
    az_blob_name = str(uuid.uuid4()) + '.py'
    bbs = BlockBlobService(account_name=context.az_account_name,
                           account_key=context.az_account_key)
    bbs.create_container(az_container_name)
    bbs.create_blob_from_text(az_container_name, az_blob_name, code,
                              content_settings=ContentSettings(content_type='application/text'))
    blob_sas = bbs.generate_blob_shared_access_signature(
        az_container_name,
        az_blob_name,
        BlobPermissions.READ,
        datetime.utcnow() + timedelta(days=30))
    package_location = 'http://{}.blob.core.windows.net/{}/{}?{}'.format(context.az_account_name,
                                                                         az_container_name, az_blob_name, blob_sas)

    if verbose:
        print("Package uploaded to " + package_location)

    for asset in json_payload['properties']['assets']:
        if asset['id'] == 'driver_package_asset':
            if verbose:
                print("Current driver location:", str(asset['location']))
                print("Replacing with:", package_location)
            asset['location'] = package_location

    # modify json payload to set ACR credentials
    if verbose:
        print("Current ACR creds in payload:")
        print('location:', json_payload['properties']['registryInfo']['location'])
        print('user:', json_payload['properties']['registryInfo']['user'])
        print('password:', json_payload['properties']['registryInfo']['password'])

    json_payload['properties']['registryInfo']['location'] = context.acr_home
    json_payload['properties']['registryInfo']['user'] = context.acr_user
    json_payload['properties']['registryInfo']['password'] = context.acr_pw

    if verbose:
        print("New ACR creds in payload:")
        print('location:', json_payload['properties']['registryInfo']['location'])
        print('user:', json_payload['properties']['registryInfo']['user'])
        print('password:', json_payload['properties']['registryInfo']['password'])

    # call ICE with payload to create docker image

    # Set base ICE URL
    base_ice_url = 'https://amlacsagent.azureml-int.net'

    create_url = base_ice_url + '/images/' + service_name
    get_url = base_ice_url + '/jobs'
    headers = {'Content-Type': 'application/json', 'User-Agent': 'aml-cli-{}'.format(__version__)}

    image = ''
    max_retries = 3
    try_number = 0
    ice_put_result = {}
    while try_number < max_retries:
        try:
            ice_put_result = requests.put(
                create_url, headers=headers, data=json.dumps(json_payload), timeout=ice_connection_timeout)
            break
        except (requests.ConnectionError, requests.exceptions.ReadTimeout):
            if try_number < max_retries:
                try_number += 1
                continue
            print('Error: could not connect to Azure ML. Please try again later. If the problem persists, please contact deployml@microsoft.com') #pylint: disable=line-too-long
            return

    if ice_put_result.status_code == 401:
        print("Invalid API key. Please update your key by running 'az ml env key -u'.")
        return
    elif ice_put_result.status_code != 201:
        print('Error connecting to Azure ML. Please contact deployml@microsoft.com with the stack below.')
        print(ice_put_result.content)
        return

    if verbose:
        print(ice_put_result)
    if isinstance(ice_put_result.json(), str):
        return json.dumps(ice_put_result.json())

    job_id = ice_put_result.json()['Job Id']
    if verbose:
        print('ICE URL: ' + create_url)
        print('Submitted job with id: ' + json.dumps(job_id))
    else:
        sys.stdout.write('Creating docker image.')
        sys.stdout.flush()

    job_status = requests.get(get_url + '/' + job_id, headers=headers)
    response_payload = job_status.json()
    while 'Provisioning State' in response_payload:
        job_status = requests.get(get_url + '/' + job_id, headers=headers)
        response_payload = job_status.json()
        if response_payload['Provisioning State'] == 'Running':
            sleep(5)
            if verbose:
                print("Provisioning image. Details: " + response_payload['Details'])
            else:
                sys.stdout.write('.')
                sys.stdout.flush()
            continue
        else:
            if response_payload['Provisioning State'] == 'Succeeded':
                acs_payload = response_payload['ACS_PayLoad']
                acs_payload['container']['docker']['image'] = json_payload['properties']['registryInfo']['location'] \
                                                              + '/' + service_name
                image = acs_payload['container']['docker']['image']
                break
            else:
                print('Error creating image: ' + json.dumps(response_payload))
                return

    print('done.')
    print('Image available at : {}'.format(image))
    return image

def batch_get_k8s_frontend_url():
    frontend_service_name = 'sparkbatch'
    k8s_ops = KubernetesOperations()
    try:
        frontend_service = k8s_ops.get_service(frontend_service_name)
        if frontend_service.status.load_balancer.ingress is None:
            raise ApiException(status=404, reason="LoadBalancer has not finished being created for the Kubernetes Front-end. Please try again in a few minutes.")
    except ApiException as exc:
        print("Unable to load details for AzureML Kubernetes Front-End server. {}".format(exc))
        raise

    base_url = frontend_service.status.load_balancer.ingress[0].ip
    port = frontend_service.spec.ports[0].port

    frontend_url = '{}:{}'.format(base_url, port)

    return frontend_url
