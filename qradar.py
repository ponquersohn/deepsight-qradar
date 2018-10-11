import json
import os
import sys
import time
import importlib
import logging

from urllib2 import HTTPError
from urllib2 import URLError
from urllib2 import quote
from urllib2 import Request
from urllib2 import urlopen
from urllib2 import install_opener
from urllib2 import build_opener
from urllib2 import HTTPSHandler

import urllib

import ssl
import sys
import base64

import httplib
import urllib2

from configWrapper import configWrapper

class qradarAPI:
    
    def __init__(self,config, debug = False, version=None):
        #sys.path.append(os.path.realpath('modules'))
        #self.client = self.client_module.RestApiClient(config, version='9.0')
        self.logger = logging.getLogger('')
        
        self.config = configWrapper(config)

        if debug:
            self.debug = 1
            httplib.debuglevel = self.debug
            httplib.HTTPConnection.debuglevel = self.debug
        else:
            self.debug = 0
            
        self.headers = {'Accept': 'application/json'}
        if version is not None:
            self.headers['Version'] = version
        if self.config.has_config_value('auth_token'):
            self.headers['SEC'] = self.config.get_config_value('auth_token')
        elif (self.config.has_config_value('username') and
              self.config.has_config_value('password')):
            username = self.config.get_config_value('username')
            password = self.config.get_config_value('password')
            self.headers['Authorization'] = b"Basic " + base64.b64encode(
                (username + ':' + password).encode('ascii'))
        else:
            raise Exception('No valid credentials found in configuration.')

        self.server_ip = self.config.get_config_value('server_ip')
        self.base_uri = '/api/'

        # Create a secure SSLContext
        # PROTOCOL_SSLv23 is misleading.  PROTOCOL_SSLv23 will use the highest
        # version of SSL or TLS that both the client and server supports.
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)

        # SSL version 2 and SSL version 3 are insecure. The insecure versions
        # are disabled.
        try:
            context.options = ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
        except ValueError as e:
            # Disabling SSLv2 and SSLv3 is not supported on versions of OpenSSL
            # prior to 0.9.8m.
            if not (self.config.has_config_value('ssl_2_3_ok') and
                    self.config.get_config_value('ssl_2_3_ok') == 'true'):
                print('WARNING: Unable to disable SSLv2 and SSLv3. Caused '
                      'by exception "' + str(e) + '"')
                while True:
                    response = input(
                        "Would you like to continue anyway (yes/no)? "
                        ).strip().lower()
                    if response == "no":
                        sys.exit(1)
                    elif response == "yes":
                        self.config.set_config_value('ssl_2_3_ok', 'true')
                        break
                    else:
                        print(response + " is not a valid response.")

        context.verify_mode = ssl.CERT_REQUIRED
        if sys.version_info >= (3, 4):
            context.check_hostname = True

        check_hostname = True
        certificate_file = self.config.get_config_value('certificate_file')
        if certificate_file is not None:
            # Load the certificate if the user has specified a certificate
            # file in config.ini.

            # The default QRadar certificate does not have a valid hostname,
            # so me must disable hostname checking.
            if sys.version_info >= (3, 4):
                context.check_hostname = False
            check_hostname = False

            # Instead of loading the default certificates load only the
            # certificates specified by the user.
            context.load_verify_locations(cafile=certificate_file)
        else:
            if sys.version_info >= (3, 4):
                # Python 3.4 and above has the improved load_default_certs()
                # function.
                context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
            else:
                # Versions of Python before 3.4 do not have the
                # load_default_certs method.  set_default_verify_paths will
                # work on some, but not all systems.  It fails silently.  If
                # this call fails the certificate will fail to validate.
                context.set_default_verify_paths()

        install_opener(build_opener(
            HTTPSHandler(context=context, check_hostname=check_hostname, debuglevel=self.debug)))

    def call(self, endpoint, method, headers=None, params=[], data=None,
                 print_request=False, print_response = False):

        path = self.parse_path(endpoint, params)

        # If the caller specified customer headers merge them with the default
        # headers.
        actual_headers = self.headers.copy()
        if headers is not None:
            for header_key in headers:
                actual_headers[header_key] = headers[header_key]

        # Send the request and receive the response
        request = Request(
            'https://' + self.server_ip + self.base_uri + path,
            headers=actual_headers)
        request.get_method = lambda: method

        # Print the request if print_request is True.
        if print_request:
            self.dump_request(self, path, method,
                                                 headers=actual_headers, data = data)                                               
        try:
           
            response = urlopen(request, data)
            response_info = response.info()
            if (print_response):
                self.dump_response(response)
            if 'Deprecated' in response_info:

                # This version of the API is Deprecated. Print a warning to
                # stderr.
                print("WARNING: " + response_info['Deprecated'])

            # returns response object for opening url.
            return response
        except HTTPError as e:
            # an object which contains information similar to a request object
            return e
        except URLError as e:
            if (isinstance(e.reason, ssl.SSLError) and
                    e.reason.reason == "CERTIFICATE_VERIFY_FAILED"):
                print("Certificate verification failed.")
                sys.exit(3)
            else:
                raise e

    # This method constructs the query string
    def parse_path(self, endpoint, params):

        path = endpoint + '?'

        if isinstance(params, list):

            for kv in params:
                if kv[1]:
                    path += kv[0]+'='+quote(kv[1])+'&'

        else:
            for k, v in params.items():
                if params[k]:
                    path += k+'='+quote(v)+'&'

        # removes last '&' or hanging '?' if no params.
        return path[:len(path)-1]

    # Simple getters that can be used to inspect the state of this client.
    def get_headers(self):
        return self.headers.copy()

    def get_server_ip(self):
        return self.server_ip

    def get_base_uri(self):
        return self.base_uri            
            
    def listReferenceTables(self, print_request=False, print_response=False):
        self.logger.debug("* listReferenceTables: start")
        response = self.call('reference_data/tables', 'GET', print_request=print_request, print_response=print_response)
 
        self.logger.debug("* listReferenceTables: end")
        return response
    
    def addReferenceTable(self, table, keymap, print_request=False, print_response=False):
        self.logger.debug("* addReferenceTable: start")
        params = {
            'name': table,
            'key_name_types': json.dumps(keymap),
            'element_type': 'ALN'
        }
        response = self.call('reference_data/tables', 'POST', params=params, print_request=print_request, print_response=print_response)
        if (response.code == 409):
            self.logger.warning("Table already exists")
        elif(response.code >= 400):
            self.logger.error("An error occurred inserting table:")
            self.logger.debug(self.dump_response(response))
            sys.exit(-1)
        #self.logger.debug(self.dump_response(response))
        self.logger.debug("* addReferenceTable: end")
    
    def delReferenceTable(self, table, waitForDelete = False, print_request=False, print_response=False):
        self.logger.debug("* delReferenceTable: start")
        response = self.call('reference_data/tables/{}'.format(table), 'DELETE', print_request=print_request, print_response=print_response)
        if (response.code == 404):
            self.logger.debug("Reference table not found.")
            return
        elif (response.code >= 400):
            self.logger.error("An error occurred inserting table:")
            self.logger.debug(self.dump_response(response))
            sys.exit(-1)
        
        if waitForDelete:
            while self.checkIfReferenceTableExists(table):
                time.sleep(0.5)
        self.logger.debug("* delReferenceTable: end")
        
    def checkIfReferenceTableExists(self, table, print_request=False, print_response=False):
        response = self.call('reference_data/tables/{}'.format(table), 'GET', print_request=print_request, print_response=print_response)
        if (response.code == 404):
            self.logger.debug("Reference table not found.")
            return False
        elif (response.code >= 400):
            self.logger.error("An error occurred inserting table:")
            self.logger.debug(self.dump_response(response))
            sys.exit(-1)
        return True
    
        
    def bulkLoadReferenceTable(self, table, data, print_request=False, print_response=False):
        tab = 'reference_data/tables/bulk_load/'+table
        headers={"Content-Type": "application/json"}
        response = self.call( tab, "POST", headers=headers, data=data, print_request=print_request, print_response=print_response)
        if (response.code == 409):
            self.logger.debug(self.dump_response(response))
            print("Data already exists, using existing data")
        elif(response.code >= 400):
            print("An error occurred setting up sample data:")
            self.logger.debug(self.dump_response(response))
            sys.exit(1)
        #self.logger.debug(self.dump_response(response))
        return response

    def dump_response(self,response):
        parsed_response = json.loads(response.read().decode('utf-8'))
        return json.dumps(parsed_response, indent=4)
        
    def dump_request(self, client, path, method, headers=None, data=None):
        ip = client.get_server_ip()
        base_uri = client.get_base_uri()
    
        header_copy = client.get_headers().copy()
        if headers is not None:
            header_copy.update(headers)
    
        url = 'https://' + ip + base_uri + path
        print('Sending a ' + method + ' request to:')
        print(url)
        print('with these headers:')
        print(header_copy)
        print()
        print(data)
