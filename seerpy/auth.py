"""
Authenticate a connection by verifying a user's credentials against the auth endpoint.

Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
"""
from collections import namedtuple
import datetime
import getpass
from glob import glob
import os
import json
import random
import time

import jwt
import requests

KeyFileInfo = namedtuple('KeyFileInfo', ['key_path', 'key_id', 'region', 'default'])


def get_auth(api_key_id=None, api_key_path=None, region=None, api_url=None, seer_auth=None,
             use_email=None, email=None, password=None):
    """
    Get the correct Auth implemetation based on passed parameters and the existence of config files.

    If seer_auth is passed, it will be used and other parameters will be ignored.
    If use_email is True or email or password are passed, email authentication will be used.
    If no API key or id are passed and no API key files are found, email authentication will be
    used.
    If nothing is passed and API key files exist in ~/.seerpy matching the pattern seerpy*.pem then
    API key authentiction will be used.
    API key files can contain the following information, separated by the '.' character:
    - a default indicator 'default' to indicate to use this file as a default
    - a region string 'au' or 'uk' indicating the region to use
    - an id, to be used as the api_key_id value.
    e.g. seerpy.default.ac42d1f7-98c5-40ad-b35f-2026688411e8.uk.pem

    If api_key_id is passed but not api_key_path, a file matching that id will be used if found.
    If api_key_path is passed but not api_key_id, the api_key_path name will be parsed for an id.

    Parameters
    ----------
    api_key_id : str, optional
        The id of the API key
    api_key_path : str, optional
        The file path of an API key file
    region : {None, 'au', 'uk'}
        The region string of the API version to access.
    api_url : str, optional
        Base URL of API endpoint
    seer_auth: BaseAuth or child, optional
        An Auth instance to use. Will be used and other parameters ignored if passed
    use_email : bool, optional
        Whether to use email authentication. Will override non-email options if True
    email : str, optional
        The email address for a user's Seer account
    password : str, optional
        The password for a user's Seer account
    """

    if seer_auth:
        return seer_auth

    home_dir = os.path.expanduser("~")
    pem_files = glob(os.path.join(home_dir, '.seerpy', 'seerpy*.pem'))

    # don't treat a use_email of None as significant
    if ((use_email is True)
            or (use_email is None and
                (email or password or not (api_key_id or api_key_path or pem_files)))):
        return SeerAuth(api_url, email, password)

    return SeerApiKeyAuth(api_key_id, api_key_path, region, api_url)


# pylint: disable=no-self-use
class BaseAuth:
    """
    An authenticated connection to the Seer API. Should not be used directly,
    instead use one of the deriving classes.
    """
    def __init__(self, api_url):
        self.api_url = api_url

    def get_connection_parameters(self, party_id=None):
        url_suffix = '?partyId=' + party_id if party_id else ''

        return {
            'url': self.api_url + '/graphql' + url_suffix,
            'headers': self.get_headers(),
            'use_json': True,
            'timeout': 30
        }

    def handle_query_error_pre_sleep(self, ex):  # pylint: disable=unused-argument
        return True

    def handle_query_error_post_sleep(self, ex):
        pass

    def get_headers(self):
        return {}


class SeerAuth(BaseAuth):
    """
    Creates an authenticated connection to the Seer API. This is the default for most use cases.
    """

    default_cookie_key = 'seer.sid'
    help_message_displayed = False

    def __init__(self, api_url=None, email=None, password=None, cookie_key=default_cookie_key,
                 credential_namespace='cookie'):
        """
        Authenticate session using email address and password

        Parameters
        ----------
        api_url : str, optional
            Base URL of API endpoint
        email : str, optional
            The email address for a user's Seer account
        password : str, optional
            The password for a user's Seer account
        cookie_key : str, optional
            ?
        credential_namespace : str, optional
            ?
        """

        super(SeerAuth, self).__init__(api_url if api_url else 'https://api.seermedical.com/api')

        self.cookie = None
        self.cookie_key = cookie_key
        self.credential_namespace = credential_namespace
        self._read_cookie()

        self.email = email
        self.password = password
        self._attempt_login()

    def get_headers(self):
        return {'Cookie': f'{self.cookie_key}={self.cookie[self.cookie_key]}'}

    def handle_query_error_pre_sleep(self, ex):
        if 'NOT_AUTHENTICATED' in str(ex):
            self.logout()
            return False
        return True

    def handle_query_error_post_sleep(self, ex):
        if 'NOT_AUTHENTICATED' in str(ex):
            self.login()

    def login(self):
        if not self.email or not self.password:
            self._login_details()
        body = {'email': self.email, 'password': self.password}
        login_url = self.api_url + '/auth/login'
        response = requests.post(url=login_url, data=body)
        print("login status_code", response.status_code)
        if (response.status_code == requests.codes.ok  # pylint: disable=maybe-no-member
                and response.cookies):

            seer_sid = response.cookies.get(self.cookie_key, False)
            self.cookie = {}
            self.cookie[self.cookie_key] = seer_sid

        else:
            self.cookie = None

    def logout(self):
        home = os.path.expanduser('~')
        cookie_file = home + self._get_cookie_path()
        if os.path.isfile(cookie_file):
            os.remove(cookie_file)
        self.cookie = None

    def _attempt_login(self):
        response = self._verify_login()
        if response == requests.codes.ok:  # pylint: disable=maybe-no-member
            print('Login Successful')
            return

        allowed_attempts = 4
        for i in range(allowed_attempts):
            self.login()
            response = self._verify_login()

            if response == requests.codes.ok:  # pylint: disable=maybe-no-member
                print('Login Successful')
                return

            if i >= allowed_attempts - 1:
                print('Login failed. please check your username and password or go to',
                      'app.seermedical.com to reset your password')
                raise InterruptedError('Authentication Failed')

            if response == 401:
                print('\nLogin error, please re-enter your email and password: \n')
                self.cookie = None
                self.password = None
            else:
                # Sleep for ~5, 20, 60 seconds, with jitter to avoid thundering heard problem
                sleep_time = 5 + 5 * i + 11 * i**2 + random.uniform(0, 5)
                print(f'\nLogin failed, retrying in {sleep_time:.0f} seconds...')
                time.sleep(sleep_time)

    def _verify_login(self):
        """
        Attempt to verify user by making a GET request with the current session cookie.
        If successful, save cookie to disk. Returns response status code.
        """
        if self.cookie is None:
            return 401

        verify_url = self.api_url + '/auth/verify'
        response = requests.get(url=verify_url, cookies=self.cookie)
        if response.status_code != requests.codes.ok:  # pylint: disable=maybe-no-member
            print("api verify call returned", response.status_code, "status code")
            return response.status_code

        json_response = response.json()
        if not json_response or not json_response['session'] == "active":
            print("api verify call did not return an active session")
            return 440

        self._write_cookie()
        return response.status_code

    def _login_details(self):
        """
        Get user's email address and password, either from file or stdin.
        """
        home = os.path.expanduser('~')
        pswdfile = home + '/.seerpy/credentials'
        if os.path.isfile(pswdfile):
            with open(pswdfile, 'r') as f:
                lines = f.readlines()
                self.email = lines[0].rstrip()
                self.password = lines[1].rstrip()
        else:
            self.email = input('Email Address: ')
            self.password = getpass.getpass('Password: ')
            if not self.help_message_displayed:
                print(f"\nHint: To skip this in future, save your details to {pswdfile}")
                print("See README.md - 'Authenticating' for details\n")
                self.help_message_displayed = True

    def _get_cookie_path(self):
        """Get the path to the local cookie file"""
        return f'/.seerpy/{self.credential_namespace}'

    def _write_cookie(self):
        """Save the current cookie to file"""
        try:
            home = os.path.expanduser('~')
            cookie_file = home + self._get_cookie_path()
            if not os.path.isdir(home + '/.seerpy'):
                os.mkdir(home + '/.seerpy')
            with open(cookie_file, 'w') as f:
                f.write(json.dumps(self.cookie))
        except Exception:  # pylint:disable=broad-except
            pass

    def _read_cookie(self):
        """Read the latest cookie saved to file"""
        home = os.path.expanduser('~')
        cookie_file = home + self._get_cookie_path()
        if os.path.isfile(cookie_file):
            with open(cookie_file, 'r') as f:
                self.cookie = json.loads(f.read().strip())


class SeerApiKeyAuth(BaseAuth):
    """
    Creates an authenticated connection to the Seer API using an API key. This will become the
    default for most use cases.
    """
    def __init__(self, api_key_id, api_key_path=None, region='au', api_key=None, api_url=None):
        """
        Authenticate session using API key

        Parameters
        ----------
        api_key_id : str
            The UUID for a Seer api key
        api_key_path : str, optional
            The path to a Seer api key file
        api_key : str, optional
            The actual api key string - for the case where you can't use a file e.g. in AWS Lambda
        api_url : str, optional
            Base URL of API endpoint
        """

        if api_key:
            self.api_key = api_key
            if not (api_key_id and (region or api_url)):
                raise ValueError('api_key_id and region or api_url must be provided with api_key')
        else:
            api_key_id, api_key_path, region = self._get_parameters(api_key_id, api_key_path,
                                                                    region)

        if not api_url:
            api_url = f'https://sdk-{region}.seermedical.com/api'

        super(SeerApiKeyAuth, self).__init__(api_url)

        self.api_key_id = api_key_id
        if not api_key:
            with open(api_key_path, 'r') as api_key_file:
                self.api_key = api_key_file.read()

    def _get_parameters(self, api_key_id, api_key_path, region):
        home_dir = os.path.expanduser("~")
        pem_files = glob(os.path.join(home_dir, '.seerpy', 'seerpy*.pem'))

        if not api_key_path and not pem_files:
            raise ValueError('No API key file available')

        api_key_file = None
        if api_key_path:
            api_key_file = self._get_key_filename_parts(api_key_path)
        else:
            api_key_files = [self._get_key_filename_parts(pem_file) for pem_file in pem_files]
            if len(api_key_files) == 1:
                api_key_file = api_key_files[0]

        if api_key_id and not api_key_file:
            # choose the pem file which matches id if found
            matching_files = self._get_objects_matching_value(api_key_files, 'key_id', api_key_id)
            if not matching_files:
                raise ValueError('No API key file matches the API key id provided')
            if len(matching_files) == 1:
                api_key_file = matching_files[0]
            else:
                # if there's more than one, choose the one that matches the region
                matching_files = self._get_objects_matching_value(matching_files, 'region', region)
                if len(matching_files) == 1:
                    api_key_file = matching_files[0]
                else:
                    raise ValueError('Multiple API key files match the API key id provided')

        if not api_key_file:
            # choose file with default in the name
            default_files = self._get_objects_matching_value(api_key_files, 'default', 'default')
            if len(default_files) == 1:
                api_key_file = default_files[0]
            else:
                # choose the file that matches the region (from default files if any, else from all)
                list_to_pick_from = api_key_files
                if default_files:
                    list_to_pick_from = default_files
                region_files = self._get_objects_matching_value(list_to_pick_from, 'region', region)
                if len(region_files) == 1:
                    api_key_file = region_files[0]
                elif default_files:
                    raise ValueError('Multiple default API key files found')
                else:
                    raise ValueError('No default API key file found')

        if not api_key_id:
            if not api_key_file.key_id:
                raise ValueError('No API key id found in key file name')
            api_key_id = api_key_file.key_id

        if not api_key_path:
            api_key_path = api_key_file.key_path

        if (not region or region == 'au') and api_key_file.region:
            region = api_key_file.region

        return (api_key_id, api_key_path, region)

    @classmethod
    def _get_key_filename_parts(cls, pem_filename):
        filename_parts = os.path.splitext(os.path.basename(pem_filename))[0].split('.')[1:]
        filename_region = cls._get_part_from_filename_parts(filename_parts, ['au', 'uk'], 'region')
        filename_default = cls._get_part_from_filename_parts(filename_parts, ['default'],
                                                             'default indicator')
        filename_id = None
        if len(filename_parts) == 1:
            filename_id = filename_parts[0]

        return KeyFileInfo(pem_filename, filename_id, filename_region, filename_default)

    @staticmethod
    def _get_part_from_filename_parts(filename_parts, possible_values, part_title):
        parts = [part for part in filename_parts if part in possible_values]
        if not parts:
            return None
        if len(parts) > 1:
            raise ValueError(f'Multiple {part_title}s found in key file name')
        part = parts[0]
        filename_parts = filename_parts.remove(part)
        return part

    def _get_objects_matching_value(self, objects, attr_name, value):
        return [obj for obj in objects if getattr(obj, attr_name) == value]

    def get_headers(self):
        timestamp = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
        payload = {'keyId': self.api_key_id, 'iat': timestamp}
        token = jwt.encode(payload, self.api_key, algorithm='RS256')
        return {"Authorization": "Bearer " + token.decode('utf-8')}

    def handle_query_error_pre_sleep(self, ex):
        if 'NOT_AUTHENTICATED' in str(ex):
            raise ex

        return True


class SeerDevAuth(SeerAuth):
    """
    Creates an auth instance for connecting to dev servers, based on the default
    SeerAuth authentication approach.
    """
    def __init__(self, api_url, email=None, password=None):
        super(SeerDevAuth, self).__init__(api_url, email, password, cookie_key='seerdev.sid',
                                          credential_namespace='cookie-dev')
