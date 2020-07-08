"""
Authenticate a connection by verifying a user's credentials against the auth endpoint.

Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
"""
import datetime
import getpass
import os
import json
import random
import time

import jwt
import requests


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

    def __init__(self, api_key_id, api_key_path, api_url=None):
        """
        Authenticate session using API key

        Parameters
        ----------
        api_key_id : str
            The UUID for a Seer api key
        api_key_path : str
            The path to a Seer api key file
        api_url : str, optional
            Base URL of API endpoint
        """

        super(SeerApiKeyAuth,
              self).__init__(api_url if api_url else 'https://sdk-au.seermedical.com/api')

        self.api_key_id = api_key_id
        # TODO: should we default the path to something like "~/.ssh/seerpy.pem"?
        self.api_key_path = api_key_path
        with open(self.api_key_path, 'r') as api_key_file:
            self.api_key = api_key_file.read()

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
