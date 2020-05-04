# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import getpass
import os
import json

import requests

DEFAULT_COOKIE_KEY = 'seer.sid'


class BaseAuth:
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

    def login(self):
        pass

    def logout(self):
        pass

    def get_headers(self):
        return {}


class SeerAuth(BaseAuth):
    """
    Creates a default connection factory, which should be used
    for most API use cases.
    """

    help_message_displayed = False

    def __init__(
            self,
            api_url,
            email=None,
            password=None,
            region='',
            cookie_key=DEFAULT_COOKIE_KEY,
            credential_namespace='cookie'):

        # default to no region unless specified
        use_region = region if len(region) > 0 and region.lower() != 'au' else ''

        super(SeerAuth, self).__init__(
            api_url if api_url is not None else f"https://api{use_region}.seermedical.com/api"
        )

        self.cookie = None
        self.cookie_key = cookie_key
        self.credential_namespace = credential_namespace
        self.__read_cookie()

        self.email = email
        self.password = password
        self.__attempt_login()

    def get_connection_parameters(self, party_id=None):
        return super().get_connection_parameters(party_id=party_id)

    def get_headers(self):
        cookie = self.cookie
        return {
            'Cookie': f'{self.cookie_key}={cookie[self.cookie_key]}'
        }

    def login(self):
        login_url = self.api_url + '/auth/login'
        body = {'email': self.email, 'password': self.password}
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
        cookie_file = home + self.__get_cookie_path()
        if os.path.isfile(cookie_file):
            os.remove(cookie_file)
        self.cookie = None

    def __attempt_login(self):
        if self.__verify_login() == 200:
            print('Login Successful')
            return

        allowed_attempts = 3

        for i in range(allowed_attempts):
            if not self.email or not self.password:
                self.__login_details()
            self.login()
            response = self.__verify_login()

            if response == requests.codes.ok:  # pylint: disable=maybe-no-member
                print('Login Successful')
                break

            if i < allowed_attempts - 1:
                print('\nLogin error, please re-enter your email and password: \n')
                self.cookie = None
                self.password = None
            else:
                print('Login failed. please check your username and password or go to',
                      'app.seermedical.com to reset your password')
                self.cookie = None
                self.password = None
                raise InterruptedError('Authentication Failed')

    def __verify_login(self):
        if self.cookie is None:
            return 401

        verify_url = self.api_url + '/auth/verify'
        response = requests.get(url=verify_url, cookies=self.cookie)
        if response.status_code != requests.codes.ok:  # pylint: disable=maybe-no-member
            print("api verify call returned",
                  response.status_code, "status code")
            return 401

        json_response = response.json()
        if not json_response or not json_response['session'] == "active":
            print("api verify call did not return an active session")
            return 401

        self.__write_cookie()
        return response.status_code

    def __login_details(self):
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

    def __get_cookie_path(self):
        return f'/.seerpy/${self.credential_namespace}'

    def __write_cookie(self):
        try:
            home = os.path.expanduser('~')
            cookie_file = home + self.__get_cookie_path()
            if not os.path.isdir(home + '/.seerpy'):
                os.mkdir(home + '/.seerpy')
            with open(cookie_file, 'w') as f:
                f.write(json.dumps(self.cookie))
        except Exception:  # pylint:disable=broad-except
            pass

    def __read_cookie(self):
        home = os.path.expanduser('~')
        cookie_file = home + self.__get_cookie_path()
        if os.path.isfile(cookie_file):
            with open(cookie_file, 'r') as f:
                self.cookie = json.loads(f.read().strip())


class SeerDevAuth(SeerAuth):
    """
    Creates an auth instance for connecting to dev servers, based on the default
    SeerAuth authentication approach.
    """

    def __init__(self, api_url, email=None, password=None):
        super(SeerDevAuth, self).__init__(
            api_url,
            email,
            password,
            cookie_key='seerdev.sid',
            credential_namespace='cookie-dev')
