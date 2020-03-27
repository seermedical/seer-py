"""
Authenticate a connection by verifying a user's ID against the auth endpoint.

Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
"""
import getpass
import json
import os

import requests

COOKIE_KEY_PROD = 'seer.sid'
COOKIE_KEY_DEV = 'seerdev.sid'


class SeerAuth:
    """Class to handle authenticating a user"""

    help_message_displayed = False
    seerpy_dir = f"{os.path.expanduser('~')}/.seerpy"
    pswdfile = f"{seerpy_dir}/credentials"

    def __init__(self, api_url: str, email: str = None, password: str = None, dev: bool = False):
        """
        Authenticate session using email address and password

        Parameters
        ----------
        api_url: The base URL of the API endpoint
        email: The email address for a user's https://app.seermedical.com account
        password: The password for a user's https://app.seermedical.com account
        dev: Flag to query the dev rather than production endpoint
        """
        self.api_url = api_url
        self.cookie = None
        self.dev = dev

        self.read_cookie()
        if self.verify_login() == 200:
            print('Login Successful')
            return

        self.email = email
        self.password = password
        allowed_attempts = 3

        for i in range(allowed_attempts):
            if not self.email or not self.password:
                self.get_login_details()
            self.login()
            response = self.verify_login()
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

    def login(self) -> None:
        """
        Request and save a session cookie from the auth endpoint, by making a
        POST request with a user's email address and password.
        """
        login_url = self.api_url + '/auth/login'
        body = {'email': self.email, 'password': self.password}
        response = requests.post(url=login_url, data=body)
        print("Login status code:", response.status_code)
        # pylint: disable=maybe-no-member
        if (response.status_code == requests.codes.ok and response.cookies):
            seer_sid = response.cookies.get(COOKIE_KEY_PROD, False)
            seerdev_sid = response.cookies.get(COOKIE_KEY_DEV, False)

            self.cookie = {}
            if seer_sid:
                self.cookie[COOKIE_KEY_PROD] = seer_sid
            elif seerdev_sid:
                self.cookie[COOKIE_KEY_DEV] = seerdev_sid
        else:
            self.cookie = None

    def verify_login(self) -> int:
        """
        Attempt to verify user by making a GET request with the current session cookie.
        If successful, save cookie to disk. Returns response status code.
        """
        if self.cookie is None:
            return 401

        verify_url = self.api_url + '/auth/verify'
        response = requests.get(url=verify_url, cookies=self.cookie)
        if response.status_code != requests.codes.ok:  # pylint: disable=maybe-no-member
            print(f"API verify call returned {response.status_code} status code")
            return 401

        json_response = response.json()
        if not json_response or not json_response['session'] == "active":
            print("API verify call did not return an active session")
            return 401

        self.write_cookie()
        return response.status_code

    def get_login_details(self) -> None:
        """
        Read in user's email address and password, either from file or stdin.
        """
        if os.path.isfile(self.pswdfile):
            with open(self.pswdfile, 'r') as f:
                lines = f.readlines()
                self.email = lines[0].rstrip('\n')
                self.password = lines[1].rstrip('\n')
        else:
            self.email = input('Email Address: ')
            self.password = getpass.getpass('Password: ')
            if not self.help_message_displayed:
                print(f"\nHint: To skip this in future, save your details to {self.pswdfile}")
                print("See README.md - 'Authenticating' for details\n")
                self.help_message_displayed = True

    def get_cookie_path(self) -> str:
        """Get the path to the local cookie file"""
        return f"{self.seerpy_dir}/cookie-dev" if self.dev else f"{self.seerpy_dir}/cookie"

    def write_cookie(self) -> None:
        """Save the current cookie to file"""
        try:
            cookie_file = self.get_cookie_path()
            if not os.path.isdir(self.seerpy_dir):
                os.mkdir(self.seerpy_dir)
            with open(cookie_file, 'w') as f:
                f.write(json.dumps(self.cookie))
        except Exception:  # pylint:disable=broad-except
            pass

    def read_cookie(self) -> None:
        """Read the latest cookie saved to file"""
        cookie_file = self.get_cookie_path()
        if os.path.isfile(cookie_file):
            with open(cookie_file, 'r') as f:
                self.cookie = json.loads(f.read().strip())

    def destroy_cookie(self) -> None:
        """Delete any saved cookie"""
        cookie_file = self.get_cookie_path()
        if os.path.isfile(cookie_file):
            os.remove(cookie_file)
        self.cookie = None
