# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import getpass
import os
import json

import requests


class SeerAuth:

    def __init__(self, api_url, email=None, password=None, dev=False):
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
                self.login_details()
            self.login()
            response = self.verify_login()
            if response == requests.codes.ok:  # pylint: disable=maybe-no-member
                print('Login Successful')
                break
            elif i < allowed_attempts - 1:
                print('\nLogin error, please re-enter your email and password: \n')
                self.cookie = None
                self.password = None
            else:
                print('Login failed. please check your username and password or go to',
                      'app.seermedical.com to reset your password')
                self.cookie = None
                self.password = None
                raise InterruptedError('Authentication Failed')

    def login(self):
        login_url = self.api_url + '/auth/login'
        body = {'email': self.email, 'password': self.password}
        response = requests.post(url=login_url, data=body)
        print("login status_code", response.status_code)
        if (response.status_code == requests.codes.ok  # pylint: disable=maybe-no-member
                and response.cookies):

            seer_sid = response.cookies.get('seer.sid', False)
            seerdev_sid = response.cookies.get('seerdev.sid', False)

            self.cookie = {}
            if seer_sid:
                self.cookie['seer.sid'] = seer_sid
            if seerdev_sid:
                self.cookie['seerdev.sid'] = seerdev_sid

        else:
            self.cookie = None

    def verify_login(self):
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

        self.write_cookie()
        return response.status_code

    def login_details(self):
        home = os.path.expanduser('~')
        pswdfile = home + '/.seerpy/credentials'
        if os.path.isfile(pswdfile):
            with open(pswdfile, 'r') as f:
                lines = f.readlines()
                self.email = lines[0][:-1]
                self.password = lines[1][:-1]
        else:
            self.email = input('Email Address: ')
            self.password = getpass.getpass('Password: ')

    def get_cookie_path(self):
        return '/.seerpy/cookie-dev' if self.dev else '/.seerpy/cookie'

    def write_cookie(self):
        try:
            home = os.path.expanduser('~')
            cookie_file = home + self.get_cookie_path()
            if not os.path.isdir(home + '/.seerpy'):
                os.mkdir(home + '/.seerpy')
            with open(cookie_file, 'w') as f:
                f.write(json.dumps(self.cookie))
        except Exception:  # pylint:disable=broad-except
            pass

    def read_cookie(self):
        home = os.path.expanduser('~')
        cookie_file = home + self.get_cookie_path()
        if os.path.isfile(cookie_file):
            with open(cookie_file, 'r') as f:
                self.cookie = json.loads(f.read().strip())

    def destroy_cookie(self):
        home = os.path.expanduser('~')
        cookie_file = home + self.get_cookie_path()
        if os.path.isfile(cookie_file):
            os.remove(cookie_file)
        self.cookie = None
