# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
import requests
import getpass
import os

class SeerAuth:

    def __init__(self, apiUrl, email=None, password=None):
        self.apiUrl = apiUrl
        self.email = email
        self.password = password
        allowedAttempts = 3
        for i in range(allowedAttempts):
            if self.email is None or self.password is None:
                self.loginDetails()
            self.login()
            response = self.verifyLogin()
            if response == 200:
                print('Login Successful')
                break
            elif i < allowedAttempts-1:
                print('\nLogin error, please re-enter your email and password: \n')
                self.cookie = None
                self.password = None
            else:
                print('Login failed. please check your username and password or go to app.seermedical.com to reset your password')
                raise InterruptedError('Authentication Failed')
                self.cookie = None
                self.password = None

    def login(self):
        apiUrl = self.apiUrl + '/api/auth/login'
        body = {'email': self.email, 'password': self.password}
        r = requests.post(url=apiUrl, data=body)
        if r.cookies is not None:
            self.cookie = {'seer.sid' : r.cookies['seer.sid']}
        else:
            self.cookie = None

    def verifyLogin(self):
        if self.cookie is None:
            return 401
        else:
            apiUrl = self.apiUrl + '/api/auth/verify'
            r = requests.get(url=apiUrl, cookies=self.cookie)
            return r.status_code

    def loginDetails(self):
        home = os.environ['HOME'] if 'HOME' in os.environ else '~'
        pswdfile = home + '/.seerpy/credentials'
        if os.path.isfile(pswdfile):
            f=open(pswdfile, 'r')
            lines=f.readlines()
            self.email=lines[0][:-1]
            self.password=lines[1][:-1]
            f.close()
        else:
            self.email = input('Email Address: ')
            self.password = getpass.getpass('Password: ')
