# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
import requests
import getpass


class seerAuth:

    def __init__(self, apiUrl):
        self.apiUrl = apiUrl
        allowedAttempts = 3
        for i in range(allowedAttempts):
            self.loginDetails()
            self.login()
            response = self.verifyLogin()
            if response == 200:
                print('Login Successful')
                break
            elif response==401:
                print('\nhttp error, please check your url: \n')
                print(response)
                self.cookie = None
                break
            elif i < allowedAttempts-1:
                print('\nLogin error, please re-enter your email and password: \n')
                print(response)
                self.cookie = None
            else:
                print('Login failed. please go to www.seermedical.com to reset your password')
                raise InterruptedError('Authentication Failed')
                self.cookie = None
    

    def login(self):
        apiUrl = self.apiUrl + '/api/auth/login'
        body = {'email': self.email, 'password': self.password}
        r = requests.post(url=apiUrl, data=body)
        self.cookie = {'seer.sid' : r.cookies['seer.sid']}
    
    def verifyLogin(self):
        apiUrl = self.apiUrl + '/api/auth/verify'
        r = requests.get(url=apiUrl, cookies=self.cookie)
        return r.status_code
    
    def loginDetails(self):
        self.email = input('Email Adddress: ')
        self.password = getpass.getpass('Password: ')

