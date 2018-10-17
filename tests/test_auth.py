# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from unittest import mock

import pytest

from seerpy.auth import SeerAuth


# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use


@mock.patch('seerpy.auth.getpass.getpass', autospec=True, return_value="password")
@mock.patch('builtins.input', autospec=True, return_value="email")
@mock.patch('seerpy.auth.requests.get', autospec=True)
@mock.patch('seerpy.auth.requests.post', autospec=True)
class TestAuth:

    def test_success(self, requests_post, requests_get,
                     email_input, password_getpass):  # pylint:disable=unused-argument
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = 200

        result = SeerAuth("api-url")

        assert result.cookie['seer.sid'] == "cookie"

    def test_401_error(self, requests_post, requests_get,
                       email_input, password_getpass):  # pylint:disable=unused-argument
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = 401

        result = SeerAuth("api-url")

        assert result.cookie is None

    def test_other_error(self, requests_post, requests_get,
                       email_input, password_getpass):  # pylint:disable=unused-argument
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = "undefined"

        with pytest.raises(InterruptedError):
            SeerAuth("api-url")
