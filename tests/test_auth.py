# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from unittest import mock

import pytest

from seerpy.auth import BaseAuth, SeerAuth

# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# this isn't a useful check for test code
# pylint:disable=too-many-arguments


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.auth.getpass.getpass', autospec=True, return_value="password")
@mock.patch('builtins.input', autospec=True, return_value="email")
@mock.patch('seerpy.auth.requests.get', autospec=True)
@mock.patch('seerpy.auth.requests.post', autospec=True)
class TestAuth:

    # if there is an existing cookie then readCookie will interfere with the test
    @mock.patch.object(SeerAuth, "_read_cookie", autospec=True)
    def test_success(self, unused_read_cookie, requests_post, requests_get, unused_email_input,
                     unused_password_getpass, unused_sleep):
        requests_post.return_value.status_code = 200
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = 200
        requests_get.return_value.json.return_value = {"session": "active"}

        result = SeerAuth("api-url")

        assert result.cookie['seer.sid'] == "cookie"
        unused_sleep.assert_not_called()

    def test_401_error(self, requests_post, requests_get, unused_email_input,
                       unused_password_getpass, unused_sleep):
        requests_post.return_value.status_code = 200
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = 401

        with pytest.raises(InterruptedError):
            SeerAuth("api-url")
        unused_sleep.assert_not_called()

    def test_other_error(self, requests_post, requests_get, unused_email_input,
                         unused_password_getpass, unused_sleep):
        requests_post.return_value.status_code = 200
        requests_post.return_value.cookies = {'seer.sid': "cookie"}
        requests_get.return_value.status_code = "undefined"

        with pytest.raises(InterruptedError):
            SeerAuth("api-url")
        assert unused_sleep.call_count == 3

class TestBaseAuth:
    def test_get_connection_parameters_with_party_id(self):
        auth = BaseAuth('abcd')
        params = auth.get_connection_parameters('1234')
        assert params['url'] == 'abcd/graphql?partyId=1234'

    def test_correct_parameters_are_returned(self):
        auth = BaseAuth('abcd')
        params = auth.get_connection_parameters()

        assert params == {'url': 'abcd/graphql', 'headers': {}, 'use_json': True, 'timeout': 30}
