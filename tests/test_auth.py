# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from unittest import mock
from unittest.mock import mock_open

import pytest

from seerpy import auth
from seerpy.auth import BaseAuth, SeerAuth, SeerApiKeyAuth

# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# this isn't a useful check for test code
# pylint:disable=too-many-arguments


@mock.patch('seerpy.auth.glob', autospec=True)
class TestGetAuth:
    def test_auth_provided(self, mock_glob):
        # setup
        mock_glob.return_value = []
        expected_return = BaseAuth('api_url')

        # run test
        result = auth.get_auth(seer_auth=expected_return)

        # check result
        assert result == expected_return

    @mock.patch.object(SeerAuth, '__init__', autospec=True, return_value=None)
    def test_use_email(self, seer_auth_init, mock_glob):
        # setup
        mock_glob.return_value = ['seerpy.pem']

        # run test
        result = auth.get_auth(api_key_id='api_key_id', api_key_path='api_key_path', use_email=True)

        # check result
        assert isinstance(result, SeerAuth)
        seer_auth_init.assert_called_once_with(mock.ANY, None, None, None)

    @mock.patch.object(SeerAuth, '__init__', autospec=True, return_value=None)
    def test_email_provided(self, seer_auth_init, mock_glob):
        # setup
        mock_glob.return_value = []

        # run test
        result = auth.get_auth(api_key_id='api_key_id', api_key_path='api_key_path', use_email=None,
                               email='email', password='password')

        # check result
        assert isinstance(result, SeerAuth)
        seer_auth_init.assert_called_once_with(mock.ANY, None, 'email', 'password')

    @mock.patch.object(SeerAuth, '__init__', autospec=True, return_value=None)
    def test_no_pem_files(self, seer_auth_init, mock_glob):
        # setup
        mock_glob.return_value = []

        # run test
        result = auth.get_auth()

        # check result
        assert isinstance(result, SeerAuth)
        seer_auth_init.assert_called_once_with(mock.ANY, None, None, None)

    @mock.patch.object(SeerApiKeyAuth, '__init__', autospec=True, return_value=None)
    def test_pem_files_exist(self, seer_key_auth_init, mock_glob):
        # setup
        mock_glob.return_value = ['seerpy.pem']

        # run test and check result
        result = auth.get_auth()

        # check result
        assert isinstance(result, SeerApiKeyAuth)
        seer_key_auth_init.assert_called_once_with(mock.ANY, None, None, None, None)

    def test_email_false(self, mock_glob):
        # setup
        mock_glob.return_value = []

        # run test and check result
        with pytest.raises(ValueError, match='No API key file available'):
            auth.get_auth(use_email=False, email='email', password='password')


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
        requests_post.return_value.cookies = {SeerAuth.default_cookie_key: "cookie"}
        requests_get.return_value.status_code = 200
        requests_get.return_value.json.return_value = {"session": "active"}

        result = SeerAuth("api-url")

        assert result.cookie[SeerAuth.default_cookie_key] == "cookie"
        unused_sleep.assert_not_called()

    def test_401_error(self, requests_post, requests_get, unused_email_input,
                       unused_password_getpass, unused_sleep):
        requests_post.return_value.status_code = 200
        requests_post.return_value.cookies = {SeerAuth.default_cookie_key: "cookie"}
        requests_get.return_value.status_code = 401

        with pytest.raises(InterruptedError):
            SeerAuth("api-url")
        unused_sleep.assert_not_called()

    def test_other_error(self, requests_post, requests_get, unused_email_input,
                         unused_password_getpass, unused_sleep):
        requests_post.return_value.status_code = 200
        requests_post.return_value.cookies = {SeerAuth.default_cookie_key: "cookie"}
        requests_get.return_value.status_code = "undefined"

        with pytest.raises(InterruptedError):
            SeerAuth("api-url")
        assert unused_sleep.call_count == 3


class TestBaseAuth:
    def test_get_connection_parameters_with_party_id(self):
        base_auth = BaseAuth('abcd')
        params = base_auth.get_connection_parameters('1234')
        assert params['url'] == 'abcd/graphql?partyId=1234'

    def test_correct_parameters_are_returned(self):
        base_auth = BaseAuth('abcd')
        params = base_auth.get_connection_parameters()

        assert params == {'url': 'abcd/graphql', 'headers': {}, 'use_json': True, 'timeout': 30}


@mock.patch('builtins.open', new_callable=mock_open, read_data='1234')
@mock.patch('seerpy.auth.glob', autospec=True)
class TestSeerApiKeyAuth:
    @mock.patch('jwt.encode', autospec=True, return_value="an_encoded_key".encode('utf-8'))
    def test_get_connection_parameters(self, unused_jwt_encode, unused_glob, open_mock):

        apikey_auth = SeerApiKeyAuth(api_key_id='id', api_key_path='path/seerpy.pem')
        params = apikey_auth.get_connection_parameters()

        assert params == {
            'url': 'https://sdk-au.seermedical.com/api/graphql',
            'headers': {
                'Authorization': 'Bearer an_encoded_key'
            },
            'use_json': True,
            'timeout': 30
        }
        open_mock.assert_called_with('path/seerpy.pem', 'r')

    @mock.patch('jwt.encode', autospec=True, return_value="an_unencoded_key")
    def test_get_connection_parameters_no_decode(self, unused_jwt_encode, unused_glob, open_mock):

        apikey_auth = SeerApiKeyAuth(api_key_id='id', api_key_path='path/seerpy.pem')
        params = apikey_auth.get_connection_parameters()

        assert params == {
            'url': 'https://sdk-au.seermedical.com/api/graphql',
            'headers': {
                'Authorization': 'Bearer an_unencoded_key'
            },
            'use_json': True,
            'timeout': 30
        }
        open_mock.assert_called_with('path/seerpy.pem', 'r')

    def test_no_files(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = []

        # run test and check result
        with pytest.raises(ValueError, match='No API key file available'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_id_and_key(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = []

        # run test
        result = SeerApiKeyAuth(api_key_id='id', api_key_path='path/seerpy.pem')

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_key_path_but_no_id(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = []

        # run test and check result
        with pytest.raises(ValueError, match='No API key id found in key file name'):
            SeerApiKeyAuth(api_key_id=None, api_key_path='path')

        open_mock.assert_not_called()

    def test_key_file_but_no_id(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem']

        # run test and check result
        with pytest.raises(ValueError, match='No API key id found in key file name'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_key_file_with_id(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.id.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_default_key_file_but_no_id(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.default.pem']

        # run test and check result
        with pytest.raises(ValueError, match='No API key id found in key file name'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_default_key_file_with_id(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.default.id.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.default.id.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_multiple_default_key_files(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.default.pem', 'path/seerpy.default.id.pem']

        # run test and check result
        with pytest.raises(ValueError, match='Multiple default API key files found'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_multiple_default_key_files_with_matching_region(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.default.pem', 'path/seerpy.default.id.au.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.default.id.au.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_multiple_key_files_with_matching_region(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.id.au.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.au.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_key_file_with_id_and_region(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.id.uk.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.uk.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-uk.seermedical.com/api'

    def test_multiple_key_files_with_no_match(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.id.pem']

        # run test and check result
        with pytest.raises(ValueError, match='No default API key file found'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_key_path_with_id_and_region(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.au.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id=None, api_key_path='path/seerpy.id.uk.pem')

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.uk.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-uk.seermedical.com/api'

    def test_id_with_single_file(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id='id', api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_id_with_no_matching_file(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.id2.uk.pem']

        # run test and check result
        with pytest.raises(ValueError, match='No API key file matches the API key id provided'):
            SeerApiKeyAuth(api_key_id='id', api_key_path=None)

        open_mock.assert_not_called()

    def test_id_with_matching_file(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.pem', 'path/seerpy.id.uk.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id='id', api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.uk.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-uk.seermedical.com/api'

    def test_id_with_multiple_matching_files_with_region(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.id.au.pem', 'path/seerpy.id.uk.pem']

        # run test
        result = SeerApiKeyAuth(api_key_id='id', api_key_path=None)

        # check result
        assert result.api_key_id == 'id'
        open_mock.assert_called_with('path/seerpy.id.au.pem', 'r')
        assert result.api_key == '1234'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'

    def test_id_with_multiple_matching_files(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.id.pem', 'path/seerpy.id.uk.pem']

        # run test and check result
        with pytest.raises(ValueError,
                           match='Multiple API key files match the API key id provided'):
            SeerApiKeyAuth(api_key_id='id', api_key_path=None)

        open_mock.assert_not_called()

    def test_multiple_regions(self, mock_glob, open_mock):
        # setup
        mock_glob.return_value = ['path/seerpy.id.pem', 'path/seerpy.id.uk.au.pem']

        # run test and check result
        with pytest.raises(ValueError, match='Multiple regions found in key file name'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None)

        open_mock.assert_not_called()

    def test_key(self, mock_glob, open_mock):
        # run test
        result = SeerApiKeyAuth(api_key_id='id', api_key_path=None, api_key='key')

        # check result
        assert result.api_key_id == 'id'
        assert result.api_key == 'key'
        assert result.api_url == 'https://sdk-au.seermedical.com/api'
        open_mock.assert_not_called()
        mock_glob.assert_not_called()

    def test_key_no_id(self, unused_glob, open_mock):
        # run test and check result
        with pytest.raises(ValueError,
                           match='api_key_id and region or api_url must be provided with api_key'):
            SeerApiKeyAuth(api_key_id=None, api_key_path=None, api_key='key')

        open_mock.assert_not_called()

    def test_key_no_region_or_url(self, unused_glob, open_mock):
        # run test and check result
        with pytest.raises(ValueError,
                           match='api_key_id and region or api_url must be provided with api_key'):
            SeerApiKeyAuth(api_key_id='id', api_key_path=None, region=None, api_key='key',
                           api_url=None)

        open_mock.assert_not_called()

    @mock.patch.dict('os.environ', { 'SDK_API_BASE_URL': 'https://random-sdk.url' })
    @mock.patch('jwt.encode', autospec=True, return_value="an_encoded_key".encode('utf-8'))
    def test_api_url_read_from_env_var(self, unused_jwt_encode, mock_glob, open_mock):
        apikey_auth = SeerApiKeyAuth(api_key_id='id', api_key_path=None, api_key='key')
        params = apikey_auth.get_connection_parameters()

        assert apikey_auth.api_url == 'https://random-sdk.url'
        assert params['url'] == 'https://random-sdk.url/graphql'
        open_mock.assert_not_called()
        mock_glob.assert_not_called()
