# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from unittest import mock

import pytest

from seerpy.seerpy import SeerConnect


# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestSeerConnect:

    def test_success(self, seer_auth):
        print("seer_auth", seer_auth)
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        result = SeerConnect()

        assert result.graphqlClient

    def test_login_unauthorized(self, seer_auth):
        print("seer_auth", seer_auth)
        seer_auth.return_value.cookie = None

        # not really desired behaviour, just documenting current behaviour
        with pytest.raises(AttributeError):
            SeerConnect()

    def test_login_error(self, seer_auth):
        print("seer_auth", seer_auth)
        seer_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()
