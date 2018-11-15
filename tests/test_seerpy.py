# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import json
import pathlib
from unittest import mock

import pytest
import pandas as pd

from seerpy.seerpy import SeerConnect


# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

test_data_dir = pathlib.Path(__file__).parent / "test_data"


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestSeerConnect:

    def test_success(self, seer_auth):
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        result = SeerConnect()

        assert result.graphqlClient

    def test_login_unauthorized(self, seer_auth):
        seer_auth.return_value.cookie = None

        # not really desired behaviour, just documenting current behaviour
        with pytest.raises(AttributeError):
            SeerConnect()

    def test_login_error(self, seer_auth):
        seer_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()


@mock.patch.object(SeerConnect, "getAllMetaData", autospec=True)
@mock.patch.object(SeerConnect, "__init__", autospec=True, return_value=None)
class TestCreateMetaData:

    # as we don't rely on anything in __init() I have mocked it for simplicity

    def test_single_study(self, seer_connect_init,  # pylint:disable=unused-argument
                          get_all_metadata):

        # setup
        with open(test_data_dir / "study1_metadata.json", "r") as f:
            test_input = json.load(f)
        get_all_metadata.return_value = {'studies': [test_input['study']]}

        test_result = pd.read_csv(test_data_dir / "study1_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(test_result)

    def test_four_studies(self, seer_connect_init,  # pylint:disable=unused-argument
                          get_all_metadata):

        # setup
        studies = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(test_data_dir / filename, "r") as f:
                studies.append(json.load(f)['study'])

        get_all_metadata.return_value = {'studies': studies}

        test_result = pd.read_csv(test_data_dir / "studies1-4_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(test_result)


@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetAllMetaData:

    def test_no_study_param(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})

        # these are the calls from the loop in getAllMetaData()
        expected_results = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(test_data_dir / filename, "r") as f:
                study = json.load(f)
                side_effects.append(study)
                expected_results.append(study['study'])

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().getAllMetaData()

        # check result
        assert result == {'studies' : expected_results}

    def test_existing_study_param(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})

        # these are the calls from the loop in getAllMetaData()
        expected_results = []
        with open(test_data_dir / "study1_metadata.json", "r") as f:
            study = json.load(f)
            side_effects.append(study)
            expected_results = [study['study']]

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().getAllMetaData("Study 1")

        # check result
        assert result == {'studies' : expected_results}

    def test_nonexistent_study_param(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            gql_client.return_value.execute.return_value = {'studies': json.load(f)}

        # run test
        result = SeerConnect().getAllMetaData("Study 12")

        # check result
        assert result == {'studies' : []}
        # the only call will be in getStudies()
        assert gql_client.return_value.execute.call_count == 1


@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetDataChunks:

    def test_success(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        with open(test_data_dir / "study1_data_chunks_1_1.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        test_result = pd.read_csv(test_data_dir / "study1_data_chunks_1_1.csv", index_col=0)

        # run test
        result = SeerConnect().getDataChunks("study-1-id", "study-1-channel-group-1-id",
                                             1526275675734.375, 1526275776671.875)

        # check result
        assert result.equals(test_result)

    def test_no_chunks_returned(self, seer_auth, gql_client):

        # we don't explicitly handle an exception based on a study not found
        # but this is probably the correct action

        # the same would be true with other query methods

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        error_string = ("{'errorCode': 'NOT_FOUND', 'locations': [{'column': 2, 'line': 1}], "
                        "'message': 'Study does not exist', 'path': ['study'], 'statusCode': 404}")
        gql_client.return_value.execute.side_effect = Exception(error_string)

        # run test
        with pytest.raises(Exception) as exception_info:
            SeerConnect().getDataChunks("study", "channel-group-id",1, 1)

        # check result
        assert str(exception_info.value) == error_string


# test getLinks
