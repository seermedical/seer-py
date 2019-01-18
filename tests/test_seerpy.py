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

        expected_result = pd.read_csv(test_data_dir / "study1_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(expected_result)

    def test_four_studies(self, seer_connect_init,  # pylint:disable=unused-argument
                          get_all_metadata):

        # setup
        studies = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(test_data_dir / filename, "r") as f:
                studies.append(json.load(f)['study'])

        get_all_metadata.return_value = {'studies': studies}

        expected_result = pd.read_csv(test_data_dir / "studies1-4_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().createMetaData()

        # check result
        assert result.equals(expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetAllMetaData:

    def test_no_study_param(self, seer_auth, gql_client,
                            time_sleep):  # pylint:disable=unused-argument

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

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

    def test_existing_study_param(self, seer_auth, gql_client,
                                  time_sleep):  # pylint:disable=unused-argument

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

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

    def test_nonexistent_study_param(self, seer_auth, gql_client,
                                     time_sleep):  # pylint:disable=unused-argument

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(test_data_dir / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().getAllMetaData("Study 12")

        # check result
        assert result == {'studies' : []}
        # the only call will be in getStudies()
        assert gql_client.return_value.execute.call_count == 2


@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetSegmentUrls:

    def test_success(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        with open(test_data_dir / "segment_urls_1.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        expected_result = pd.read_csv(test_data_dir / "segment_urls_1.csv", index_col=0)

        # run test
        result = SeerConnect().getSegmentUrls(["segment-1-id", "segment-2-id"])

        # check result
        assert result.equals(expected_result)

    @mock.patch('time.sleep', return_value=None)
    def test_multiple_batches(self, time_sleep, seer_auth,  # pylint:disable=unused-argument
                              gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []
        for file_name in ["segment_urls_1.json", "segment_urls_2.json"]:
            with open(test_data_dir / file_name, "r") as f:
                side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(test_data_dir / "segment_urls_2.csv", index_col=0)

        # run test
        result = SeerConnect().getSegmentUrls(["segment-1-id", "segment-2-id",
                                               "segment-3-id", "segment-4-id"], 2)

        # check result
        assert result.equals(expected_result)

    def test_none_segment_ids(self, seer_auth, gql_client):

        # TODO: should we check for none and make it an empty list???

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        error_string = ("object of type 'NoneType' has no len()")
        gql_client.return_value.execute.side_effect = Exception(error_string)

        # run test
        with pytest.raises(Exception) as exception_info:
            SeerConnect().getSegmentUrls(None)

        # check result
        assert str(exception_info.value) == error_string

    def test_empty_segment_ids(self, seer_auth, gql_client):  # pylint:disable=unused-argument

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # gql_client is never called as we don't enter the loop

        # run test
        result = SeerConnect().getSegmentUrls([])

        # check result
        assert result.empty

    def test_unmatched_segment_ids(self, seer_auth, gql_client):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        with open(test_data_dir / "segment_urls_no_match.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        # run test
        result = SeerConnect().getSegmentUrls(["blah", "blah1"])

        # check result
        assert result.empty


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestCreateDataChunkUrls:

    def test_success(self, seer_auth):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(test_data_dir / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(test_data_dir / "segment_urls_3.csv", index_col=0)

        expected_result = pd.read_csv(test_data_dir / "study1_data_chunk_urls.csv", index_col=0)

        # run test
        result = SeerConnect().createDataChunkUrls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_input(self, seer_auth):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(test_data_dir / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(test_data_dir / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().createDataChunkUrls(meta_data, segment_urls)

        print("result", result)

        # check result
        assert result.equals(expected_result)

    def test_empty_metadata(self, seer_auth):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(test_data_dir / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(test_data_dir / "segment_urls_3.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().createDataChunkUrls(meta_data, segment_urls)

        print("result", result)

        # check result
        assert result.equals(expected_result)

    def test_empty_segments_urls(self, seer_auth):

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(test_data_dir / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(test_data_dir / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().createDataChunkUrls(meta_data, segment_urls)

        print("result", result)

        # check result
        assert result.equals(expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetLabels:

    def test_success(self, seer_auth, gql_client, time_sleep):  # pylint:disable=unused-argument

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        with open(test_data_dir / "labels_1.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for getLabels()
        with open(test_data_dir / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(test_data_dir / "labels_1.csv", index_col=0)

        # run test
        result = SeerConnect().getLabels("study-1-id", "label-group-1-id")

        # check result
        assert result.equals(expected_result)
