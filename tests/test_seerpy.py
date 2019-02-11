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

# not really a problem for these test classes
# pylint:disable=too-few-public-methods


TEST_DATA_DIR = pathlib.Path(__file__).parent / "test_data"


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestSeerConnect:

    def test_success(self, seer_auth):
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        result = SeerConnect()

        assert result.graphql_client

    def test_login_unauthorized(self, seer_auth):
        seer_auth.return_value.cookie = None

        # not really desired behaviour, just documenting current behaviour
        with pytest.raises(AttributeError):
            SeerConnect()

    def test_login_error(self, seer_auth):
        seer_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()


@mock.patch.object(SeerConnect, "get_all_study_metadata_by_ids", autospec=True)
@mock.patch.object(SeerConnect, "__init__", autospec=True, return_value=None)
class TestGetAllStudyMetaDataDataframeByIds:

    # as we don't rely on anything in __init() I have mocked it for simplicity

    def test_single_study(self, unused_seer_connect_init, get_all_metadata):
        # setup
        with open(TEST_DATA_DIR / "study1_metadata.json", "r") as f:
            test_input = json.load(f)
        get_all_metadata.return_value = {'studies': [test_input['study']]}

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().get_all_study_metadata_dataframe_by_ids()

        # check result
        assert result.equals(expected_result)

    def test_four_studies(self, unused_seer_connect_init, get_all_metadata):
        # setup
        studies = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(TEST_DATA_DIR / filename, "r") as f:
                studies.append(json.load(f)['study'])

        get_all_metadata.return_value = {'studies': studies}

        expected_result = pd.read_csv(TEST_DATA_DIR / "studies1-4_metadata.csv", index_col=0)

        # run test
        result = SeerConnect().get_all_study_metadata_dataframe_by_ids()

        # check result
        assert result.equals(expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetAllStudyMetaDataByNames:

    def test_no_study_param(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(TEST_DATA_DIR / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

        # these are the calls from the loop in getAllMetaData()
        expected_results = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(TEST_DATA_DIR / filename, "r") as f:
                study = json.load(f)
                side_effects.append(study)
                expected_results.append(study['study'])

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().get_all_study_metadata_by_names()

        # check result
        assert result == {'studies' : expected_results}

    def test_existing_study_param(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(TEST_DATA_DIR / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

        # these are the calls from the loop in getAllMetaData()
        expected_results = []
        with open(TEST_DATA_DIR / "study1_metadata.json", "r") as f:
            study = json.load(f)
            side_effects.append(study)
            expected_results = [study['study']]

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().get_all_study_metadata_by_names("Study 1")

        # check result
        assert result == {'studies' : expected_results}

    def test_nonexistent_study_param(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        # this is the call in getStudies()
        with open(TEST_DATA_DIR / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for getStudies()
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().get_all_study_metadata_by_names("Study 12")

        # check result
        assert result == {'studies' : []}
        # the only call will be in getStudies()
        assert gql_client.return_value.execute.call_count == 2


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetSegmentUrls:

    def test_success(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        with open(TEST_DATA_DIR / "segment_urls_1.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_1.csv", index_col=0)

        # run test
        result = SeerConnect().get_segment_urls(["segment-1-id", "segment-2-id"])

        # check result
        assert result.equals(expected_result)

    def test_multiple_batches(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []
        for file_name in ["segment_urls_1.json", "segment_urls_2.json"]:
            with open(TEST_DATA_DIR / file_name, "r") as f:
                side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_2.csv", index_col=0)

        # run test
        result = SeerConnect().get_segment_urls(["segment-1-id", "segment-2-id", "segment-3-id",
                                                 "segment-4-id"], 2)

        # check result
        assert result.equals(expected_result)

    def test_none_segment_ids(self, seer_auth, gql_client, unused_time_sleep):
        # TODO: should we check for none and make it an empty list???

        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        error_string = ("object of type 'NoneType' has no len()")
        gql_client.return_value.execute.side_effect = Exception(error_string)

        # run test
        with pytest.raises(Exception) as exception_info:
            SeerConnect().get_segment_urls(None)

        # check result
        assert str(exception_info.value) == error_string

    def test_empty_segment_ids(self, seer_auth, unused_gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # gql_client is never called as we don't enter the loop

        # run test
        result = SeerConnect().get_segment_urls([])

        # check result
        assert result.empty

    def test_unmatched_segment_ids(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        with open(TEST_DATA_DIR / "segment_urls_no_match.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        # run test
        result = SeerConnect().get_segment_urls(["blah", "blah1"])

        # check result
        assert result.empty


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestCreateDataChunkUrls:

    def test_success(self, seer_auth):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_data_chunk_urls.csv", index_col=0)

        # run test
        result = SeerConnect().create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_input(self, seer_auth):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_metadata(self, seer_auth):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_segments_urls(self, seer_auth):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = SeerConnect().create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetLabels:

    def test_success(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        with open(TEST_DATA_DIR / "labels_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "labels_2.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for getLabels()
        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        with open(TEST_DATA_DIR / "labels_result.json", "r") as f:
            expected_result = json.load(f)

        # run test
        result = SeerConnect().get_labels("study-1-id", "label-group-1-id")

        # check result
        assert result == expected_result


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetLabelsDataframe:

    def test_success(self, seer_auth, gql_client, unused_time_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = []

        with open(TEST_DATA_DIR / "labels_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "labels_2.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for getLabels()
        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "labels_1.csv", index_col=0)

        # run test
        result = SeerConnect().get_labels_dataframe("study-1-id", "label-group-1-id")

        # check result
        assert result.equals(expected_result)
