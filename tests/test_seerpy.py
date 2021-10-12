# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import json
import pathlib
from unittest import mock

import pytest
import pandas as pd

from seerpy import auth
from seerpy.seerpy import SeerConnect
import seerpy.graphql as graphql

from tests.test_data import label_groups_for_study, label_groups_for_studies

# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# not really a problem for these test classes
# pylint:disable=too-few-public-methods

# so that usage of fixtures doesn't get flagged
# pylint: disable=redefined-outer-name

TEST_DATA_DIR = pathlib.Path(__file__).parent / "test_data"

DEFAULT_CONNECTION_PARAMS = {'url': '.'}


@pytest.fixture
def seer_connect():
    return SeerConnect(seer_auth=auth.BaseAuth(api_url=''))


class TestSeerConnect:
    def test_success(self):
        result = SeerConnect(seer_auth=auth.BaseAuth(api_url=''))

        assert result.graphql_client

    @mock.patch.object(auth, 'get_auth', autospec=True)
    def test_login_error(self, get_auth):
        get_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestPassingQueryVariables:
    def test_query_variables_are_passed(self, gql_client, unused_sleep, seer_connect):
        gql_client.return_value.execute.side_effect = [None]

        seer_connect.execute_query("query Q { test { id } }", variable_values={'a': 'b'})

        assert gql_client.return_value.execute.call_count == 1
        assert gql_client.return_value.execute.call_args[1]['variable_values'] == {'a': 'b'}

    def test_query_variables_are_passed_on_initial_failure(self, gql_client, unused_sleep,
                                                           seer_connect):
        gql_client.return_value.execute.side_effect = [Exception('503 Server Error'), None]

        seer_connect.execute_query("query Q { test { id } }", variable_values={'a': 'b'})

        assert gql_client.return_value.execute.call_count == 2
        assert gql_client.return_value.execute.call_args[1]['variable_values'] == {'a': 'b'}


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestPaginatedQuery:
    @classmethod
    def check_paginated_query(cls, seer_connect, function_to_test, function_args=None,
                              function_kwargs=None, expected_result=None, query_response=None):
        """Generic function to test a function which calls Seer_connect.get_paginated_response()."""
        # run test
        with mock.patch.object(SeerConnect, 'get_paginated_response',
                               wraps=seer_connect.get_paginated_response) as paginate:
            if function_args and function_kwargs:
                result = function_to_test(*function_args, **function_kwargs)
            elif function_args:
                result = function_to_test(*function_args)
            else:
                result = function_to_test()

        # check result
        assert paginate.call_args

        query_string = paginate.call_args[0][0]
        assert 'limit' in query_string
        assert 'offset' in query_string

        variable_values = paginate.call_args[0][1]
        missing_keys = [key for key in variable_values.keys() if key not in query_string]
        assert not missing_keys, f'key(s) {missing_keys} not found in query string {query_string}'

        object_path = paginate.call_args[0][3]
        missing_path_items = [
            path_item for path_item in object_path if path_item not in query_string
        ]
        assert not missing_path_items, (
            f'object path item(s) {missing_path_items} not found in query string {query_string}')

        iteration_path = None
        if len(paginate.call_args[0]) > 4:
            iteration_path = paginate.call_args[0][4]
            missing_path_items = [
                path_item for path_item in iteration_path if path_item not in query_string
            ]
            assert not missing_path_items, (f'iteration path item(s) {missing_path_items} not found'
                                            f' in query string {query_string}')

        if query_response:
            expected_result = query_response
            for path_item in object_path:
                expected_result = expected_result[path_item]

            if iteration_path:
                iteration_result = expected_result
                for iteration_path_item in iteration_path:
                    iteration_result = iteration_result[iteration_path_item]
                if not iteration_result:
                    # if this is empty, the expected result is empty
                    expected_result = iteration_result

        if expected_result:
            assert result == expected_result

    @classmethod
    def check_paginated_query_with_data(cls, gql_client, seer_connect, function_to_test,
                                        function_args=None, function_kwargs=None,
                                        response_file=None, response=None, empty_response_file=None,
                                        empty_response=None, expected_result=None):
        # setup
        side_effects = []

        query_response = None
        if response_file:
            with open(TEST_DATA_DIR / response_file, 'r') as f:
                query_response = json.load(f)
            side_effects.append(query_response)
        elif response:
            query_response = response
            side_effects.append(query_response)

        # this is the "no more data" response
        if empty_response_file:
            with open(TEST_DATA_DIR / empty_response_file, 'r') as f:
                side_effects.append(json.load(f))
        elif empty_response:
            side_effects.append(empty_response)

        gql_client.return_value.execute.side_effect = side_effects

        # run test and check result
        cls.check_paginated_query(seer_connect, function_to_test, function_args, function_kwargs,
                                  expected_result, query_response)

    @classmethod
    def check_paginated_query_with_data_variations(cls, gql_client, seer_connect, function_to_test,
                                                   function_args=None, function_kwargs=None,
                                                   response_file=None, response=None,
                                                   empty_response_file=None, empty_response=None,
                                                   expected_result=None):
        # test with only empty response
        if not expected_result:
            cls.check_paginated_query_with_data(gql_client, seer_connect, function_to_test,
                                                function_args, function_kwargs,
                                                response_file=empty_response_file,
                                                response=empty_response)

        cls.check_paginated_query_with_data(gql_client, seer_connect, function_to_test,
                                            function_args, function_kwargs, response_file, response,
                                            empty_response_file, empty_response, expected_result)

    def test_get_studies(self, gql_client, unused_sleep, seer_connect):
        # run test and check result
        self.check_paginated_query_with_data_variations(gql_client, seer_connect,
                                                        function_to_test=seer_connect.get_studies,
                                                        response_file='studies.json',
                                                        empty_response={'studies': []})

    def test_get_studies_by_id(self, gql_client, unused_sleep, seer_connect):
        # run test and check result
        self.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_studies_by_id,
            function_args=[['study-1-id', 'study-2-id']], response_file='studies.json',
            empty_response={'studies': []})


    def test_get_documents_for_studies(self, gql_client, unused_sleep, seer_connect):
        # run test and check result
        self.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_documents_for_studies,
            function_args=[['study-1-id', 'study-2-id']], response_file='study_documents.json',
            empty_response={'studies': []})

    def test_get_diary_insights(self, gql_client, unused_sleep, seer_connect):
        # TODO: I'm uncertain if the values in this file are appropriate

        # run test and check result
        self.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_diary_insights,
            function_args=['patient-1-id'], response_file='diary_insights.json',
            empty_response_file='diary_insights_empty.json')

    def test_get_diary_study_labels(self, gql_client, unused_sleep, seer_connect):
        # TODO: check that the contents of this file are appropriate

        # run test and check result
        self.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_diary_study_labels,
            function_args=['patient-1-id', 'label-group-1-id'], response_file='diary_labels.json',
            empty_response_file='diary_labels_empty.json')

    def test_get_mood_survey_results(self, gql_client, unused_sleep, seer_connect):
        # run test and check result
        self.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_mood_survey_results,
            function_args=[['survey-1-id', 'survey-2-id']],
            response_file='mood_survey_response_1.json', empty_response={'surveys': []})

    def test_get_study_ids_in_study_cohort(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_study_ids_in_study_cohort()
        with open(TEST_DATA_DIR / "study_cohorts.json", "r") as f:
            query_data = json.load(f)
        side_effects.append(query_data)
        expected_result = [study['id'] for study in query_data['studyCohort']['studies']]
        # this is the "no more data" response for get_study_ids_in_study_cohort()
        with open(TEST_DATA_DIR / "study_cohorts_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        # run test and check result
        self.check_paginated_query(seer_connect, seer_connect.get_study_ids_in_study_cohort,
                                   ['study-cohort-1-id'], expected_result=expected_result)

    def test_get_user_ids_in_user_cohort(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_user_ids_in_user_cohort()
        with open(TEST_DATA_DIR / "user_cohorts.json", "r") as f:
            query_data = json.load(f)
        side_effects.append(query_data)
        expected_result = [user['id'] for user in query_data['userCohort']['users']]
        # this is the "no more data" response for get_user_ids_in_user_cohort()
        with open(TEST_DATA_DIR / "user_cohorts_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        # run test and check result
        self.check_paginated_query(seer_connect, seer_connect.get_user_ids_in_user_cohort,
                                   ['study-cohort-1-id'], expected_result=expected_result)


@mock.patch.object(SeerConnect, "get_all_study_metadata_by_ids", autospec=True)
class TestGetAllStudyMetaDataDataframeByIds:
    def test_single_study(self, get_all_metadata, seer_connect):
        # setup
        with open(TEST_DATA_DIR / "study1_metadata.json", "r") as f:
            test_input = json.load(f)
        get_all_metadata.return_value = {'studies': [test_input['study']]}

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_metadata.csv", index_col=0)

        # run test
        result = seer_connect.get_all_study_metadata_dataframe_by_ids()

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_four_studies(self, get_all_metadata, seer_connect):
        # setup
        studies = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(TEST_DATA_DIR / filename, "r") as f:
                studies.append(json.load(f)['study'])

        get_all_metadata.return_value = {'studies': studies}

        expected_result = pd.read_csv(TEST_DATA_DIR / "studies1-4_metadata.csv", index_col=0)

        # run test
        result = seer_connect.get_all_study_metadata_dataframe_by_ids()

        # check result
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetAllStudyMetaDataByNames:
    def test_no_study_param(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_studies()
        with open(TEST_DATA_DIR / "studies.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_studies()
        side_effects.append({'studies': []})

        # these are the calls from the loop in get_all_study_metadata_by_ids()
        expected_results = []
        for i in range(1, 5):
            filename = "study" + str(i) + "_metadata.json"
            with open(TEST_DATA_DIR / filename, "r") as f:
                study = json.load(f)
                side_effects.append(study)
                expected_results.append(study['study'])

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = seer_connect.get_all_study_metadata_by_names()

        # check result
        assert result == {'studies': expected_results}

    def test_existing_study_param(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_studies()
        with open(TEST_DATA_DIR / "studies_filtered1.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for get_studies()
        side_effects.append({'studies': []})

        # these are the calls from the loop in get_all_study_metadata_by_ids()
        expected_results = []
        with open(TEST_DATA_DIR / "study1_metadata.json", "r") as f:
            study = json.load(f)
            side_effects.append(study)
            expected_results = [study['study']]

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = seer_connect.get_all_study_metadata_by_names("Study 1")

        # check result
        assert result == {'studies': expected_results}

    def test_getting_multiple_study_ids_by_name(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_studies()
        with open(TEST_DATA_DIR / "studies_filtered1.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for get_studies()
        side_effects.append({'studies': []})

        with open(TEST_DATA_DIR / "studies_filtered2.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
        # this is the "no more data" response for get_studies()
        side_effects.append({'studies': []})

        # Build up the expected results from file
        expected_results = []
        with open(TEST_DATA_DIR / "study1_metadata.json", "r") as f:
            with open(TEST_DATA_DIR / "study2_metadata.json", "r") as g:
                study1 = json.load(f)
                study2 = json.load(g)
                expected_results = [study1['study']['id'], study2['study']['id']]

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = seer_connect.get_study_ids_from_names(["Study 1", "Study 2"])

        # check result
        assert result == expected_results

    def test_nonexistent_study_param(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        # this is the call in get_studies() when no objects are found
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = seer_connect.get_all_study_metadata_by_names("Study 12")

        # check result
        assert result == {'studies': []}
        # the only call will be in getStudies()
        assert gql_client.return_value.execute.call_count == 1


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetSegmentUrls:
    def test_success(self, gql_client, unused_sleep, seer_connect):
        # setup
        with open(TEST_DATA_DIR / "segment_urls_1.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_1.csv", index_col=0)

        # run test
        result = seer_connect.get_segment_urls(["segment-1-id", "segment-2-id"])

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_multiple_batches(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []
        for file_name in ["segment_urls_1.json", "segment_urls_2.json"]:
            with open(TEST_DATA_DIR / file_name, "r") as f:
                side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_2.csv", index_col=0)

        # run test
        result = seer_connect.get_segment_urls(
            ["segment-1-id", "segment-2-id", "segment-3-id", "segment-4-id"], 2)

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_none_segment_ids(self, unused_gql_client, unused_sleep, seer_connect):
        # setup
        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_empty.csv", index_col=0)

        # run test
        result = seer_connect.get_segment_urls(None)

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_empty_segment_ids(self, unused_gql_client, unused_sleep, seer_connect):
        # gql_client is never called as we don't enter the loop

        # run test
        result = seer_connect.get_segment_urls([])

        # check result
        assert result.empty

    def test_unmatched_segment_ids(self, gql_client, unused_sleep, seer_connect):
        # setup
        with open(TEST_DATA_DIR / "segment_urls_no_match.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        # run test
        result = seer_connect.get_segment_urls(["blah", "blah1"])

        # check result
        assert result.empty


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetLabels:
    def test_success_single_and_empty(self, gql_client, unused_sleep, seer_connect):
        # run test and check result
        TestPaginatedQuery.check_paginated_query_with_data_variations(
            gql_client, seer_connect, function_to_test=seer_connect.get_labels,
            function_args=['study-id-1', 'label-group-id-1'], response_file='labels_1.json',
            empty_response_file='labels_1_empty.json')

    def test_success_multiple(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "labels_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "labels_2.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_labels()
        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        with open(TEST_DATA_DIR / "labels_result.json", "r") as f:
            expected_result = json.load(f)

        # run test
        result = seer_connect.get_labels("study-1-id", "label-group-1-id")

        # check result
        assert result == expected_result


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetLabelsDataframe:
    def test_success(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "labels_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "labels_2.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_labels()
        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "labels_1.csv", index_col=0)

        # run test
        result = seer_connect.get_labels_dataframe("study-1-id", "label-group-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetViewedTimesDataframe:
    def test_success(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "view_groups.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_viewed_times_dataframe()
        with open(TEST_DATA_DIR / "view_groups_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        # need to set parse_dates and float_precision='round_trip' to make the comparison work
        expected_result = pd.read_csv(TEST_DATA_DIR / "views.csv", index_col=0,
                                      parse_dates=['createdAt',
                                                   'updatedAt'], float_precision='round_trip')

        # run test
        result = seer_connect.get_viewed_times_dataframe("study-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetDocumentsForStudiesDataframe:
    def test_success(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "study_documents.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "study_documents_empty.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_documents_for_studies_dataframe()
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # need to set parse_dates and float_precision='round_trip' to make the comparison work
        expected_result = pd.read_csv(TEST_DATA_DIR / "study_documents.csv", index_col=0,
                                      parse_dates=['uploaded'], float_precision='round_trip')
        expected_result['uploaded'] = expected_result['uploaded'].astype(int)

        # run test
        result = seer_connect.get_documents_for_studies_dataframe("study-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result, check_like=True)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetMoodSurveyResults:
    def test_get_results(self, gql_client, unused_sleep, seer_connect):
        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        with open(TEST_DATA_DIR / "mood_survey_results.json", "r") as f:
            expected_result = json.load(f)

        result = seer_connect.get_mood_survey_results(["aMoodSurveyId"])
        assert result == expected_result

    def test_get_results_dataframe(self, gql_client, unused_sleep, seer_connect):
        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "mood_survey_results.csv")

        result = seer_connect.get_mood_survey_results_dataframe(["aMoodSurveyId"])
        pd.testing.assert_frame_equal(result, expected_result)

    def test_get_multiple_results_pages_dataframe(self, gql_client, unused_sleep, seer_connect):
        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_2.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "mood_survey_results_multipage.csv")

        result = seer_connect.get_mood_survey_results_dataframe(["aMoodSurveyId"])
        pd.testing.assert_frame_equal(result, expected_result)

    def test_get_empty_results_dataframe(self, gql_client, unused_sleep, seer_connect):
        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        result = seer_connect.get_mood_survey_results_dataframe(["aMoodSurveyId"])
        assert result.empty


class TestStudyCohorts:
    @mock.patch('time.sleep', return_value=None)
    @mock.patch('seerpy.seerpy.GQLClient', autospec=True)
    def test_get_study_ids_in_study_cohort(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "study_cohorts.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "study_cohorts_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = ['study1', 'study2']

        # run test and check result
        result = seer_connect.get_study_ids_in_study_cohort('cohort1')
        assert result == expected_result

    def test_generating_create_mutation(self):
        query_string = graphql.create_study_cohort_mutation_string('test_cohort',
                                                                   study_ids=['study1', 'study2'])

        assert query_string == """
        mutation {
            createStudyCohort(input: {
                name: "test_cohort", studyIds: ["study1", "study2"]
            }) {
                studyCohort {
                    id
                }
            }
        }
    """

    def test_generating_add_studies_to_cohort_mutation(self):
        query_string = graphql.add_studies_to_study_cohort_mutation_string(
            'cohort_id', ['study1', 'study2'])

        assert query_string == """
        mutation {
            addStudiesToStudyCohort(
                studyCohortId: "cohort_id",
                studyIds: ["study1", "study2"]
            ) {
                studyCohort {
                    id
                }
            }
        }
    """

    def test_generating_remove_studies_from_cohort_mutation(self):
        query_string = graphql.remove_studies_from_study_cohort_mutation_string(
            'cohort_id', ['study1', 'study2'])

        assert query_string == """
        mutation {
            removeStudiesFromStudyCohort(
                studyCohortId: "cohort_id",
                studyIds: ["study1", "study2"]
            ) {
                studyCohort {
                    id
                }
            }
        }
    """


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestUserCohorts:
    def test_get_user_ids_in_user_cohort(self, gql_client, unused_sleep, seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "user_cohorts.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "user_cohorts_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = ['user1', 'user2']

        # run test and check result
        result = seer_connect.get_user_ids_in_user_cohort('cohort1')
        assert result == expected_result

    def test_get_user_ids_in_user_cohort_with_cohort_not_found(self, gql_client, unused_sleep,
                                                               seer_connect):
        # setup
        side_effects = [Exception('NOT_FOUND')]
        gql_client.return_value.execute.side_effect = side_effects

        with pytest.raises(Exception) as ex:
            seer_connect.get_user_ids_in_user_cohort('random-cohort-that-doesnt-exist')
            assert str(ex.value) == 'NOT_FOUND'

    def test_get_user_ids_in_user_cohort_with_no_users(self, gql_client, unused_sleep,
                                                       seer_connect):
        # setup
        side_effects = []

        with open(TEST_DATA_DIR / "user_cohorts_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = []

        # run test and check result
        result = seer_connect.get_user_ids_in_user_cohort('a-missing-cohort')
        assert result == expected_result

    def test_generating_create_user_cohort_mutation(self, unused_gql_client, unused_sleep):
        query_string = graphql.get_create_user_cohort_mutation_string('test_cohort',
                                                                      user_ids=['user1', 'user2'])

        assert query_string == """
        mutation {
            createUserCohort(input: {
                name: "test_cohort", userIds: ["user1", "user2"]
            }) {
                userCohort {
                    id
                }
            }
        }
    """

    def test_generating_add_users_to_cohort_mutation(self, unused_gql_client, unused_sleep):
        query_string = graphql.get_add_users_to_user_cohort_mutation_string(
            'cohort_id', ['user1', 'user2'])

        assert query_string == """
        mutation {
            addUsersToUserCohort(
                userCohortId: "cohort_id",
                userIds: ["user1", "user2"]
            ) {
                userCohort {
                    id
                }
            }
        }
    """

    def test_generating_remove_users_from_cohort_mutation(self, unused_gql_client, unused_sleep):
        query_string = graphql.get_remove_users_from_user_cohort_mutation_string(
            'cohort_id', ['user1', 'user2'])

        assert query_string == """
        mutation {
            removeUsersFromUserCohort(
                userCohortId: "cohort_id",
                userIds: ["user1", "user2"]
            ) {
                userCohort {
                    id
                }
            }
        }
    """


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestGetTagIds:
    def test_success(self, gql_client, unused_sleep, seer_connect):
        # setup
        expected_result = [{
            'category': {
                'description': None,
                'id': 'algorithm',
                'name': 'Algorithm'
            },
            'forDiary': None,
            'forStudy': True,
            'id': 'tag-1-id',
            'value': 'Needs Review'
        }, {
            'category': {
                'description': None,
                'id': 'algorithm',
                'name': 'Algorithm'
            },
            'forDiary': None,
            'forStudy': False,
            'id': 'tag-2-id',
            'value': 'Epileptiform'
        }]
        gql_client.return_value.execute.return_value = {'labelTags': expected_result}

        # run test
        result = seer_connect.get_tag_ids()

        # check result
        assert result == expected_result

    def test_empty(self, gql_client, unused_sleep, seer_connect):
        # setup
        expected_result = []
        gql_client.return_value.execute.return_value = {'labelTags': expected_result}

        # run test
        result = seer_connect.get_tag_ids()

        # check result
        assert result == expected_result


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
class TestLabelGroups:
    def test_get_label_groups_for_study(self, gql_client, unused_sleep, seer_connect):
        raw_paginated_responses = label_groups_for_study.raw_paginated_responses
        expected_seerpy_response = label_groups_for_study.expected_seerpy_response

        gql_client.return_value.execute.side_effect = raw_paginated_responses
        response = seer_connect.get_label_groups_for_study("study1")
        assert response == expected_seerpy_response

    def test_get_label_groups_for_studies(self, gql_client, unused_sleep, seer_connect):
        with mock.patch.object(seer_connect, "get_label_groups_for_study") as mock_stdy_labelgroups:
            mock_stdy_labelgroups.side_effect = label_groups_for_studies.individual_study_responses
            response = seer_connect.get_label_groups_for_studies(["study1","study2"])
        assert label_groups_for_studies.expected_seerpy_response == response

    def test_get_label_groups_for_studies_dataframe(self, gql_client, unused_sleep, seer_connect):
        with mock.patch.object(seer_connect, "get_label_groups_for_study") as mock_stdy_labelgroups:
            mock_stdy_labelgroups.side_effect = label_groups_for_studies.individual_study_responses
            response = seer_connect.get_label_groups_for_studies_dataframe(["study1","study2"])
        assert label_groups_for_studies.expected_seerpy_df.equals(response)
