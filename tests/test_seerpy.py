# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import json
import pathlib
from unittest import mock

import pytest
import pandas as pd

from seerpy.seerpy import SeerConnect
import seerpy.graphql as graphql
from seerpy.auth import DEFAULT_COOKIE_KEY

# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# not really a problem for these test classes
# pylint:disable=too-few-public-methods

TEST_DATA_DIR = pathlib.Path(__file__).parent / "test_data"

DEFAULT_CONNECTION_PARAMS = {'url': '.'}


@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestSeerConnect:
    def test_success(self, seer_auth):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}

        result = SeerConnect()

        assert result.graphql_client

    def test_login_error(self, seer_auth):
        seer_auth.side_effect = InterruptedError('Authentication Failed')

        with pytest.raises(InterruptedError):
            SeerConnect()


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestPassingQueryVariables:
    def test_query_variables_are_passed(self, seer_auth, gql_client, unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        gql_client.return_value.execute.side_effect = [None]

        SeerConnect().execute_query("query Q { test { id } }", variable_values={'a': 'b'})
        assert gql_client.return_value.execute.call_count == 1
        assert gql_client.return_value.execute.call_args[1]['variable_values'] == {'a': 'b'}

    def test_query_variables_are_passed_on_initial_failure(self, seer_auth, gql_client,
                                                           unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        gql_client.return_value.execute.side_effect = [Exception('503 Server Error'), None]

        SeerConnect().execute_query("query Q { test { id } }", variable_values={'a': 'b'})

        assert gql_client.return_value.execute.call_count == 2
        assert gql_client.return_value.execute.call_args[1]['variable_values'] == {'a': 'b'}


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
        pd.testing.assert_frame_equal(result, expected_result)

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
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetAllStudyMetaDataByNames:
    def test_no_study_param(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        # this is the call in get_studies()
        with open(TEST_DATA_DIR / "studies.json", "r") as f:
            side_effects.append({'studies': json.load(f)})
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
        seer_auth.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        # run test
        result = SeerConnect().get_all_study_metadata_by_names()

        # check result
        assert result == {'studies': expected_results}

    def test_existing_study_param(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_all_study_metadata_by_names("Study 1")

        # check result
        assert result == {'studies': expected_results}

    def test_getting_multiple_study_ids_by_name(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_study_ids_from_names(["Study 1", "Study 2"])

        # check result
        assert result == expected_results

    def test_nonexistent_study_param(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        # this is the call in get_studies() when no objects are found
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # run test
        result = SeerConnect().get_all_study_metadata_by_names("Study 12")

        # check result
        assert result == {'studies': []}
        # the only call will be in getStudies()
        assert gql_client.return_value.execute.call_count == 1


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetSegmentUrls:
    def test_success(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        with open(TEST_DATA_DIR / "segment_urls_1.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_1.csv", index_col=0)

        # run test
        result = SeerConnect().get_segment_urls(["segment-1-id", "segment-2-id"])

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_multiple_batches(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []
        for file_name in ["segment_urls_1.json", "segment_urls_2.json"]:
            with open(TEST_DATA_DIR / file_name, "r") as f:
                side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_2.csv", index_col=0)

        # run test
        result = SeerConnect().get_segment_urls(
            ["segment-1-id", "segment-2-id", "segment-3-id", "segment-4-id"], 2)

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_none_segment_ids(self, seer_auth, unused_gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}

        expected_result = pd.read_csv(TEST_DATA_DIR / "segment_urls_empty.csv", index_col=0)

        # run test
        result = SeerConnect().get_segment_urls(None)

        # check result
        pd.testing.assert_frame_equal(result, expected_result)

    def test_empty_segment_ids(self, seer_auth, unused_gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}

        # gql_client is never called as we don't enter the loop

        # run test
        result = SeerConnect().get_segment_urls([])

        # check result
        assert result.empty

    def test_unmatched_segment_ids(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        with open(TEST_DATA_DIR / "segment_urls_no_match.json", "r") as f:
            gql_client.return_value.execute.return_value = json.load(f)

        # run test
        result = SeerConnect().get_segment_urls(["blah", "blah1"])

        # check result
        assert result.empty


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetLabels:
    def test_success_single(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "labels_1.json", "r") as f:
            query_data = json.load(f)
        side_effects.append(query_data)
        # this is the "no more data" response for get_labels()
        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = query_data['study']

        # run test
        result = SeerConnect().get_labels("study-1-id", "label-group-1-id")

        # check result
        assert result == expected_result

    def test_success_multiple(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_labels("study-1-id", "label-group-1-id")

        # check result
        assert result == expected_result

    def test_success_empty(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "labels_1_empty.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = []

        # run test
        result = SeerConnect().get_labels("study-1-id", "label-group-1-id")

        # check result
        assert result == expected_result


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetLabelsDataframe:
    def test_success(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_labels_dataframe("study-1-id", "label-group-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetViewedTimesDataframe:
    def test_success(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_viewed_times_dataframe("study-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetDocumentsForStudiesDataframe:
    def test_success(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "study_documents.json", "r") as f:
            side_effects.append(json.load(f))
        # # this is the "no more data" response for get_documents_for_studies_dataframe()
        with open(TEST_DATA_DIR / "study_documents_empty.json", "r") as f:
            side_effects.append(json.load(f))
        # this is the "no more data" response for get_studies()
        side_effects.append({'studies': []})

        gql_client.return_value.execute.side_effect = side_effects

        # need to set parse_dates and float_precision='round_trip' to make the comparison work
        expected_result = pd.read_csv(TEST_DATA_DIR / "study_documents.csv", index_col=0,
                                      parse_dates=['uploaded'], float_precision='round_trip')
        expected_result['uploaded'] = expected_result['uploaded'].astype(int)

        # run test
        result = SeerConnect().get_documents_for_studies_dataframe("study-1-id")

        # check result
        pd.testing.assert_frame_equal(result, expected_result, check_like=True)


@mock.patch('time.sleep', return_value=None)
@mock.patch('seerpy.seerpy.GQLClient', autospec=True)
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetMoodSurveyResults:
    def test_get_results(self, seer_auth, gql_client, unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        with open(TEST_DATA_DIR / "mood_survey_results.json", "r") as f:
            expected_result = json.load(f)

        result = SeerConnect().get_mood_survey_results(["aMoodSurveyId"])
        assert result == expected_result

    def test_get_results_dataframe(self, seer_auth, gql_client, unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "mood_survey_results.csv")

        result = SeerConnect().get_mood_survey_results_dataframe(["aMoodSurveyId"])
        pd.testing.assert_frame_equal(result, expected_result)

    def test_get_multiple_results_pages_dataframe(self, seer_auth, gql_client, unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_1.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_2.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        expected_result = pd.read_csv(TEST_DATA_DIR / "mood_survey_results_multipage.csv")

        result = SeerConnect().get_mood_survey_results_dataframe(["aMoodSurveyId"])
        pd.testing.assert_frame_equal(result, expected_result)

    def test_get_empty_results_dataframe(self, seer_auth, gql_client, unused_sleep):
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []
        with open(TEST_DATA_DIR / "mood_survey_response_empty.json", "r") as f:
            side_effects.append(json.load(f))
        gql_client.return_value.execute.side_effect = side_effects

        result = SeerConnect().get_mood_survey_results_dataframe(["aMoodSurveyId"])
        assert result.empty


class TestStudyCohorts:
    @mock.patch('time.sleep', return_value=None)
    @mock.patch('seerpy.seerpy.GQLClient', autospec=True)
    @mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
    def test_get_study_ids_in_study_cohort(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "study_cohorts_1_get.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "study_cohorts_2_get.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = ['study1', 'study2']

        # run test and check result
        result = SeerConnect().get_study_ids_in_study_cohort('cohort1')
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
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestUserCohorts:
    def test_get_user_ids_in_user_cohort(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "user_cohorts_1_get.json", "r") as f:
            side_effects.append(json.load(f))
        with open(TEST_DATA_DIR / "user_cohorts_2_get.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = ['user1', 'user2']

        # run test and check result
        result = SeerConnect().get_user_ids_in_user_cohort('cohort1')
        assert result == expected_result

    def test_get_user_ids_in_user_cohort_with_cohort_not_found(self, seer_auth, gql_client,
                                                               unused_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}

        side_effects = [Exception('NOT_FOUND')]
        gql_client.return_value.execute.side_effect = side_effects

        with pytest.raises(Exception) as ex:
            SeerConnect().get_user_ids_in_user_cohort('random-cohort-that-doesnt-exist')
            assert str(ex.value) == 'NOT_FOUND'

    def test_get_user_ids_in_user_cohort_with_no_users(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {'seer.sid': "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        side_effects = []

        with open(TEST_DATA_DIR / "user_cohorts_2_get.json", "r") as f:
            side_effects.append(json.load(f))

        gql_client.return_value.execute.side_effect = side_effects

        expected_result = []

        # run test and check result
        result = SeerConnect().get_user_ids_in_user_cohort('a-missing-cohort')
        assert result == expected_result

    def test_generating_create_user_cohort_mutation(self, unused_seer_auth, unused_gql_client,
                                                    unused_sleep):
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

    def test_generating_add_users_to_cohort_mutation(self, unused_seer_auth, unused_gql_client,
                                                     unused_sleep):
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

    def test_generating_remove_users_from_cohort_mutation(self, unused_seer_auth, unused_gql_client,
                                                          unused_sleep):
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
@mock.patch('seerpy.seerpy.SeerAuth', autospec=True)
class TestGetTagIds:
    def test_success(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

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
        result = SeerConnect().get_tag_ids()

        # check result
        assert result == expected_result

    def test_empty(self, seer_auth, gql_client, unused_sleep):
        # setup
        seer_auth.return_value.cookie = {DEFAULT_COOKIE_KEY: "cookie"}
        seer_auth.return_value.get_connection_parameters.return_value = DEFAULT_CONNECTION_PARAMS

        expected_result = []
        gql_client.return_value.execute.return_value = {'labelTags': expected_result}

        # run test
        result = SeerConnect().get_tag_ids()

        # check result
        assert result == expected_result
