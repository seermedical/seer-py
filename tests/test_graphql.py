from gql import gql

import seerpy.graphql as graphql


def test_graphql_query_string():
    """Ensure query strings parse correctly."""

    # TODO: it would be good if all queries were iterable so we could do this in a loop, either by
    # looping through all module variables if possible (or all CAPS), or having them in a class
    # the main advantage being we wouldn't miss any then.

    gql(graphql.GET_STUDY_WITH_DATA)
    gql(graphql.GET_LABELS_PAGED)
    gql(graphql.GET_LABELS_STRING)
    gql(graphql.GET_LABEL_GROUPS_FOR_STUDY_IDS_PAGED)
    gql(graphql.GET_STUDIES_BY_SEARCH_TERM_PAGED)
    gql(graphql.GET_STUDIES_BY_STUDY_ID_PAGED)
    gql(graphql.ADD_LABELS)
    gql(graphql.GET_TAG_IDS)
    gql(graphql.EDIT_STUDY_LABEL_GROUP)
    gql(graphql.GET_ORGANISATIONS)
    gql(graphql.GET_PATIENTS)
    gql(graphql.GET_DIARY_INSIGHTS_PAGED)
    gql(graphql.GET_DIARY_LABELS)
    gql(graphql.GET_DIARY_MEDICATION_ALERTS)
    gql(graphql.GET_DIARY_MEDICATION_ALERT_WINDOWS)
    gql(graphql.GET_DIARY_MEDICATION_COMPLIANCE)
    gql(graphql.GET_DOCUMENTS_FOR_STUDY_IDS_PAGED)
    gql(graphql.GET_LABELS_FOR_DIARY_STUDY_PAGED)
    gql(graphql.GET_STUDY_IDS_IN_STUDY_COHORT_PAGED)
    gql(graphql.GET_MOOD_SURVEY_RESULTS_PAGED)
    gql(graphql.GET_USER_IDS_IN_USER_COHORT_PAGED)
