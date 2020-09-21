from gql import gql

import seerpy.graphql as graphql


def test_graphql_query_strings():
    """Ensure query strings parse correctly."""
    gql(graphql.get_add_labels_mutation_string())
    gql(graphql.get_tag_id_query_string())
    gql(graphql.EDIT_STUDY_LABEL_GROUP)
    gql(graphql.get_organisations_query_string())
    gql(graphql.get_patients_query_string())
    gql(graphql.get_diary_labels_query_string())
    gql(graphql.get_diary_medication_alerts_query_string())
    gql(graphql.get_diary_medication_alert_windows_query_string())
    gql(graphql.get_diary_medication_compliance_query_string())
