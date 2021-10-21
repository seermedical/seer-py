"""
Data for mocking intermediate function calls, as well as expected return values 
when testing the following functions: 
- client.get_label_groups_for_studies()
- client.get_label_groups_for_studies_dataframe()

For condition in which the studies do not contain any label groups.
"""
import io
import pandas as pd

# Individual responses it gets from calling client.get_label_groups_for_study()
individual_study_responses = [
    {
        "id": "study1_id",
        "name": "study1_name",
        "labelGroups": []
    },
    {
        "id": "study2_id",
        "name": "study2_name",
        "labelGroups": []
    }
]

# The expected result from client.get_label_groups_for_studies()
expected_seerpy_response = [
    {
        "id": "study1_id",
        "name": "study1_name",
        "labelGroups": []
    },
    {
        "id": "study2_id",
        "name": "study2_name",
        "labelGroups": []
    },
]

# The expected result from client.get_label_groups_for_studies_dataframe()
expected_seerpy_df = pd.DataFrame([])
