"""
Data for mocking intermediate function calls, as well as expected return values 
when testing the following functions: 
- client.get_label_groups_for_studies()
- client.get_label_groups_for_studies_dataframe()
"""
import io
import pandas as pd

# Individual responses it gets from calling client.get_label_groups_for_study()
individual_study_responses = [
    {
        "id": "study1_id",
        "name": "study1_name",
        "labelGroups": [
            {
                "name": "study1_labelgroup1_name",
                "description": "study1_labelgroup1_description",
                "id": "study1_labelgroup1_id",
                "numberOfLabels": 101,
            },
            {
                "name": "study1_labelgroup2_name",
                "description": "study1_labelgroup2_description",
                "id": "study1_labelgroup2_id",
                "numberOfLabels": 102,
            }
        ]
    },
    {
        "id": "study2_id",
        "name": "study2_name",
        "labelGroups": [
            {
                "name": "study2_labelgroup1_name",
                "description": "study2_labelgroup1_description",
                "id": "study2_labelgroup1_id",
                "numberOfLabels": 201,
            },
            {
                "name": "study2_labelgroup2_name",
                "description": "study2_labelgroup2_description",
                "id": "study2_labelgroup2_id",
                "numberOfLabels": 202,
            }
        ]
    },
]

# The expected result from client.get_label_groups_for_studies()
expected_seerpy_response = [
    {
        "id": "study1_id",
        "name": "study1_name",
        "labelGroups": [
            {
                "name": "study1_labelgroup1_name",
                "description": "study1_labelgroup1_description",
                "id": "study1_labelgroup1_id",
                "numberOfLabels": 101,
            },
            {
                "name": "study1_labelgroup2_name",
                "description": "study1_labelgroup2_description",
                "id": "study1_labelgroup2_id",
                "numberOfLabels": 102,
            }
        ]
    },
    {
        "id": "study2_id",
        "name": "study2_name",
        "labelGroups": [
            {
                "name": "study2_labelgroup1_name",
                "description": "study2_labelgroup1_description",
                "id": "study2_labelgroup1_id",
                "numberOfLabels": 201,
            },
            {
                "name": "study2_labelgroup2_name",
                "description": "study2_labelgroup2_description",
                "id": "study2_labelgroup2_id",
                "numberOfLabels": 202,
            }
        ]
    },
]

csv = """
labelGroup.id,labelGroup.name,labelGroup.description,labelGroup.numberOfLabels,id,name
study1_labelgroup1_id,study1_labelgroup1_name,study1_labelgroup1_description,101,study1_id,study1_name
study1_labelgroup2_id,study1_labelgroup2_name,study1_labelgroup2_description,102,study1_id,study1_name
study2_labelgroup1_id,study2_labelgroup1_name,study2_labelgroup1_description,201,study2_id,study2_name
study2_labelgroup2_id,study2_labelgroup2_name,study2_labelgroup2_description,202,study2_id,study2_name
"""

# The expected result from client.get_label_groups_for_studies_dataframe()
expected_seerpy_df = pd.read_csv(io.StringIO(csv))
