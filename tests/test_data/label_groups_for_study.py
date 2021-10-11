
# RAW RESPONSES IT GETS FROM GRAPH QL QUERY
raw_paginated_responses = [
    {
        "study": {
            "id": "study1_id",
            "name": "study1_name",
            "labelGroups": [
                {
                    "name": "labelgroup1_name",
                    "description": "labelgroup1_description",
                    "id": "labelgroup1_id",
                    "labelType": "default",
                    "numberOfLabels": 2,
                },
            ]
        }
    },
    {
        "study": {
            "id": "study1_id",
            "name": "study1_name",
            "labelGroups": [
                {
                    "name": "labelgroup2_name",
                    "description": "labelgroup2_description",
                    "id": "labelgroup2_id",
                    "labelType": "default",
                    "numberOfLabels": 5,
                }
            ]
        }
    },
    {
        "study": {
            "id": "study1_id",
            "name": "study1_name",
            "labelGroups": []
        }
    },
]

expected_seerpy_response = {
    "id": "study1_id",
    "name": "study1_name",
    "labelGroups": [
        {
            "name": "labelgroup1_name",
            "description": "labelgroup1_description",
            "id": "labelgroup1_id",
            "labelType": "default",
            "numberOfLabels": 2,
        },
        {
            "name": "labelgroup2_name",
            "description": "labelgroup2_description",
            "id": "labelgroup2_id",
            "labelType": "default",
            "numberOfLabels": 5,
        }
    ]
}