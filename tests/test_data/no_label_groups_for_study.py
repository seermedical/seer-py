"""
Data for mocking intermediate function calls, as well as expected return values 
when testing the following function: 
- client.get_label_groups_for_study()

For condition in which the study does not contain any label groups.
"""

# Mocked paginated responses on each subsequent call of client.execute_query()
# within the client.get_paginated_response() function that gets called by 
# client.get_label_groups_for_study()
raw_paginated_responses = [
    {
        "study": {
            "id": "study1_id",
            "name": "study1_name",
            "labelGroups": []
        }
    },
]

# Expected return value when calling client.get_label_groups_for_study()
expected_seerpy_response = {
    "id": "study1_id",
    "name": "study1_name",
    "labelGroups": []
}
