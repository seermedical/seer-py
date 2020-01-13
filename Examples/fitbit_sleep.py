import seerpy

seer_client = seerpy.SeerConnect()

# Access to Fitbit labels is by knowing the name of the corret label group
fitbit_sleep_name = 'Fitbit Sleep Labels'

patients = seer_client.get_patients_dataframe()

for n in range(len(patients)):

    patient_id = patients.loc[n]['id']
    patient_email = patients.loc[n]['user.email']
    
#    test on i.e. Pip's data
    if patient_email == 'pip@seermedical.com':
        
#        Get available label groups
        diary_study_groups = seer_client.get_diary_study_label_groups_dataframe(patient_id)
        
        for group in range(len(diary_study_groups)):
            group_name = diary_study_groups.loc[group]['name']
    
#            get the label group for fitbit sleep labels
            if  group_name == fitbit_sleep_name:
                label_group_id = diary_study_groups.loc[group]['id']
                sleep_labels = seer_client.get_diary_study_labels_dataframe(patient_id, label_group_id)