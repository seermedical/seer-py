import seerpy

seer_client = seerpy.SeerConnect()

fitbit_step_name = 'Fitbit Activity Steps'
fitbit_heart_rate_name = 'Fitbit Heart Rate'

patients = seer_client.get_patients_dataframe()
patient_ids = patients['id']

for idx, patient_id in enumerate(patient_ids):

    patient_email = patients.loc[idx]['user.email']
    
#    test on i.e. Pip's data
    if patient_email == 'pip@seermedical.com':
        
#        Get available channel groups
        diary_data_channels = seer_client.get_diary_channel_groups_dataframe(patient_id)
        data = seer_client.get_diary_data(diary_data_channels['dataChunks.url'][0])
