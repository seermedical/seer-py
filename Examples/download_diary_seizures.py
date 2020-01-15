import seerpy

seer_client = seerpy.SeerConnect()

patients = seer_client.get_patients_dataframe() #party_id can be blank to return all results
for n in range(len(patients)):

    patient_id = patients.loc[n]['id']
    patient_email = patients.loc[n]['user.email']
    
    if patient_email == 'pip@seermedical.com':
        print(patient_email)
        diary_df = seer_client.get_diary_labels_dataframe(patient_id)
        
#        if 'labelType' in diary_df.columns and not diary_df.loc[diary_df['labelType'] == 'seizure'].empty:
#            nEvents = diary_df.loc[diary_df['labelType'] == 'seizure'].iloc[0]['numberOfLabels']
#            if nEvents >= 10:
#                events = diary_df.loc[diary_df['labelType'] == 'seizure']
#                mdict = {'data': events}
#                savename = 'Patient '  + str(n)
#                print('saving ' + savename + '...')
#                events.to_excel(savename + '.xlsx')
