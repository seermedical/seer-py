# Example code to download all diary data for patients with N_EVENTS or more events recorded

import seerpy

N_EVENTS = 10

seer_client = seerpy.SeerConnect()

patients = seer_client.get_patients_dataframe() #party_id can be blank to return all results

for n in range(len(patients)):

    patient_id = patients.loc[n]['id']
    
    diary_df = seer_client.get_diary_labels_dataframe(patient_id)
    
#    Limit label groups to people with an epilepsy diary (where the default labelType is seizure)
    if 'labelType' in diary_df.columns and not diary_df.loc[diary_df['labelType'] == 'seizure'].empty:
        
        nEvents = diary_df.loc[diary_df['labelType'] == 'seizure'].iloc[0]['numberOfLabels']
        
        if nEvents >= N_EVENTS:
#            only save the events of interest
            events = diary_df.loc[diary_df['labelType'] == 'seizure']
            mdict = {'data': events}
            savename = 'Patient '  + str(n)
            print('saving ' + savename + '...')
            events.to_excel(savename + '.xlsx')
