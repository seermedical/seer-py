# Example code to download all diary data for patients with N_EVENTS or more events recorded

import seerpy

N_EVENTS = 10

seer_client = seerpy.SeerConnect()

# party_id can be blank to return all results
patients = seer_client.get_patients_dataframe()
patient_ids = patients['id']

for patient_id in patient_ids:

    diary_df = seer_client.get_diary_labels_dataframe(patient_id)

#    Limit label groups to people with an epilepsy diary (where the default labelType is seizure)
    if diary_df is not None and \
        'labelType' in diary_df.columns \
            and not diary_df.loc[diary_df['labelType'] == 'seizure'].empty:

        n_events = diary_df.loc[diary_df['labelType']
                                == 'seizure'].iloc[0]['numberOfLabels']

        if n_events >= N_EVENTS:
            #            only save the events of interest
            events = diary_df.loc[diary_df['labelType'] == 'seizure']
            mdict = {'data': events}
            savename = 'P_' + patient_id.replace('-', '')
            print('saving ' + savename + '...')
            events.to_excel(savename + '.xlsx')
