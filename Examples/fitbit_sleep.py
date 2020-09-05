import seerpy
from datetime import datetime
from datetime import timedelta
from dateutil import tz

seer_client = seerpy.SeerConnect()

# Access to Fitbit labels is by knowing the name of the corret label group
fitbit_sleep_name = 'Fitbit Sleep Labels'

patients = seer_client.get_patients_dataframe()

# Get the previous week worth of data
today = datetime.utcnow().date()
end_time = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc())
start_time = end_time - timedelta(days=7)

# time stamps are in ms
from_time = start_time.timestamp() * 1000
to_time = end_time.timestamp() * 1000

for n in range(len(patients)):

    patient_id = patients.loc[n]['id']
    patient_email = patients.loc[n]['user.email']

    #    get data for i.e. patient email
    if patient_email == 'EMAIL':

        #        Get available label groups
        diary_data_groups = seer_client.get_diary_data_groups_dataframe(patient_id)

        for group in range(len(diary_data_groups)):
            group_name = diary_data_groups.loc[group]['name']

            #            get the label group for fitbit sleep labels
            if group_name == fitbit_sleep_name:
                group_id = diary_data_groups.loc[group]['id']
                sleep_labels = seer_client.get_diary_data_labels_dataframe(
                    patient_id, group_id, from_time, to_time)
