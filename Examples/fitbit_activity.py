import seerpy
from datetime import datetime
from datetime import timedelta
from dateutil import tz

seer_client = seerpy.SeerConnect()

fitbit_step_name = 'Fitbit Activity Steps'
fitbit_heart_rate_name = 'Fitbit Heart Rate'

patients = seer_client.get_patients_dataframe()
patient_ids = patients['id']

# Get the previous week worth of data
today = datetime.utcnow().date()
end_time = datetime(today.year, today.month, today.day, tzinfo=tz.tzutc())
start_time = end_time - timedelta(days=7)

# time stamps are in ms
from_time = start_time.timestamp() * 1000
to_time = end_time.timestamp() * 1000

for idx, patient_id in enumerate(patient_ids):

    patient_email = patients.loc[idx]['user.email']

    #    get data for i.e. patient email
    if patient_email == 'EMAIL':

        #        Get available channel groups
        diary_data_channels = seer_client.get_diary_channel_groups_dataframe(
            patient_id, from_time, to_time)

        #        can change to get heart rate or step count here
        segments = diary_data_channels.loc[diary_data_channels['name'] == fitbit_heart_rate_name]
        segments = segments.reset_index(drop=True)

        data = seer_client.get_diary_fitbit_data(segments)
