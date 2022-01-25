"""Script to download patient medication adherence for a specified date range.

To run this script:
-------------------
1. Save this script in a folder, or folder location you wish to save data to.
2. Open up your terminal.
3. Navigate to the folder you have saved this script.
4. Run
    ```
    python medication_compliance_downloader.py
    ```
The default start date is 2021-01-01 and default end date is today's date. If you
wish to specify a date range, you can run, e.g.
    ```
    python medication_compliance_downloader.py -s 2021-05-05 -e 2021-05-10
    ```
N.b. The date format is YYYY-MM-DD.
If you wish to specify a path to save the date, you can run, e.g.
```
python medication_compliance_downloader.py -s 2021-05-05 -e 2021-05-10 -o '/path/to/directory'
```

N.b. This script is set to retrieving 200 patients by default.
"""
import os
import argparse
from datetime import datetime, timedelta
import pandas as pd

from seerpy import SeerConnect

# TODO: Add organisation ID support
# N.b. If no users have medication alerts set up for a portion of the date range,
# no data will be displayed for that date range.
# Pagination for the purpose of running on large numbers of patients has not been implemented.


def run(client=SeerConnect(), start_date='', end_date='', max_items=None, organisation_id=None,
        out_dir=''):

    # Get date range
    now = datetime.now()
    if not end_date:
        end_date = now.strftime("%Y-%m-%d")
    if not start_date:
        start_date = (now - timedelta(hours=24 * 77)).strftime("%Y-%m-%d")
    end_date_text = ''.join(s for s in end_date.split('-'))
    start_date_text = ''.join(s for s in start_date.split('-'))

    date_range = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d")
    date_range_text = f'{start_date_text}-{end_date_text}'

    # Get list of patient IDs and user information
    patients_list = client.get_patients(party_id=organisation_id, limit=200, max_items=max_items)

    # Make directory and subdirectory for output CSVs
    out_subdir = os.path.join(out_dir, 'Medication Adherence Per Patient')
    os.makedirs(out_subdir, exist_ok=True)

    all_data = pd.DataFrame(date_range, columns=['date'])
    for patient in patients_list:
        user_name = patient['user']['fullName']
        medication_adherence_response = client.execute_query(
            query_string=GET_MEDICATION_ADHERENCE_QUERY, variable_values={
                'patientId': patient['id'],
                'startDate': start_date,
                'endDate': end_date
            })
        # Check if data exists
        if not medication_adherence_response['patient']['diary']:
            print(f'No data in this date range for {user_name}')
        medication_adherence_data = medication_adherence_response['patient']['diary'][
            'medicationAdherences']
        if not medication_adherence_data:
            continue
        data = pd.DataFrame(medication_adherence_data).explode('medications', ignore_index=True)
        data.dropna(subset=['medications'], inplace=True)
        if data['medications'].isnull().all():
            print(f'No data for {user_name} in date range {start_date} to {end_date}')
            continue

        # Format data
        medication_data = pd.json_normalize(data['medications'])

        # Select just dates
        data = data[['date']].reset_index(drop=True)
        # Merge dates, medication, and medication adherence into pd.DataFrame
        data = pd.merge(data, medication_data, left_index=True, right_index=True)
        # Save individual data
        data.to_csv(os.path.join(out_subdir, f'{date_range_text}.csv'))

        for date in data['date'].unique():
            statuses = data[data['date'] == date]['status'].unique().tolist()
            if all(status in ['not_logged'] for status in statuses):
                status_category = 'Not logged'
            elif all(status in ['taken_as_scheduled', 'taken_as_needed'] for status in statuses):
                status_category = 'Taken'
            else:
                status_category = 'Partial'
            all_data.loc[(all_data['date'] == date), [user_name]] = status_category

        # Save all data
        all_data.to_csv(os.path.join(out_dir, f'{date_range_text}_all.csv'))
    print('Done.')
    return


GET_MEDICATION_ADHERENCE_QUERY = """query getMedicationCompliance(
        $patientId: String!,
        $startDate: Date!,
        $endDate: Date!
    ) {
  patient (id: $patientId ) {
    id
    diary {
        medicationAdherences (range: { fromDate: $startDate toDate: $endDate }) {
        date
        medications {
        drugName
        status
        }
      }
    }
  }
}
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--organisation-id", help="Organisation ID.")
    parser.add_argument(
        "-s", "--startdate", default='',
        help="The start date to retrieve medication compliance. Format: YYYY-MM-DD.")
    parser.add_argument("-e", "--enddate", default='',
                        help="The end date to retrieve medication compliance. Format: YYYY-MM-DD.")
    parser.add_argument("-o", "--outpath", default='',
                        help="The path to where data is to be saved.")
    args = parser.parse_args()

    run(start_date=args.startdate, end_date=args.enddate, out_dir=args.outpath)
