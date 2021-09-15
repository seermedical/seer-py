"""Script to download patient medication adherence for a specified date range.

To run this script:
-------------------
1. Save this script in a folder, or folder location you wish to save data to.
2. Open up your terminal.
3. Navigate to the folder you have saved this script.
4. Run
    ```
    python hep3_medication_compliance_downloader.py
    ```
The default start date is 2021-01-01 and default end date is today's date. If you
wish to specify a date range, you can run, e.g.
    ```
    python hep3_medication_compliance_downloader.py -s 2021-05-05 -e 2021-05-10
    ```
N.b. The date format is YYYY-MM-DD.
If you wish to specify a path to save the date, you can run, e.g.
```
python hep3_medication_compliance_downloader.py -s 2021-05-05 -e 2021-05-10 -o '/path/to/directory'
```
"""
import os
import argparse
import pandas as pd

from datetime import datetime
from seerpy import SeerConnect


def run(client=SeerConnect(), start_date='', end_date='', organisation_id=None, out_path=''):

    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    date_range_str = f'{start_date}-{end_date}'

    # Make directories if they do not exist
    if out_path:
        os.makedirs(out_path, exist_ok=True)

    # Get list of patient IDs and user information
    patients_list = client.execute_query(GET_PATIENTS_QUERY, party_id=organisation_id)['patients']

    all_data = pd.DataFrame()
    for patient in patients_list:
        user_name = patient['user']['fullName']
        medication_adherence_response = client.execute_query(
            query_string=GET_MEDICATION_ADHERENCE_QUERY, variable_values={
                'patientId': patient['id'],
                'startDate': start_date,
                'endDate': end_date
            })

        if not medication_adherence_response['patient']['diary']:
            print(f'No data in this date range for {user_name}')
        medication_adherence_data = medication_adherence_response['patient']['diary'][
            'medicationAdherences']
        if not medication_adherence_data:
            continue

        data = pd.DataFrame(medication_adherence_data).explode('medications', ignore_index=True)
        data.dropna(subset=['medications'], inplace=True)
        if data['medications'].isnull().all():
            print(f'No data for {user_name} in date range {start_date} - {end_date}')
            continue
        medication_data = pd.json_normalize(data['medications'])
        # Select just dates
        data = data[['date']].reset_index(drop=True)
        # Merge dates, medication, and medication adherence into pd.DataFrame
        data = pd.merge(data, medication_data, left_index=True, right_index=True)
        # Save individual data
        data.to_csv(os.path.join(out_path, f'{date_range_str}-{user_name}.csv'))
        # Add individual data to all data
        data.rename(columns={'drugName': user_name, 'status': user_name}, inplace=True)
        all_data = pd.merge(all_data, data, on='date', how='left') if not all_data.empty else data
    # Save all data
    all_data.to_csv(f'{date_range_str}-all.csv')
    print('Done.')
    return


GET_PATIENTS_QUERY = """
{
    patients {
        id
        user {
            fullName
        }
    } 
    }
"""

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
        "-s", "--startdate", default='2021-01-01', help=
        "The start date to retrieve medication compliance. Format: YYYY-MM-DD. Default = 2020-01-01"
    )
    parser.add_argument(
        "-e", "--enddate", default='', help=
        "The end date to retrieve medication compliance. Format: YYYY-MM-DD. Default = Today's Date."
    )
    parser.add_argument("-o", "--outpath", default='',
                        help="The path to where data is to be saved.")
    args = parser.parse_args()

    run(start_date=args.startdate, end_date=args.enddate, out_path=args.outpath)
