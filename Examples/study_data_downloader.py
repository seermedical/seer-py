# -*- coding: utf-8 -*-
import sys
import time
import argparse

from tqdm import tqdm
from seerpy import SeerConnect
from downloader.downloader import DataDownloader
from urllib3.exceptions import ReadTimeoutError

STUDY_IDS = ['03eb4c39-fed3-491b-9df2-cd3351011060', '3a39f279-4f0f-41e1-beb0-a1f0bd006058']
OFFSET = 2000


def run(client, output_dir):
    """Downloads all data available on Seer's public API."""

    attempts = 0
    while True:
        try:
            if attempts != 0:
                time.sleep(3)
                client = SeerConnect()

            for study_id in STUDY_IDS:
                downloader = DataDownloader(client, study_id, output_dir)

                for label_group in downloader.label_groups[0]['labelGroups']:
                    # filter to label groups of interest
                    if not label_group['name'] in [
                            'Abnormal / Epileptiform',
                            'Diary labels',
                            # 'Diary labels - Timing Adjusted', 'Reported Events', 'Exemplar',
                            # 'Unreported Events'
                    ]:
                        continue
                    if label_group['name'] == 'Abnormal / Epileptiform':
                        label_group['name'] = 'Abnormal Epileptiform'

                    # check for folder with name of label group

                    print(f"Downloading EEG chunks for {label_group['name']}")
                    for label in tqdm(label_group['labels']):
                        # get EEG 2000 msec before to 2000 msec after label
                        get_data_from_time = label['startTime'] - OFFSET
                        get_data_to_time = label['startTime'] + label['duration'] + OFFSET

                        # Download channel data
                        downloader.download_channel_data(from_time=get_data_from_time,
                                                         to_time=get_data_to_time,
                                                         label_id=label['id'],
                                                         label_group_name=label_group['name'])
                # Download label data
                downloader.download_label_data()

            print('Download completed.')
            return

        except ReadTimeoutError:
            attempts += 1
            for i in range(5, 0, -1):
                sys.stdout.write(f'ReadTimeoutError. Re-trying after a short break... {str(i)}')
                sys.stdout.flush()
                time.sleep(1)

        # If user reaches 5 attempts, break and request user to return later
        if attempts == 5:
            print("ReadTimeoutError! Number of tries exceeded. Please re-run \
                this script at a later time.")
            break

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--outpath", default='/Users/dominique',
                        help="Path for data to be saved (optional).")
    args = parser.parse_args()

    run(client=SeerConnect(), output_dir=args.outpath)
