# -*- coding: utf-8 -*-
import sys
import time
import argparse

from tqdm import tqdm
from os import makedirs
from os.path import dirname, isdir, join
from seerpy import SeerConnect
from downloader.downloader import DataDownloader
from urllib3.exceptions import ReadTimeoutError

STUDY_IDS = ['4ddade20-51c3-4d8c-8a21-6c4dec6dd5b3']
OFFSET = 2000


def run(client, path_out):
    """Downloads all data available on Seer's public API."""

    if path_out:
        if not isdir(path_out):
            print('Please specify a path that exists.')
            return
        output_dir = join(path_out, 'data')
    else:
        output_dir = join(dirname(__file__), 'data')
    makedirs(output_dir, exist_ok=True)

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
                            'Abnormal / Epileptiform', 'Diary labels',
                            'Diary labels - Timing Adjusted', 'Reported Events', 'Exemplar',
                            'Unreported Events'
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
    parser.add_argument("-o", "--outpath", help="Path for data to be saved (optional).")
    args = parser.parse_args()

    run(SeerConnect(), args.outpath)
