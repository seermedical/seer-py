# -*- coding: utf-8 -*-
from os import makedirs
from os.path import abspath, dirname, join
import sys
import time
import argparse
import pandas as pd

from tqdm import tqdm
from seerpy import SeerConnect
from downloader.downloader import DataDownloader
from urllib3.exceptions import ReadTimeoutError

OFFSET = 2000


def create_dir(path):
    """Creates a directory and returns the path."""
    makedirs(path, exist_ok=True)
    return path


def json_to_csv(labels):
    "Flattens a single-level JSON object to pandas.DataFrame."
    return pd.json_normalize(labels)


def get_study_ids_from_party_id(client, party_id):
    """Gets study IDs from a specific organisation using a party ID."""
    # N.b. This queries a list of patient IDs first because study IDs have
    # not been added to the BMTH organisation. Therefore, we cannot query
    # study IDs directly, or map patient IDs to study IDs directly. So, this
    # is a two-step process.

    patient_ids = [patient['id'] for patient in client.get_patients(party_id=party_id)]
    study_ids = []
    for patient_id in patient_ids:
        patient_study_ids = client.get_study_id_from_patient(patient_id=patient_id)
        study_ids.extend(patient_study_ids)
    return study_ids


def run(client, party_id, study_ids, channel_groups, label_groups, output_dir):
    """Downloads all data available on Seer's public API."""
    breakpoint()
    if not isinstance(channel_groups, list):
        raise Exception('Input argument channel groups must be a list.')
    if not party_id or not study_ids:
        raise Exception('Please specify party ID or study IDs.'
                        )  # TODO: expand to enable all studies to be downloaded if none specified

    if not study_ids:
        study_ids = []

    attempts = 0
    while True:
        try:
            if attempts != 0:
                time.sleep(3)
                client = SeerConnect()

            if party_id:
                study_ids.extend(get_study_ids_from_party_id(client=client, party_id=party_id))

            for study_id in study_ids:
                print(f"Checking for study Id: {study_id}")
                downloader = DataDownloader(client, study_id,
                                            channel_groups_to_download=channel_groups)

                for channel_group in channel_groups:
                    # create root_dir > study_id > channel_group directory
                    channel_group_dir = create_dir(
                        path=join(output_dir, study_id,
                                  channel_group))  # TODO: improve to take *args and run checks

                    for label_group in downloader.label_groups[0]['labelGroups']:
                        # filter to label groups of interest
                        if not label_group['name'] in label_groups:
                            continue
                        if not label_group['labels']:
                            continue
                        if '/' in label_group['name']:
                            label_group['name'] = label_group['name'].replace('/', '')
                        if '  ' in label_group['name']:
                            label_group['name'] = label_group['name'].replace('  ', ' ')

                        # create root_dir > study_id > channel_group > label group directory
                        channel_group_label_group_dir = create_dir(
                            path=join(channel_group_dir, label_group['name']))
                        # create data frame for each label group
                        label_group_dataframe = json_to_csv(label_group['labels'])
                        # add column for endTime
                        label_group_dataframe['endTime'] = label_group_dataframe[
                            'startTime'] + label_group_dataframe['duration']
                        # Save labels
                        label_group_dataframe.to_csv(
                            join(channel_group_label_group_dir, f"{label_group['name']}.csv"))

                        print(f"Downloading EEG chunks for {label_group['name']}")
                        for label in tqdm(label_group['labels']):
                            # get EEG 2000 msec before to 2000 msec after label
                            get_data_from_time = label['startTime'] - OFFSET
                            get_data_to_time = label['startTime'] + label['duration'] + OFFSET

                            # Download channel data
                            downloader.download_channel_data(
                                from_time=get_data_from_time, to_time=get_data_to_time,
                                label_id=label['id'],
                                path_to_save_channel_group_metadata=channel_group_dir,
                                path_to_save_segments=channel_group_label_group_dir)

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

    run(
        client=SeerConnect(), party_id='', study_ids=[], channel_groups=['EEG'], label_groups=[
            'Abnormal / Epileptiform', 'Diary labels', 'Diary labels - Timing Adjusted',
            'Reported Events', 'Unreported Events', 'Normal / Routine', 'Suspect High'
        ], output_dir=join(dirname(dirname(abspath(__file__))), 'data'))
