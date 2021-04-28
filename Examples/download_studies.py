# -*- coding: utf-8 -*-
import sys
import time
import argparse

from os import mkdir
from os.path import dirname, isdir, isfile, join
from urllib3.exceptions import ReadTimeoutError
from downloader.utils import read_json, write_json
from seerpy import SeerConnect
from downloader.downloader import DataDownloader


def get_download_status_file(client, update_study_ids, party_id):
    """Retrieves JSON of download status for all study IDs."""
    studies_file = join(dirname(__file__), 'studies.json')

    if not isfile(studies_file):
        status = {
            study_id: 0
            for study_id in client.get_study_ids(party_id=party_id)
        }
        write_json(studies_file, status)
    else:
        if update_study_ids:
            status = read_json(studies_file)
            add_to_status = {
                study_id: 0
                for study_id in client.get_study_ids(party_id=party_id) if study_id not in status.keys()
            }
            status.update(add_to_status)
            write_json(studies_file, status)
    return studies_file


def run(client, path_out, update_study_ids, channel_groups_to_download, channels_to_download, party_id):
    """Downloads all data available on Seer's public API."""
    if path_out:
        if not isdir(path_out):
            print('Please specify a path that exists.')
            return
        output_dir = join(path_out, 'data')
    else:
        output_dir = join(dirname(__file__), 'data')
    if not isdir(output_dir):
        mkdir(output_dir)

    download_status_file = get_download_status_file(client, update_study_ids, party_id)
    download_status = read_json(download_status_file)
    study_ids = [study_id for study_id in download_status if not download_status[study_id]]

    attempts = 0
    while True:
        try:
            for study_id in study_ids:
                if attempts != 0:
                    time.sleep(3)
                    client = SeerConnect()
                downloader = DataDownloader(client, study_id, output_dir)
                # Download channel data
                downloader.download_channel_data(channel_groups_to_download, channels_to_download)
                download_status[study_id] = 1
                write_json(download_status_file, download_status)

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

    print('Download completed.')
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--outpath", help="Path for data to be saved (optional).")
    parser.add_argument("-u", "--update", default=False, help="Updates list of cached study IDs (optional). Default is False.")
    parser.add_argument("-cg", "--channelgroups", nargs="+", help="Name of channel groups to download (optional).")
    parser.add_argument("-c", "--channels", nargs="+", help="Name of channels to download (optional).")
    parser.add_argument("-p", "--party_id", help="Party ID for specific organisation to download studies from.")
    args = parser.parse_args()
 
    run(SeerConnect(), args.outpath, args.update, args.channelgroups, args.channels, args.party_id)
