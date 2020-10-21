# -*- coding: utf-8 -*-
import sys
import time
import argparse

from os import mkdir
from os.path import dirname, isdir, isfile, join
from seerpy import SeerConnect
from downloader.downloader import DataDownloader
from urllib3.exceptions import ReadTimeoutError
from downloader.utils import read_json, write_json


def get_download_status_file(client):
    """Retrieves JSON of download status for all study IDs."""
    studies_file = join(dirname(__file__), 'studies.json')
    if not isfile(studies_file):
        status = {
            study_id: 0
            for study_id in [
                'fff9aaa9-b104-46e8-9227-b1b76d6f333e', '372482db-bcd8-4c79-85aa-16de76d8df69',
                '92434006-5506-4d4a-97e4-f7232dfd68bd', '4f88cf00-8bf3-43fd-970d-9e434acf6edb',
                'fba0f4f1-29c5-4387-b076-c6f788bbcaca', '72397e46-c424-4580-af50-1c460894664d',
                'bab85744-4040-4496-b35e-dfb1ed638b73', 'ab1d9a55-190a-431f-ad31-653e137f584c',
                '446364b3-ccb3-4927-8a32-f70ab0f5aa13', '3112c100-d71e-438f-96e5-4defc3b9cfa7'
            ]
        }
        write_json(studies_file, status)

    return studies_file


def run(client, path_out):
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

    download_status_file = get_download_status_file(client)
    download_status = read_json(download_status_file)
    study_ids = [study_id for study_id in download_status if download_status[study_id] == 0]

    attempts = 0
    while True:
        try:
            if attempts != 0:
                time.sleep(3)
                client = SeerConnect()

            for study_id in study_ids:
                downloader = DataDownloader(client, study_id, output_dir)
                # Download channel data
                downloader.download_channel_data()
                # Download label data
                downloader.download_label_data()

                download_status[study_id] = 1
                write_json(download_status_file, download_status)
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
