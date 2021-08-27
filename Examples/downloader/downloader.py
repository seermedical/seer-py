from os import mkdir, makedirs, remove
from os.path import dirname, isdir, isfile, join
import pandas as pd

import seerpy
from tqdm import tqdm

from downloader.utils import write_json, add_to_csv


def get_download_log_file():
    """Retrieves JSON of download errors."""
    log_file = join(dirname(__file__), 'logger.csv')
    if not isfile(log_file):
        pd.DataFrame().to_csv(log_file)

    return log_file


class DataDownloader:
    def __init__(self, client, study_id, path_out, channel_group_to_download=None):
        self.client = client
        self.study_id = study_id
        self.label_groups = self.get_label_groups()
        self.study_metadata = self.get_channel_groups_metadata()
        self.channel_group_to_download = channel_group_to_download

        self.study_name = self.study_metadata['name'][0]
        self.folder_out = join(path_out, self.study_name)
        if not isdir(self.folder_out):
            mkdir(self.folder_out)

    def get_channel_groups_metadata(self):
        channel_groups = self.client.get_all_study_metadata_dataframe_by_ids([self.study_id])
        return channel_groups

    def get_label_groups(self):
        return self.client.get_label_groups_for_studies([self.study_id])

    def get_channels(self):
        channels = self.study_metadata[['channelGroups.name', 'channels.name']].drop_duplicates()
        return ([channel_group, channel] for channel_group, channel in zip(
            channels['channelGroups.name'].tolist(), channels['channels.name'].tolist()))

    def get_segment_data(self, folder_out, channel_group, channels, channel_group_metadata,
                         segment_ids):
        for index, segment_id in enumerate(tqdm(segment_ids)):
            segment_metadata = channel_group_metadata[channel_group_metadata['segments.id'] ==
                                                      segment_id]
            segment_files_list = []
            for channel in channels:
                segment_files_list.append(
                    join(folder_out,
                         f'{self.study_name}_{channel_group}_{channel}_segment_{index}.parquet'))

            if all([isfile(segment_file) for segment_file in segment_files_list]):
                continue
            try:
                segment_data = self.client.get_channel_data(segment_metadata)
            except ValueError:
                print('WARNING: Skipping file...')
                log_file = get_download_log_file()
                add_to_csv(log_file, segment_metadata)
                continue

            raw_data_list = []
            for channel in channels:
                raw_data_list.append(
                    pd.DataFrame({
                        'time': segment_data['time'].tolist(),
                        'data': segment_data[channel].tolist()
                    }))

            yield raw_data_list, segment_files_list

    def download_channel_data(self):
        print(f'Downloading channel data for {self.study_name}...')
        for channel_group in self.study_metadata['channelGroups.name'].unique():
            if self.channel_group_to_download is not None:
                if not channel_group == self.channel_group_to_download:
                    continue
            print(channel_group, 'is in list to download.')
            # Create folder
            folder_out = join(self.folder_out, channel_group)
            if not isdir(folder_out):
                mkdir(folder_out)

            channel_group_metadata = self.study_metadata[(
                self.study_metadata['channelGroups.name'] == channel_group)]

            # Get channels
            channels = channel_group_metadata['channels.name'].unique()
            # Save channel metadata
            for channel in channels:
                # Get channel group metadata
                channel_metadata = self.study_metadata[
                    (self.study_metadata['channelGroups.name'] == channel_group)
                    & (self.study_metadata['channels.name'] == channel)]
                channel_metadata_file = join(
                    self.folder_out, f'{self.study_name}_{channel_group}_{channel}_metadata.csv')
                # Save channel metadata
                channel_metadata.to_csv(channel_metadata_file)

            # Get segment IDs
            segment_ids = channel_group_metadata['segments.id'].unique()
            print(f'Progress for channel group: {channel_group} with channels: {channels}')
            # Get segment data
            for segment_data_list, segment_file_list in self.get_segment_data(
                    folder_out, channel_group, channels, channel_group_metadata, segment_ids):
                for segment_data, segment_file in zip(segment_data_list, segment_file_list):
                    # Save segment data to JSON
                    segment_data.to_parquet(segment_file)
        print('Done.')

    def download_label_data(self):
        print(f'Downloading label data for {self.study_name}...')
        labels_file = join(self.folder_out, f'{self.study_name}_labels.csv')
        if isfile(labels_file):
            return
        label_group_ids = [x['id'] for x in self.label_groups[0]['labelGroups']]
        labels = pd.concat([
            self.client.get_labels_dataframe(self.study_id, label_group_id)
            for label_group_id in label_group_ids
        ])
        labels = labels.drop(columns=[
            'labelGroup.description', 'labelGroup.name', 'labelGroup.labelType',
            'labelGroup.numberOfLabels', 'labels.createdAt', 'labels.createdBy.fullName',
            'labels.confidence', 'labels.timezone', 'labels.updatedAt', 'tags.id'
        ])
        # Save labels to CSV
        labels.to_csv(join(labels_file))
        print('Done.')