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
    def __init__(self, client, study_id, path_out):
        self.client = client
        self.study_id = study_id
        self.label_groups = self.get_label_groups()
        self.channel_groups = self.get_channel_groups()

        self.study_name = self.channel_groups['name'][0]
        self.folder_out = join(path_out, self.study_name)
        if not isdir(self.folder_out):
            mkdir(self.folder_out)

    def get_channel_groups(self):
        channel_groups = self.client.get_all_study_metadata_dataframe_by_ids([self.study_id])
        channel_groups = channel_groups.drop(columns=[
            'description', 'patient', 'channelGroups.chunkPeriod', 'channelGroups.compression',
            'channelGroups.timestamped', 'segments.timezone', 'channels.channelType.category',
            'channels.channelType.name'
        ], errors='ignore')
        return channel_groups

    def get_label_groups(self):
        return self.client.get_label_groups_for_studies([self.study_id])

    def get_channels(self):
        channels = self.channel_groups[['channelGroups.name', 'channels.name']].drop_duplicates()
        return channels.to_numpy().tolist()

    def get_segment_data(self, folder_out, channel_group, channel, channel_metadata, segment_ids):
        for index, segment_id in enumerate(tqdm(segment_ids)):
            segment_metadata = channel_metadata[channel_metadata['segments.id'] == segment_id]
            segment_file = join(
                folder_out, f'{self.study_name}_{channel_group}_{channel}_segment_{index}.json')
            if isfile(segment_file):
                continue
            try:
                segment_data = self.client.get_channel_data(segment_metadata)
            except ValueError:
                print('WARNING: Skipping file...')
                log_file = get_download_log_file()
                add_to_csv(log_file, segment_metadata)
                continue

            segment_row = segment_metadata.iloc[0, :]

            data = {}
            data['id'] = segment_row['id']
            data['channel_group'] = {}
            data['channel_group']['channel_group_id'] = segment_row['channelGroups.id']
            data['channel_group']['channel_group_name'] = channel_group
            data['channel_group']['sample_rate'] = int(segment_row['channelGroups.sampleRate'])
            data['channel_group']['units'] = segment_row['channelGroups.units']
            data['channel_group']['exponent'] = int(segment_row['channelGroups.exponent'])
            data['channel_group']['signal_min'] = int(segment_row['channelGroups.signalMin'])
            data['channel_group']['signal_max'] = int(segment_row['channelGroups.signalMax'])
            data['channel_group']['channel'] = {}
            data['channel_group']['channel']['channel_id'] = segment_row['channels.id']
            data['channel_group']['channel']['channel_name'] = channel
            data['channel_group']['channel']['segment'] = {}
            data['channel_group']['channel']['segment']['segment_index'] = index
            data['channel_group']['channel']['segment']['segment_id'] = segment_row['segments.id']
            data['channel_group']['channel']['segment']
            data['channel_group']['channel']['segment']['time'] = segment_data['time'].tolist()
            data['channel_group']['channel']['segment']['data'] = segment_data[channel].tolist()

            yield data, segment_file

    def download_channel_data(self):
        print(f'Downloading channel data for {self.study_name}...')

        for (channel_group, channel) in self.get_channels():
            # Create folder
            folder_out = join(self.folder_out, channel_group)
            if not isdir(folder_out):
                mkdir(folder_out)

            # Get channel metadata
            channel_metadata_file = join(
                self.folder_out, f'{self.study_name}_{channel_group}_{channel}_metadata.csv')
            channel_metadata = self.channel_groups[
                (self.channel_groups['channelGroups.name'] == channel_group)
                & (self.channel_groups['channels.name'] == channel)]
            # Save channel metadata
            channel_metadata.to_csv(channel_metadata_file)

            # Get segment IDs
            segment_ids = channel_metadata['segments.id'].unique()
            print(f'Progress for channel: {channel}')
            # Get segment data
            for segment_data, segment_file in self.get_segment_data(folder_out, channel_group,
                                                                    channel, channel_metadata,
                                                                    segment_ids):
                # Save segment data to JSON
                write_json(segment_file, segment_data)
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
