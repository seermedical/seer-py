from os import makedirs
from os.path import dirname, isfile, join
import pandas as pd

from downloader.utils import add_to_csv


def get_download_log_file():
    """Retrieves JSON of download errors."""
    log_file = join(dirname(__file__), 'logger.csv')
    if not isfile(log_file):
        pd.DataFrame().to_csv(log_file)

    return log_file


class DataDownloader:
    def __init__(self, client, study_id, output_dir, channel_group_to_download='EEG'):
        self.client = client
        self.study_id = study_id
        self.label_groups = self.get_label_groups()
        self.study_metadata = self.get_channel_groups_metadata()
        self.channel_group_to_download = channel_group_to_download

        self.study_name = self.study_metadata['name'][0]
        # Define folder out w/ study_id
        self.folder_out = join(output_dir, self.study_id)

        makedirs(self.folder_out, exist_ok=True)

    def get_channel_groups_metadata(self):
        channel_groups = self.client.get_all_study_metadata_dataframe_by_ids([self.study_id])
        return channel_groups

    def get_label_groups(self):
        return self.client.get_label_groups_for_studies([self.study_id], limit=200)

    def get_segment_data(self, channel_group_metadata, segment_ids):
        for index, segment_id in enumerate(segment_ids):
            segment_metadata = channel_group_metadata[channel_group_metadata['segments.id'] ==
                                                      segment_id]
            try:
                segment_data = self.client.get_channel_data(segment_metadata)
                yield index, segment_data
            except ValueError:
                print('WARNING: Skipping file...')
                log_file = get_download_log_file()
                add_to_csv(log_file, segment_metadata)
                continue

    def download_channel_data(
            self, from_time, to_time, label_id=None,
            label_group_name=None):  # label_group_id and label_group_name for Jodie
        for channel_group in self.study_metadata['channelGroups.name'].unique():
            if self.channel_group_to_download is not None:
                if channel_group != self.channel_group_to_download:
                    continue
            # Create folder
            folder_out = join(self.folder_out, label_group_name, channel_group)
            makedirs(folder_out, exist_ok=True)

            channel_group_metadata = self.study_metadata[(
                self.study_metadata['channelGroups.name'] == channel_group)]
            # Filter segment data to from_time and to_time
            # Due to the way the data is structured and stored, cannot retrieve specific time frame.
            # So just take next chunk.
            # If label exists over a chunk, this could result in data not being retrieved in the 2nd chunk
            # (as I haven't put implentation in for this)
            segment_metadata = channel_group_metadata[
                channel_group_metadata['segments.startTime'] >= from_time].iloc[[0]]

            # Get segment IDs
            segment_ids = segment_metadata['segments.id'].unique()
            # Get segment data
            segment_file_path = join(folder_out, f'{channel_group}_{label_id}.csv')
            if isfile(segment_file_path):
                continue
            for segment_index, segment_data in self.get_segment_data(channel_group_metadata,
                                                                     segment_ids):
                # Un/comment here to slice the data
                sliced_segment_data = segment_data[(segment_data['time'] >= from_time)
                                                   & (segment_data['time'] < to_time)]
                segment_data.to_csv(segment_file_path)

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
