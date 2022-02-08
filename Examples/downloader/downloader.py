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
    def __init__(self, client, study_id, channel_groups_to_download=[]):
        self.client = client
        self.study_id = study_id
        self.label_groups = self.get_label_groups()
        self.study_metadata = self.get_channel_groups_metadata()
        self.channel_groups_to_download = channel_groups_to_download

        self.study_name = self.study_metadata['name'][0]

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

    def download_channel_data(self, from_time, to_time, label_id,
                              path_to_save_channel_group_metadata, path_to_save_segments):

        for channel_group in self.study_metadata['channelGroups.name'].unique():
            if self.channel_groups_to_download is not None:
                if channel_group not in self.channel_groups_to_download:
                    continue
            # Get channel group metadata
            channel_group_metadata = self.study_metadata[(
                self.study_metadata['channelGroups.name'] == channel_group)]
            # Save channel group metadata if doesn't already exist
            channel_group_metadata_file = join(path_to_save_channel_group_metadata, 'metadata.csv')
            if not isfile(channel_group_metadata_file):
                channel_group_metadata.to_csv(channel_group_metadata_file)
            # Filter segment data to from_time and to_time
            # Due to the way the data is structured and stored, cannot retrieve specific time frame.
            # So just take next chunk.
            # If label exists over a chunk, this could result in data not being retrieved in the 2nd chunk
            # (as I haven't put implentation in for this)
            segment_metadata = channel_group_metadata[
                channel_group_metadata['segments.startTime'] <= from_time]
            # Take chunk closest to from_time
            segment_metadata = segment_metadata.iloc[[len(segment_metadata) - 1]]

            # Get segment IDs
            segment_ids = segment_metadata['segments.id'].unique()
            # Get segment data
            segment_file_path = join(path_to_save_segments, f'{label_id}.csv')
            if isfile(segment_file_path):
                continue
            for segment_index, segment_data in self.get_segment_data(channel_group_metadata,
                                                                     segment_ids):
                # Un/comment here to slice the data
                sliced_segment_data = segment_data[(segment_data['time'] >= from_time)
                                                   & (segment_data['time'] < to_time)]
                sliced_segment_data.to_csv(segment_file_path)
