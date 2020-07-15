# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from collections import namedtuple
import pathlib

import pandas as pd

from seerpy import utils

# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# not really a problem for these test classes
# pylint:disable=too-few-public-methods

TEST_DATA_DIR = pathlib.Path(__file__).parent / "test_data"

MockResponse = namedtuple('MockResponse', ['content'])


class TestDownloadChannelData:
    @staticmethod
    def mock_download_function(filename):
        with open(filename, mode='rb') as f:
            content = f.read()
        return MockResponse(content)

    def test_sense_data_success(self):
        # setup
        meta_data_dict = {
            'dataChunks.url': [TEST_DATA_DIR / 'sense_chunk_data_1s.dat'],
            'dataChunks.time': [1593760757000.],
            'segments.startTime': [1593760697000.],
            'segments.duration': [2362000.],
            'channelGroups.sampleEncoding': ['float32'],
            'channelGroups.sampleRate': [250],
            'channelGroups.samplesPerRecord': [250],
            'channelGroups.recordsPerChunk': [10],
            'channelGroups.compression': ['gzip'],
            'channelGroups.signalMin': [0],
            'channelGroups.signalMax': [0],
            'channelGroups.exponent': [-6],
            'channelGroups.timestamped': [True]
        }
        meta_data = pd.DataFrame(meta_data_dict).iloc[0]
        print('meta_data.dtypes', meta_data.dtypes)

        study_id = 'sense-study-id'
        channel_groups_id = 'sense-channel-group-id'
        segments_id = 'sense-segment-id'
        channel_names = [
            'a1', 't3', 'p3', 'c3', 'cz', 'f7', 'o2', 't5', 'c4', 'f8', 'pz', 'fp2', 'f4', 'o1',
            'f3', 'fp1', 'fz', 'a2', 't6', 't4', 'p4'
        ]

        data_q = [meta_data, study_id, channel_groups_id, segments_id, channel_names]

        # run test
        result = utils.download_channel_data(data_q, self.mock_download_function)

        # check result
        expected_result = pd.read_csv(TEST_DATA_DIR / 'sense_channel_data_1s.csv', index_col=0)
        # dtype of floats is float32 in result, but float64 as read from csv
        # use check_dtype=False to save having to convert
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

    def test_sense_data_short(self):
        # setup
        meta_data_dict = {
            'dataChunks.url': [TEST_DATA_DIR / 'sense_chunk_data_1s.dat'],
            'dataChunks.time': [1593760757000.],
            'segments.startTime': [1593760697000.],
            'segments.duration': [60500.],  # .5 seconds after chunk start
            'channelGroups.sampleEncoding': ['float32'],
            'channelGroups.sampleRate': [250],
            'channelGroups.samplesPerRecord': [250],
            'channelGroups.recordsPerChunk': [10],
            'channelGroups.compression': ['gzip'],
            'channelGroups.signalMin': [0],
            'channelGroups.signalMax': [0],
            'channelGroups.exponent': [-6],
            'channelGroups.timestamped': [True]
        }
        meta_data = pd.DataFrame(meta_data_dict).iloc[0]
        print('meta_data.dtypes', meta_data.dtypes)

        study_id = 'sense-study-id'
        channel_groups_id = 'sense-channel-group-id'
        segments_id = 'sense-segment-id'
        channel_names = [
            'a1', 't3', 'p3', 'c3', 'cz', 'f7', 'o2', 't5', 'c4', 'f8', 'pz', 'fp2', 'f4', 'o1',
            'f3', 'fp1', 'fz', 'a2', 't6', 't4', 'p4'
        ]

        data_q = [meta_data, study_id, channel_groups_id, segments_id, channel_names]

        # run test
        result = utils.download_channel_data(data_q, self.mock_download_function)

        # check result
        expected_result = pd.read_csv(TEST_DATA_DIR / 'sense_channel_data_half_sec.csv',
                                      index_col=0)
        # dtype of floats is float32 in result, but float64 as read from csv
        # use check_dtype=False to save having to convert
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)

    def test_siesta_data_success(self):
        # setup
        meta_data_dict = {
            'dataChunks.url': [TEST_DATA_DIR / 'siesta_chunk_data_2s.dat'],
            'dataChunks.time': [1571729124019.5312],
            'segments.startTime': [1571727804019.5312],
            'segments.duration': [8138019.53125],
            'channelGroups.sampleEncoding': ['float32'],
            'channelGroups.sampleRate': [256],
            'channelGroups.samplesPerRecord': [256],
            'channelGroups.recordsPerChunk': [10],
            'channelGroups.compression': ['gzip'],
            'channelGroups.signalMin': [0],
            'channelGroups.signalMax': [0],
            'channelGroups.exponent': [-6],
            'channelGroups.timestamped': [False]
        }
        meta_data = pd.DataFrame(meta_data_dict).iloc[0]
        print('meta_data.dtypes', meta_data.dtypes)

        study_id = 'siesta_study-id'
        channel_groups_id = 'siesta-channel-group-id'
        segments_id = 'siesta-segment-id'
        channel_names = [
            'fz', 'cz', 'pz', 'c3', 'f3', 'f4', 'p4', 'p3', 'a2', 't4', 'a1', 't3', 'fp1', 'fp2',
            'o2', 'o1', 'f7', 'f8', 't6', 't5', 'c4'
        ]

        data_q = [meta_data, study_id, channel_groups_id, segments_id, channel_names]

        # run test
        result = utils.download_channel_data(data_q, self.mock_download_function)

        # check result
        expected_result = pd.read_csv(TEST_DATA_DIR / 'siesta_channel_data_2s.csv', index_col=0)
        # dtype of floats is float32 in result, but float64 as read from csv
        # use check_dtype=False to save having to convert
        pd.testing.assert_frame_equal(result, expected_result, check_dtype=False)


class TestCreateDataChunkUrls:
    def test_success(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_data_chunk_urls.csv", index_col=0)

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_success_from_to_on_segment_borders(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_data_chunk_urls_short.csv",
                                      index_col=0)

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls, from_time=1526275685734.375,
                                              to_time=1526275725734.375)

        # check result
        assert result.equals(expected_result)

    def test_success_from_to_within_segments(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.read_csv(TEST_DATA_DIR / "study1_data_chunk_urls_short.csv",
                                      index_col=0)

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls, from_time=1526275685834.375,
                                              to_time=1526275725634.375)

        # check result
        assert result.equals(expected_result)

    def test_empty_input(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_metadata(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "empty_metadata.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "segment_urls_3.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_empty_segments_urls(self):
        # setup
        meta_data = pd.read_csv(TEST_DATA_DIR / "study1_metadata_short_durations.csv", index_col=0)
        segment_urls = pd.read_csv(TEST_DATA_DIR / "empty_segment_urls.csv", index_col=0)

        expected_result = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])

        # run test
        result = utils.create_data_chunk_urls(meta_data, segment_urls)

        # check result
        assert result.equals(expected_result)

    def test_quote_str(self):
        assert utils.quote_str('test') == '"test"'
