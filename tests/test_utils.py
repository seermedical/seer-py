# Copyright 2017,2018 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import pathlib

import pandas as pd

from seerpy import utils


# having a class is useful to allow patches to be shared across mutliple test functions, but then
# pylint complains that the methods could be a function. this disables that warning.
# pylint:disable=no-self-use

# not really a problem for these test classes
# pylint:disable=too-few-public-methods


TEST_DATA_DIR = pathlib.Path(__file__).parent / "test_data"


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

    def test_quote_list_of_str(self):
        assert utils.quote_list_of_str(['test1', 'test2']) == '["test1","test2"]'
