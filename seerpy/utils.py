# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import functools
import gzip
from multiprocessing import Pool
import os

from matplotlib.collections import LineCollection
from matplotlib import gridspec
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import requests
from scipy.signal import butter, sosfilt


# pylint:disable=too-many-locals,too-many-statements
def download_channel_data(data_q, download_function):
    meta_data, study_id, channel_groups_id, segments_id, channel_names = data_q
    try:
        raw_data = download_function(meta_data['dataChunks.url'])
        data = raw_data.content

        try:
            if meta_data['channelGroups.compression'] == 'gzip':
                data = gzip.decompress(data)
        except OSError:
            pass

        data_type = meta_data['channelGroups.sampleEncoding']
        data = np.frombuffer(data, dtype=np.dtype(data_type))
        data = data.astype(np.float32)
        data = data.reshape(-1, len(channel_names),
                            int(meta_data['channelGroups.samplesPerRecord']))
        data = np.transpose(data, (0, 2, 1))
        data = data.reshape(-1, data.shape[2])
        if 'int' in data_type:
            nan_mask = np.all(data == np.iinfo(np.dtype(data_type)).min, axis=1)
            if nan_mask[-1]:
                nan_mask_corrected = np.ones(nan_mask.shape, dtype=bool)
                for i in range(len(nan_mask) - 1, -1, -1):
                    if nan_mask[i]:
                        nan_mask_corrected[i] = False
                    else:
                        break
                data = data[nan_mask_corrected]

            # fill missing values with nans
            data[np.all(data == np.iinfo(np.dtype(data_type)).min, axis=1), :] = np.nan
        ## TODO: what happens for floats?
        chan_min = meta_data['channelGroups.signalMin'].astype(np.float64)
        chan_max = meta_data['channelGroups.signalMax'].astype(np.float64)
        exponent = meta_data['channelGroups.exponent'].astype(np.float64)
        if 'int' in data_type:
            chan_diff = chan_max - chan_min
            dig_min = np.iinfo(data_type).min
            dig_max = np.iinfo(data_type).max
            dig_diff = abs(dig_min) + abs(dig_max)

            with np.errstate(divide='ignore', invalid='ignore'):
                data = (data - dig_min) / dig_diff * chan_diff + chan_min

        data = data * 10.0**exponent
        data = pd.DataFrame(data=data, index=None, columns=channel_names)
        data = data.fillna(method='ffill', axis='columns')
        data = data.fillna(method='bfill', axis='columns')
        data = data.fillna(value=0., axis='columns')
        data['time'] = (np.arange(data.shape[0]) *
                        (1000.0 / meta_data['channelGroups.sampleRate']) +
                        meta_data['dataChunks.time'])
        data['id'] = study_id
        data['channelGroups.id'] = channel_groups_id
        data['segments.id'] = segments_id
        data = data[['time', 'id', 'channelGroups.id', 'segments.id'] + channel_names]
        return data
    except Exception as ex:
        print(ex)
        print(study_id)
        print(channel_names)
        print(meta_data['dataChunks.url'])
        print('{0:.2f}'.format(meta_data['dataChunks.time']))
        print(meta_data)
        try:
            print(raw_data.headers)
        except Exception as ex:  # pylint: disable=broad-except
            pass
        raise


# pylint:disable=too-many-locals
def create_data_chunk_urls(metadata, segment_urls, from_time=0, to_time=9e12):
    chunk_pattern = '00000000000.dat'

    data_chunks = []
    metadata = metadata.drop_duplicates('segments.id').reset_index(drop=True)
    for index in range(len(metadata.index)):
        row = metadata.iloc[index]

        seg_base_urls = segment_urls.loc[segment_urls['segments.id'] ==
                                         row['segments.id'], 'baseDataChunkUrl']
        if seg_base_urls.empty:
            continue
        seg_base_url = seg_base_urls.iloc[0]

        chunk_period = row['channelGroups.chunkPeriod']
        num_chunks = int(np.ceil(row['segments.duration'] / chunk_period / 1000.))
        start_time = row['segments.startTime']

        for i in range(num_chunks):
            chunk_start_time = chunk_period * 1000 * i + start_time
            next_chunk_start_time = chunk_period * 1000 * (i + 1) + start_time
            if (chunk_start_time <= to_time and next_chunk_start_time >= from_time):
                data_chunk_name = str(i).zfill(len(chunk_pattern) - 4) + chunk_pattern[-4:]
                data_chunk_url = seg_base_url.replace(chunk_pattern, data_chunk_name)
                data_chunk = [row['segments.id'], data_chunk_url, chunk_start_time]
                data_chunks.append(data_chunk)

    return pd.DataFrame.from_records(data_chunks,
                                     columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])


# pylint:disable=too-many-locals
def get_channel_data(
        all_data,
        segment_urls,  # pylint:disable=too-many-arguments
        download_function=requests.get,
        threads=None,
        from_time=0,
        to_time=9e12):
    """Download data chunks and stich them together in one dataframe

    Parameters
    ----------
    all_data : pandas DataFrame
            metadata required for downloading and processing raw data
    segment_urls : pandas DataFrame
            columns=['segments.id', 'baseDataChunkUrl']
    download_function: function
            the function used to download the channel data. defaults to requests.get
    threads : int
            number of threads to use. If > 1 then will use multiprocessing
            if None (default), it will use 1 on Windows and 5 on Linux/MacOS

    Returns
    -------
    data : pandas DataFrame
            dataframe containing studyID, channelGroupIDs, semgmentIDs, time, and raw data

    Example
    -------
    data = get_channel_data(all_data, segment_urls)

    """
    if threads is None:
        if os.name == 'nt':
            threads = 1
        else:
            threads = 5

    segment_ids = all_data['segments.id'].drop_duplicates().tolist()

    data_q = []
    data_list = []

    for segment_id in segment_ids:
        metadata = all_data[all_data['segments.id'].values == segment_id]
        actual_channel_names = get_channel_names_or_ids(metadata)
        metadata = metadata.drop_duplicates('segments.id')

        study_id = metadata['id'].iloc[0]
        channel_groups_id = metadata['channelGroups.id'].iloc[0]

        data_chunks = create_data_chunk_urls(metadata, segment_urls, from_time=from_time,
                                             to_time=to_time)
        metadata = metadata.merge(data_chunks, how='left', left_on='segments.id',
                                  right_on='segments.id', suffixes=('', '_y'))

        metadata = metadata[[
            'dataChunks.url', 'dataChunks.time', 'channelGroups.sampleEncoding',
            'channelGroups.sampleRate', 'channelGroups.samplesPerRecord',
            'channelGroups.recordsPerChunk', 'channelGroups.compression', 'channelGroups.signalMin',
            'channelGroups.signalMax', 'channelGroups.exponent'
        ]]
        metadata = metadata.drop_duplicates()
        metadata = metadata.dropna(axis=0, how='any', subset=['dataChunks.url'])
        for i in range(len(metadata.index)):
            data_q.append(
                [metadata.iloc[i], study_id, channel_groups_id, segment_id, actual_channel_names])

    download_function = functools.partial(download_channel_data,
                                          download_function=download_function)
    if data_q:
        if threads > 1:
            pool = Pool(processes=min(threads, len(data_q) + 1))
            data_list = list(pool.map(download_function, data_q))
            pool.close()
            pool.join()
        else:
            data_list = [download_function(data_q_item) for data_q_item in data_q]

    if data_list:
        # sort=False to silence deprecation warning. This comes into play when we are processing
        # segments across multiple channel groups which have different channels.
        data = pd.concat(data_list, sort=False)
        data = data.loc[(data['time'] >= from_time) & (data['time'] < to_time)]
        data = data.sort_values(['id', 'channelGroups.id', 'time'], axis=0, ascending=True,
                                na_position='last')
        data = data.reset_index(drop=True)
    else:
        data = None

    return data


def get_channel_names_or_ids(metadata):
    """Get a list of unique channel names, or ids where name is null or duplicated.

    Parameters
    ----------
    metadata : pandas DataFrame
            a dataframe containing a 'channels.name' and 'channels.id' column

    Returns
    -------
    actual_channel_names : list
            a list of unique channels names or ids.
    """
    actual_channel_names = []
    unique_ids = metadata.drop_duplicates(subset='channels.id')
    name_value_counts = unique_ids['channels.name'].value_counts()
    for index in range(len(unique_ids.index)):
        row = unique_ids.iloc[index]
        channel_name = row['channels.name']
        if not channel_name or name_value_counts[channel_name] > 1:
            actual_channel_names.append(row['channels.id'])
        else:
            actual_channel_names.append(channel_name)
    return actual_channel_names


# pylint:disable=too-many-locals
def plot_eeg(x, y=None, pred=None, squeeze=5.0, scaling_factor=None):
    if not isinstance(x, np.ndarray):
        x = np.asarray(x)

    if len(x.shape) < 2:
        x = x.reshape(-1, 1)

    channels = x.shape[1]
    x = np.flip(x, axis=1)
    has_pred = 1 if pred is not None else 0
    grid_spec = gridspec.GridSpec(2, 1, height_ratios=[channels, 1])
    ticks = np.arange(x.shape[0]).astype(np.float32)
    fig = plt.figure(figsize=(14, (channels + has_pred) * 2))
    fig.tight_layout()
    ticklocs = []
    ax2 = fig.add_subplot(grid_spec[0])
    if scaling_factor is None:
        scaling_factor = np.nanmedian(np.abs(x)) * squeeze  # Crowd them a bit.
    y_bottom = -scaling_factor  # pylint: disable=invalid-unary-operand-type
    y_top = (channels) * scaling_factor
    ax2.set_ylim(y_bottom, y_top)
    ax2.set_xlim(ticks.min(), ticks.max())

    segs = []
    for i in range(channels):
        segs.append(np.hstack((ticks[:, np.newaxis], x[:, i, np.newaxis])))
        ticklocs.append(i * scaling_factor)

    offsets = np.zeros((channels, 2), dtype=float)
    offsets[:, 1] = ticklocs

    lines = LineCollection(segs, offsets=offsets, transOffset=None, linewidths=(0.5))
    ax2.add_collection(lines)

    if y is not None:
        if len(y.shape) > 1:
            for i in range(y.shape[-1]):
                y_ticks = ticks.copy()
                y_ticks[y[:, i].reshape(-1) == 0] = np.nan
                ax2.fill_between(y_ticks, -1000, 1000, alpha=0.1)
        else:
            ticks[y == 0] = np.nan
            ax2.fill_between(ticks, -1000, 1000, alpha=0.1)

    if pred is not None:
        ax3 = fig.add_subplot(grid_spec[1])
        ax3.set_ylim(0, 1)
        ax3.fill_between(np.arange(len(pred)) * 10, 0, pred, alpha=0.6)

    plt.tight_layout()
    return plt


def butter_bandstop(lowcut, highcut, fs, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    sos = butter(order, [low, high], analog=False, btype='bandstop', output='sos')
    return sos


def butter_bandstop_filter(data, lowcut, highcut, fs, order=5):
    sos = butter_bandstop(lowcut, highcut, fs, order=order)
    y = sosfilt(sos, data)
    return y


def butter_bandpass(lowcut, highcut, fs, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    sos = butter(order, [low, high], analog=False, btype='bandpass', output='sos')
    return sos


def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    sos = butter_bandpass(lowcut, highcut, fs, order=order)
    y = sosfilt(sos, data)
    return y
