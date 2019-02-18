# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import gzip

from matplotlib.collections import LineCollection
from matplotlib import gridspec
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
import requests


def download_link(data_q):
    meta_data, study_id, channel_groups_id, segments_id, channel_names = data_q
    try:
        data = requests.get(meta_data['dataChunks.url'])
    
        try:
            if meta_data['channelGroups.compression'] == 'gzip':
                data = gzip.decompress(data.content)
            else:
                data = data.content
        except Exception:  # pylint: disable=broad-except
            data = data.content
    
        data_type = meta_data['channelGroups.sampleEncoding']
        data = np.frombuffer(data, dtype=np.dtype(data_type))
        data = data.astype(np.float32)
        data = data.reshape(-1, len(channel_names),
                            int(meta_data['channelGroups.samplesPerRecord']))
        data = np.transpose(data, (0, 2, 1))
        data = data.reshape(-1, data.shape[2])
        if 'int' in data_type:
            nan_mask = np.all(data==np.iinfo(np.dtype(data_type)).min,axis=1)
            if nan_mask[-1]:
                nan_mask_corrected = np.ones(nan_mask.shape, dtype=bool)
                for i in range(len(nan_mask)-1,-1,-1):
                    if nan_mask[i]:
                        nan_mask_corrected[i] = False
                    else:
                        break
                data = data[nan_mask_corrected]
            
            # fill missing values with nans
            data[np.all(data==np.iinfo(np.dtype(data_type)).min,axis=1)] = np.nan
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
    
        data = data * 10.0 ** exponent
        data = pd.DataFrame(data=data, index=None, columns=channel_names)
        data = data.fillna(method='ffill', axis='columns')
        data['time'] = (np.arange(data.shape[0]) * (1000.0 / meta_data['channelGroups.sampleRate'])
                        + meta_data['dataChunks.time'])
        data['id'] = study_id
        data['channelGroups.id'] = channel_groups_id
        data['segments.id'] = segments_id
        data = data[['time', 'id', 'channelGroups.id', 'segments.id'] + channel_names]
        return data
    except Exception as e:
        print(e)
        print(study_id)
        print(channel_names)
        print(meta_data['dataChunks.url'])
        print('{0:.2f}'.format(meta_data['dataChunks.time']))
        print(meta_data)
        raise


# pylint:disable=too-many-locals
def plot_eeg(x, y=None, pred=None, squeeze=8.0, scaling_factor=None):
    if not isinstance(x, np.ndarray):
        x = np.asarray(x)

    if len(x.shape) < 2:
        x = x.reshape(-1, 1)

    channels = x.shape[1]
    x = np.flip(x, axis=1)
    has_pred = 1 if pred is not None else 0
    grid_spec = gridspec.GridSpec(2, 1, height_ratios=[channels, 1])
    ticks = np.arange(x.shape[0]).astype(np.float32)
    fig = plt.figure(figsize=(14, (channels + has_pred) * 0.4))
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
            if y != 1:
                ticks[:] = np.nan
            ax2.fill_between(ticks, -1000, 1000, alpha=0.1)

    if pred is not None:
        ax3 = fig.add_subplot(grid_spec[1])
        ax3.set_ylim(0, 1)
        ax3.fill_between(np.arange(len(pred)) * 10, 0, pred, alpha=0.6)

    plt.tight_layout()
    return plt
