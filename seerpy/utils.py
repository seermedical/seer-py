# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
import numpy as np
import pandas as pd
import requests
# import time
import gzip

def download_link(data_q):

    try:
        metaData, studyID, channelGroupsID, segmentsID, channelNames = data_q
#                    t = time.time()
        data = requests.get(metaData['dataChunks.url'])
#                    print('chunk download time: ',round(time.time()-t,2))
        
        try:
            if metaData['channelGroups.compression'] == 'gzip':
                data = gzip.decompress(data.content)
            else:
                data = data.content
        except:
            data = data.content
        dataType = metaData['channelGroups.sampleEncoding']
#                    print(data.content)
#                        print(metaData['dataChunks.url'])
        data = np.fromstring(data, dtype=np.dtype(dataType))
        data = data.astype(np.float32)
        data = data.reshape(-1, len(channelNames), int(metaData['channelGroups.samplesPerRecord']))
        data = np.transpose(data, (0, 2, 1))
        data = data.reshape(-1, data.shape[2])
        chanMin = metaData['channelGroups.signalMin'].astype(np.float64)
        chanMax = metaData['channelGroups.signalMax'].astype(np.float64)
        exponent = metaData['channelGroups.exponent'].astype(np.float64)
        chanDiff = chanMax - chanMin
        digMin = np.iinfo(dataType).min
        digMax = np.iinfo(dataType).max
        digDiff = abs(digMin) + abs(digMax)
        with np.errstate(divide='ignore', invalid='ignore'):
#                                data = np.where(data>0.0, data/np.nanmax(data, axis=0)*chanMax, data/np.nanmin(data, axis=0)*chanMin)
            data = (data - digMin) / digDiff * chanDiff + chanMin
        data = data * 10.0 ** exponent
        data = np.nan_to_num(data).astype(np.float32)
        data = pd.DataFrame(data=data, index=None, columns=channelNames)
        timeLine = np.arange(data.shape[0]) * (1000.0/metaData['channelGroups.sampleRate']) + metaData['dataChunks.time']
        data['time'] = timeLine
        data['id'] = studyID
        data['channelGroups.id'] = channelGroupsID
        data['segments.id'] = segmentsID
        dataCols = ['time', 'id', 'channelGroups.id', 'segments.id'] + channelNames
        data = data[dataCols]
        return data
    except Exception as e:
        print(e)
        raise
