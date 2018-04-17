import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta, timezone
# import h5py
from scipy.io import savemat
import sys
import time

######################
# Change this section for different studies / segment filters

## filter the amount of data returned by date - comment out to download all study data
segmentMin = 0
segmentMax = 5

## studies to download
## pick from ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
studies = ['Pat1Train', 'Pat2Train']

if __name__ == '__main__':

    GMToffset = 11 # Melb time
    baseTime = datetime(2010,1,1,0,0, tzinfo=timezone.utc) + timedelta(hours=-GMToffset)
    try:
        dtMin = (baseTime + timedelta(hours=segmentMin)).timestamp()*1000
        dtMax = (baseTime + timedelta(hours=segmentMax+1)).timestamp()*1000
    except NameError:
        print('No segment filter provided (downloading all data)')
        dtMin = None
        dtMax = None

    import seerpy

    client = seerpy.SeerConnect()

    for study in studies:
        print('\nStudy: ', study)
        print('  Retrieving metadata...')

        allData = None
        allData, channelGroups, segments, channels, dataChunks, labelGroups, labels = client.createMetaData(study)

        if dtMin is not None and dtMax is not None:
            allData = allData[(allData.loc[:,'segments.startTime']>=dtMin) & (allData.loc[:,'segments.startTime']<=dtMax)]

#        startTimes = allData.loc[:,'segments.startTime']
#        startTimes = pd.to_datetime(pd.Series(startTimes), unit='ms')
#        startTimes = startTimes.dt.minute
#        startTimesCompare = startTimes.unique()
#
#        allData['Preictal'] = np.where(startTimes==30, 1, 0)

        time.sleep(2)

        numFiles = len(allData['segments.startTime'].unique())
        print('  Downloading %d file(s)...' % numFiles)
        counter = 1

        for chunk in allData['segments.startTime'].unique():

            startTime = datetime.fromtimestamp(chunk/1000, tz=timezone.utc)
            hour = (startTime - baseTime).total_seconds()/3600
            minute = startTime.minute
            if minute == 30:
                preictal = 1
            else:
                preictal = 0

            filename = study + '_' + str(int(hour)) + '_' + str(preictal)

            b = ('   -> %s (%d/%d)' % (filename, counter, numFiles) + ''*200)
            sys.stdout.write('\r'+b)
            sys.stdout.flush()

            data = client.getLinks(allData[allData['segments.startTime']==chunk].copy(), threads=5)

            ######################
            # Change this section for different file formats

            #for csv format
            #data.to_csv(filename + '.csv', index=False, float_format='%.3f')

            ##for hdf5 format
            #data.to_hdf(filename + '.hdf5', key='data', format='table')

            ##for matlab files
            savemat(
                filename + '.mat',
                { 'data': np.asarray(data.iloc[:,-16:], dtype=np.float32) },
                appendmat = False, do_compression = True
            )
            counter += 1

        b = ('  Finished downloading study.' + ''*200)
        sys.stdout.write('\r'+b)
        sys.stdout.flush()
