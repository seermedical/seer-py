import numpy as np
import pandas as pd
#from pandas.io.json import json_normalize
from datetime import datetime, timedelta, timezone
# import h5py
from scipy.io import savemat
import sys
import time
import os

######################
# Change this section for different studies / segment filters

## Filter the amount of data returned by date - comment out segmentMin and segmentMax to download all study data.
## If you experience connection breaks you may need to specify specific values for segmentMin and segmentMax 
## to download a specific range of data segments. For 'Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 
## 'Pat3Train' the values for segmentMin and segmentMax should be chosen within the ranges of [1,216], [1,1728], [1,1002], 
## [1,2370], [1,690], [1,2396], respectively, and the total number of data segments is 216, 826, 1002, 2058, 690, 2163
## respectively. Note that for training data the segment index preserves temporal order in the data but is not necessarily 
## continuous, while for testing data the segment index is randomised and so does not preserve temporal order in the data.    
#segmentMin = 1
#segmentMax = 5

## studies to download
## pick from ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
#studies = ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
# studies = ['Pat1Test']
studies = ['Pat1Test New']
## include a path to save downloaded data segments to file
# path = 'D:/KAGGLE/data/ecosystem/test_download/' # replace with preferred path
path = './' # replace with preferred path

# Change this section for saving data segments as different file formats
# fileType = '.csv'
# fileType = '.hdf5'
fileType = '.mat'

blockSize = 10 * 60 * 1000

def getFilename(directory, study, fileType, blockStart, timezone, baseTime):
    startTime = datetime.fromtimestamp(blockStart/1000, tz=timezone.utc)
    hour = (startTime - baseTime).total_seconds()/3600
    minute = startTime.minute
    if minute >= 30:
        preictal = 1
    else:
        preictal = 0
    return directory + '/' + study + '_' + str(int(hour)) + '_' + str(preictal) + fileType

if __name__ == '__main__':

    baseTime = datetime(2010,1,1,0,0, tzinfo=timezone.utc)
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
        directory = path + study
        try:
            os.stat(directory)
        except:
            os.mkdir(directory)  
        
        print('\nStudy: ', study)
        print('  Retrieving metadata...')

        allData = None
        allData = client.createMetaData(study)
        sampleRate = allData['channelGroups.sampleRate'].iloc[0]
        
        if dtMin is not None and dtMax is not None:
            allData = allData[(allData.loc[:,'segments.startTime']>=dtMin) & (allData.loc[:,'segments.startTime']<=dtMax)]
        

        numFiles = len(allData['segments.startTime'].unique())
        print('  Downloading %d file(s)...' % numFiles)
        counter = 1

        startTime = allData['segments.startTime'].unique()[0]

        while startTime <= allData['segments.startTime'].unique()[-1]:

            blockStart = divmod(startTime, 1800)[0] * 1800
            segments = allData[
                (allData['segments.startTime'] >= blockStart) & (allData['segments.startTime'] < blockStart + blockSize)
            ]['segments.startTime'].unique()

            if len(segments) == 0:
                break

            filename = getFilename(directory, study, fileType, blockStart, timezone, baseTime)
            if os.path.exists(filename):
                counter += len(segments)
            else:

                data = pd.DataFrame(np.arange(blockStart, blockStart + blockSize, 1000 / sampleRate, dtype=np.int64), columns=['time'])

                for segmentStartTime in segments:

                    b = ('   -> %s (%d/%d)' % (filename, counter, numFiles) + ''*200)
                    sys.stdout.write('\r'+b)
                    sys.stdout.flush()

                    ## Using threads>1 may speed up your downloads, but may also cause issues
                    ## on Windows systems. Use Carefully.
                    segmentData = client.getLinks(allData[allData['segments.startTime']==segmentStartTime].copy(), threads=5)
                    data = data.merge(segmentData)
                    counter += 1

                if fileType == '.csv':
                    # csv format
                    data.to_csv(filename, index=False, float_format='%.3f')
                elif fileType == '.hdf5':
                    # hdf5 format
                    data.to_hdf(filename, key='data', format='table')
                elif fileType == '.mat':
                    # matlab files
                    savemat(filename, { 'data': np.asarray(data.iloc[:,-16:], dtype=np.float32) }, appendmat=False, do_compression=False)
                else:
                    raise('Unknown file type for download')

            remainingTimes = allData[(allData['segments.startTime'] > blockStart + blockSize)]['segments.startTime'].unique()
            if len(remainingTimes) == 0:
                break
            startTime = remainingTimes[0]

        b = ('  Finished downloading study.' + ''*200)
        sys.stdout.write('\r'+b)
        sys.stdout.flush()
