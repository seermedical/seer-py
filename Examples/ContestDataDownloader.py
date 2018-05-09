import numpy as np
#import pandas as pd
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
studies = ['Pat1Test']
## include a path to save downloaded data segments to file
path = 'D:/KAGGLE/data/ecosystem/test_download/' # replace with preferred path

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
        directory = path + study
        try:
            os.stat(directory)
        except:
            os.mkdir(directory)  
        
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

            #filename = study + '_' + str(int(hour)) + '_' + str(preictal)
            filename = directory + '/' + study + '_' + str(int(hour)) + '_' + str(preictal)          

            b = ('   -> %s (%d/%d)' % (filename, counter, numFiles) + ''*200)
            sys.stdout.write('\r'+b)
            sys.stdout.flush()

            ## Using threads>1 may speed up your downloads, but may also cause issues
            ## on Windows systems. Use Carefully.
            data = client.getLinks(allData[allData['segments.startTime']==chunk].copy(), threads=5)

            ######################
            # Change this section for saving data segments as different file formats

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
