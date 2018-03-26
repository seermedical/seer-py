import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta
# import h5py
from scipy.io import savemat
import sys
import time

if __name__ == '__main__':
    
    import seer

    client = seer.SeerConnect()
    
    ##studies to download
    ## pick from ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
    studies = ['Pat2Train']
    
    for study in studies:
        print('\nStudy: ', study)
        
        allData = None
        
        allData, channelGroups, segments, channels, dataChunks, labelGroups, labels = client.createMetaData(study)
        
        ## filter the amount of data returned by date
        segmentMin = 1195
        segmentMax = 1197
        dtMin = (datetime(2010,1,1,0,0) + timedelta(hours=segmentMin)).timestamp()*1000
        dtMax = (datetime(2010,1,1,0,0) + timedelta(hours=segmentMax)).timestamp()*1000
        allData = allData[(allData.loc[:,'segments.startTime']>=dtMin) & (allData.loc[:,'segments.startTime']<=dtMax)]
        print('Metadata retieved...\n')
        
#        startTimes = allData.loc[:,'segments.startTime']
#        startTimes = pd.to_datetime(pd.Series(startTimes), unit='ms')
#        startTimes = startTimes.dt.minute
#        startTimesCompare = startTimes.unique()
#        
#        allData['Preictal'] = np.where(startTimes==30, 1, 0)
    
        
        time.sleep(2)
        
        for chunk in allData['segments.startTime'].unique():
            
            t = time.time()
            data = client.getLinks(allData[allData['segments.startTime']==chunk].copy(), threads=5)
#            print(round(time.time()-t, 1))
            
            startTime = datetime.fromtimestamp(chunk/1000)
            hour = (startTime - datetime(2010,1,1,0,0)).total_seconds()/3600
            minute = startTime.minute
            if minute == 30:
                preictal = 1
            else:
                preictal = 0
            
            filename = study + '_' + str(hour) + '_' + str(preictal)
            
            
            ######################
            # Change this section for different file formats
            
            
            #for csv format
            #data.to_csv(filename + '.csv', index=False, float_format='%.3f')
            
            ##for hdf5 format
            #data.to_hdf(filename + '.hdf5', key='data', format='table')
            
            ##for matlab files
            savemat(filename + '.mat', {'data':np.asarray(data.iloc[:,-16:], dtype=np.float32)},
                                        appendmat=False, do_compression=True)
            
            b = ('Downloaded: ' + filename + ''*200)
            sys.stdout.write('\r'+b)
            sys.stdout.flush()