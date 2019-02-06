from datetime import datetime, timedelta, timezone
import errno
import os
import sys

# these are required if writing matlab files
import numpy as np  # pylint: disable=unused-import
from scipy.io import savemat  # pylint: disable=unused-import

import seerpy


######################
# Change this section for different studies / segment filters

## Filter the amount of data returned by date - comment out segment_min and segment_max to download
## all study data.
## If you experience connection breaks you may need to specify specific values for segment_min and
## segment_max to download a specific range of data segments.
## For 'Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train' the values for
## segment_min and segment_max should be chosen within the ranges of [1,216], [1,1728], [1,1002],
## [1,2370], [1,690], [1,2396], respectively, and the total number of data segments is 216, 826,
## 1002, 2058, 690, 2163 respectively.
## Note that for training data the segment index preserves temporal order in the data but is not
## necessarily continuous, while for testing data the segment index is randomised and so does not
## preserve temporal order in the data.
segment_min = 1
segment_max = 5

## studies to download
## pick from ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
#studies = ['Pat1Test', 'Pat1Train', 'Pat2Test', 'Pat2Train', 'Pat3Test', 'Pat3Train']
studies = ['Pat1Test']

## include a path to save downloaded data segments to file
# path = 'D:/KAGGLE/data/ecosystem/test_download/' # replace with preferred path
path = './test_download/' # replace with preferred path


if __name__ == '__main__':

    GMT_OFFSET = 11 # Melb time
    base_date_time = datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc) + timedelta(hours=-GMT_OFFSET)
    try:
        min_date_time = (base_date_time + timedelta(hours=segment_min)).timestamp() * 1000
        max_date_time = (base_date_time + timedelta(hours=segment_max+1)).timestamp() * 1000
    except NameError:
        print('No segment filter provided (downloading all data)')
        min_date_time = None
        max_date_time = None

    client = seerpy.SeerConnect()

    for study in studies:
        directory = path + study

        # we could use exist_ok=True in python >= 3.2, but this should also work in python 2.7
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except OSError as e: # Guard against race condition
                if e.errno != errno.EEXIST:
                    raise

        print('\nStudy: ', study)
        print('  Retrieving metadata...')

        all_data = None
        all_data = client.get_all_study_meta_data_dataframe_by_names(study)

        #return values in uV
        all_data['channelGroups.exponent'] = 0

        if min_date_time is not None and max_date_time is not None:
            all_data = all_data[(all_data.loc[:, 'segments.startTime'] >= min_date_time)
                                & (all_data.loc[:, 'segments.startTime'] <= max_date_time)]

        unique_start_times = all_data['segments.startTime'].drop_duplicates()

        num_files = len(unique_start_times)
        print('  Downloading %d file(s)...' % num_files)
        counter = 1

        for start_time_ms in unique_start_times:

            start_date_time = datetime.fromtimestamp(start_time_ms/1000, tz=timezone.utc)
            hour = (start_date_time - base_date_time).total_seconds() / 3600
            minute = start_date_time.minute
            if minute >= 30:
                preictal = 1
            else:
                preictal = 0

            filename = directory + '/' + study + '_' + str(int(hour)) + '_' + str(preictal)

            # write out a refreshing profress line
            progress = ('   -> %s (%d/%d)' % (filename, counter, num_files) + ' ' * 20)
            sys.stdout.write('\r' + progress)
            sys.stdout.flush()

            ## Using threads>1 may speed up your downloads, but may also cause issues on Windows.
            ## Use with caution.
            data = client.get_links(all_data[all_data['segments.startTime'] == start_time_ms],
                                    threads=5)

            ######################
            # Change this section for saving data segments as different file formats

            #for csv format
            data.to_csv(filename + '.csv', index=False, float_format='%.3f')

            ##for hdf5 format
            # data.to_hdf(filename + '.hdf5', key='data', format='table')

            ##for matlab files
            # savemat(filename + '.mat', {'data': np.asarray(data.iloc[:, -16:], dtype=np.float32)},
            #         appendmat=False, do_compression=False)

            counter += 1

        # write out a refreshing profress line
        progress = ('  Finished downloading study.' + ' ' * 20)
        sys.stdout.write('\r' + progress)
        sys.stdout.flush()
        print('')
