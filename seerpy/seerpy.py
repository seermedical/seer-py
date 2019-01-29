# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from multiprocessing import Pool
import os
import time
from time import gmtime, strftime

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from .auth import SeerAuth
from .utils import downloadLink
from . import graphql


class SeerConnect:  # pylint: disable=too-many-public-methods

    def __init__(self, apiUrl='https://api.seermedical.com', email=None, password=None):
        """Creates a GraphQL client able to interact with
            the Seer database, handling login and authorisation
        Parameters
        ----------
        None

        Returns
        -------

        Notes
        -----

        Example
        -------

        """

        self.apiUrl = apiUrl
        self.login(email, password)

        self.lastQueryTime = 0
        self.apiLimitExpire = 300
        self.apiLimit = 240

    def login(self, email=None, password=None):
        self.seerAuth = SeerAuth(self.apiUrl, email, password)
        cookie = self.seerAuth.cookie
        header = {'Cookie': list(cookie.keys())[0] + '=' + cookie['seer.sid']}
        self.graphqlClient = GQLClient(
            transport=RequestsHTTPTransport(
                url=self.apiUrl + '/api/graphql',
                headers=header,
                use_json=True,
                timeout=30
            )
        )

    def execute_query(self, queryString, invocations=0):
        rate_limit_errors = ['503 Server Error', '502 Server Error']

        try:
            time.sleep(max(0, (self.apiLimitExpire/self.apiLimit)-(time.time()-self.lastQueryTime)))
            response = self.graphqlClient.execute(gql(queryString))
            self.lastQueryTime = time.time()
            return response
        except Exception as e:
            if invocations > 4:
                print('Too many failed query invocations. raising error')
                raise
            error_string = str(e)
            if any(rate_limit_error in error_string for rate_limit_error in rate_limit_errors):
                print(error_string + ' raised, trying again after a short break')
                time.sleep(30 * (invocations+1)**2)
                invocations += 1
                self.login()
                return self.execute_query(queryString, invocations=invocations)

            if 'NOT_AUTHENTICATED' in str(e):
                self.seerAuth.destroyCookie()
                self.login()
                invocations += 1
                return self.execute_query(queryString, invocations=invocations)

            if 'Read timed out.' in str(e):
                print(error_string + ' raised, trying again after a short break')
                time.sleep(30 * (invocations+1)**2)
                invocations += 1
                self.login()
                return self.execute_query(queryString, invocations=invocations)
            raise

    def get_paginated_response(self, query_string, object_name, limit=250):
        offset = 0
        objects = []
        while True:
            formatted_query_string = query_string.format(limit=limit, offset=offset)
            response = self.execute_query(formatted_query_string)[object_name]
            if not response:
                break
            else:
                objects = objects + response
            offset += limit
        return objects

    def execute_custom_query(self, query_string):
        return self.execute_query(query_string)

    def addLabelGroup(self, studyId, name, description, labelType=None):
        """Add Label Group to study

        Parameters
        ----------
        studyID : string
                Seer study ID
        labelTypeId : string
                Seer label type ID
        name : string
                name of label
        description : string
                description of label

        Returns
        -------
        labelGroupID : string
                ID of label group

        Notes
        -----

        Example
        -------
        labelGroup = addLabelGroup(studyId, labelGroupName, labelGroupDescription)

        """
        queryString = graphql.get_add_label_group_mutation_string(studyId, name, description, labelType)
        response = self.execute_query(queryString)
        return response['addLabelGroupToStudy']['id']

    def delLabelGroup(self, groupId):
        """Delete Label Group from study

        Parameters
        ----------
        groupID : string
                Seer label group ID to delete

        Returns
        -------
        labelGroupID : string
                ID of deleted label group

        Notes
        -----

        Example
        -------
        delLG = delLabelGroup(GroupID)

        """
        queryString = graphql.get_remove_label_group_mutation_string(groupId)
        return self.execute_query(queryString)

    def addLabels(self, groupId, labels):
        """Add label to label group

        Parameters
        ----------
        groupID : string
                Seer group ID

        labels: list of:
                startTime : float
                        label start time in epoch time
                duration : float
                        duration of event in milliseconds
                timezone : float
                        local UTC timezone (eg. Melbourne = 11.0)

        Returns
        -------
        None

        Notes
        -----

        """
        if isinstance(labels, pd.DataFrame):
            labels = labels.to_dict('records')
        queryString = graphql.get_add_labels_mutation_string(groupId, labels)
        return self.execute_query(queryString)

    def get_tag_ids(self):
        queryString = graphql.get_tag_id_query_string()
        response = self.execute_query(queryString)
        return response['labelTags']

    def get_tag_ids_dataframe(self):
        tag_ids = self.get_tag_ids()
        tag_ids = json_normalize(tag_ids)
        return tag_ids


    def get_studies(self, limit=50, searchTerm=''):
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(searchTerm)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def get_studies_dataframe(self, limit=50, searchTerm=''):
        studies = self.get_studies(limit, searchTerm)

        studyList = []
        for s in studies:
            study = {}
            study['id'] = s['id']
            study['name'] = s['name']
            if s['patient'] is not None:
                study['patient.id'] = s['patient']['id']
            studyList.append(study)
        return pd.DataFrame(studyList)

    def get_study_ids_from_names_dataframe(self, study_names):
        if isinstance(study_names, str):
            study_names = [study_names]
        studies = self.get_studies_dataframe()
        return studies[studies['name'].isin(study_names)][['name', 'id']].reset_index(drop=True)

    def get_studies_by_id(self, study_ids, limit=50):
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        studies_query_string = graphql.get_studies_by_study_id_paged_query_string(study_ids)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def getChannelGroups(self, studyID):
        queryString = graphql.get_channel_groups_query_string(studyID)
        response = self.execute_query(queryString)
        return response['study']['channelGroups']

    def getSegmentUrls(self, segmentIds, limit=10000):
        segmentUrls = pd.DataFrame()
        counter = 0
        while int(counter * limit) < len(segmentIds):
            segmentIdsBatch = segmentIds[int(counter * limit):int((counter + 1) * limit)]
            queryString = graphql.get_segment_urls_query_string(segmentIdsBatch)
            response = self.execute_query(queryString)
            response = response['studyChannelGroupSegments']
            response = [i for i in response if i is not None]
            response = pd.DataFrame(response)
            segmentUrls = segmentUrls.append(response)
            counter += 1
        segmentUrls = segmentUrls.rename(columns={'id': 'segments.id'})
        return segmentUrls.reset_index(drop=True)


    def get_labels(self, study_id, label_group_id, from_time=0,  # pylint:disable=too-many-arguments
                   to_time=9e12, limit=200, offset=0):

        label_results = None

        while True:
            query_string = graphql.get_labels_query_string(study_id, label_group_id, from_time,
                                                           to_time, limit, offset)
            response = self.execute_query(query_string)['study']
            labels = response['labelGroup']['labels']
            if not labels:
                break

            if label_results is None:
                label_results = response
            else:
                label_results['labelGroup']['labels'].extend(labels)

            offset += limit

        return label_results


    def get_labels_dataframe(self, study_id, label_group_id,  # pylint:disable=too-many-arguments
                             from_time=0, to_time=9e12, limit=200, offset=0):

        label_results = self.get_labels(study_id, label_group_id, from_time, to_time, limit, offset)
        label_group = json_normalize(label_results)
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_group = label_group.drop('labelGroup.labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')

        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

        return label_group


    def getLabels(self, study_id, label_group_id, from_time=0,  # pylint:disable=too-many-arguments
                  to_time=9e12, limit=250, offset=0):

        return self.get_labels_dataframe(study_id, label_group_id, from_time, to_time, limit,
                                         offset)


    def get_label_groups_for_studies(self, study_ids, limit=50):
        if isinstance(study_ids, str):
            study_ids = [study_ids]

        labels_query_string = graphql.get_label_groups_for_study_ids_paged_query_string(study_ids)
        return self.get_paginated_response(labels_query_string, 'studies', limit)

    def get_label_groups_for_studies_dataframe(self, study_ids, limit=50):
        label_groups = []
        for study in self.get_label_groups_for_studies(study_ids, limit):
            for label_group in study['labelGroups']:
                label_group['labelGroup.id'] = label_group.pop('id')
                label_group['labelGroup.name'] = label_group.pop('name')
                label_group['id'] = study['id']
                label_group['name'] = study['name']
                label_groups.append(label_group)
        return pd.DataFrame(label_groups)

    def getViewedTimes(self, studyID):
        queryString = graphql.get_viewed_times_query_string(studyID)
        response = self.execute_query(queryString)
        response = json_normalize(response['viewGroups'])
        views = pd.DataFrame(columns=['createdAt', 'duration', 'id', 'startTime', 'updatedAt',
                                      'user', 'viewTimes'])
        for i in range(len(response)):
            view = json_normalize(response.loc[i, 'views'])
            view['user'] = response.loc[i, 'user.fullName']
            views = views.append(view)

        views['createdAt'] = pd.to_datetime(views['createdAt'])
        views['updatedAt'] = pd.to_datetime(views['updatedAt'])
        return views

    def getAllMetaData(self, study=None):
        """Get all the data available to user in the form of
        pandas DataFrames

        Parameters
        ----------
        patient (Optional) : name of patient

        Returns
        -------
        allData : pandas DataFrame
        channelGroups : pandas DataFrame
        segments : pandas DataFrame
        channels : pandas DataFrame
        dataChunks : pandas DataFrame
        labelGroups : pandas DataFrame
        labels : pandas DataFrame

        Notes
        -----

        Example
        -------
        allData, channelGroups, segments, channels, dataChunks,
        labelGroups, labels = SeerConnect.createMetaData()

        """

        searchTerm = study if study is not None else ''
        studies = self.get_studies_dataframe(searchTerm=searchTerm)

        if study is not None:
            studies = studies.loc[studies['name'] == study]

        result = []
        for row in studies.itertuples():
            # t = time.time()
            queryString = graphql.get_study_with_data_query_string(row.id)
            result.append(self.execute_query(queryString)['study'])
            # print('study query time: ', round(time.time()-t,2))

        return {'studies' : result}

    @staticmethod  # maybe this could move to a utility class
    def pandas_flatten(parent, parentName, childName):
        childList = []
        for i in range(len(parent)):
            parentId = parent[parentName+'id'][i]
            child = json_normalize(parent[parentName+childName][i])
            child.columns = [childName+'.' + str(col) for col in child.columns]
            child[parentName+'id'] = parentId
            childList.append(child)

        if childList:
            child = pd.concat(childList).reset_index(drop=True)
        if not childList or child.empty:
            columns = [parentName + 'id', childName + '.id']
            child = pd.DataFrame(columns=columns)
        return child

    def createMetaData(self, study=None):
        dataUrlsAll = self.getAllMetaData(study)
        allData = json_normalize(dataUrlsAll['studies'])
        channelGroups = self.pandas_flatten(allData, '', 'channelGroups')
        channels = self.pandas_flatten(channelGroups, 'channelGroups.', 'channels')
        segments = self.pandas_flatten(channelGroups, 'channelGroups.', 'segments')

        segments = segments.drop('segments.dataChunks', errors='ignore', axis='columns')
        channelGroups = channelGroups.drop(['channelGroups.segments', 'channelGroups.channels'],
                                           errors='ignore', axis='columns')
        allData = allData.drop(['channelGroups', 'labelGroups'], errors='ignore', axis='columns')

        channelGroupsM = channelGroups.merge(segments, how='left', on='channelGroups.id',
                                             suffixes=('', '_y'))
        channelGroupsM = channelGroupsM.merge(channels, how='left', on='channelGroups.id',
                                              suffixes=('', '_y'))
        allData = allData.merge(channelGroupsM, how='left', on='id', suffixes=('', '_y'))

        return allData

    # pylint:disable=too-many-locals
    @staticmethod
    def createDataChunkUrls(metaData, segmentUrls, fromTime=0, toTime=9e12):
        chunkPattern = '00000000000.dat'
        dataChunks = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])
        metaData = metaData.drop_duplicates('segments.id')
        for _, row in metaData.iterrows():
            segBaseUrls = segmentUrls.loc[segmentUrls['segments.id'] == row['segments.id'],
                                          'baseDataChunkUrl']
            if segBaseUrls.empty:
                continue
            segBaseUrl = segBaseUrls.iloc[0]

            chunk_period = row['channelGroups.chunkPeriod']
            num_chunks = int(np.ceil(row['segments.duration'] / chunk_period / 1000.))
            start_time = row['segments.startTime']
            for i in range(num_chunks):
                chunk_start_time = chunk_period * 1000 * i + start_time
                next_chunk_start_time = chunk_period * 1000 * (i + 1) + start_time
                if (chunk_start_time <= toTime and next_chunk_start_time >= fromTime):
                    dataChunkName = str(i).zfill(len(chunkPattern) - 4) + chunkPattern[-4:]
                    dataChunk = pd.DataFrame(columns=['segments.id', 'dataChunks.url',
                                                      'dataChunks.time'])
                    dataChunk['dataChunks.url'] = [segBaseUrl.replace(chunkPattern, dataChunkName)]
                    dataChunk['dataChunks.time'] = [chunk_start_time]
                    dataChunk['segments.id'] = [row['segments.id']]
                    dataChunks = dataChunks.append(dataChunk)

        return dataChunks.reset_index(drop=True)

    def getLinks(self, allData, segmentUrls=None, threads=None,  # pylint:disable=too-many-arguments
                 fromTime=0, toTime=9e12):
        """Download data chunks and stich them together in one dataframe

        Parameters
        ----------
        allData : pandas DataFrame
                Dataframe containing metadata required for downloading and
                processing raw data

        threads : number of threads to use. If > 1 then will use multiprocessing
                    if None (default), it will use 1 on Windows and 5 on Linux/MacOS

        Returns
        -------
        data : pandas DataFrame
                dataframe containing studyID, channelGroupIDs, semgemtIDs, time, and raw data

        Notes
        -----

        Example
        -------
        data = getLinks(allData.copy())

        """
        if threads is None:
            if os.name != 'nt':
                threads = 1
            else:
                threads = 5

        if segmentUrls is None:
            segmentIds = allData['segments.id'].unique().tolist()
            segmentUrls = self.getSegmentUrls(segmentIds)

        dataQ = []

#        uniqueUrls = allData['dataChunks.url'].copy().drop_duplicates()
        for segment_id in allData['segments.id'].copy().drop_duplicates().tolist():
            meta_data = allData[allData['segments.id'] == segment_id].copy()

            num_channels = len(meta_data['channels.id'].copy().drop_duplicates().tolist())
            channel_names = meta_data['channels.name'].copy().drop_duplicates().tolist()

            actual_channel_names = channel_names
            if len(channel_names) != num_channels:
                actual_channel_names = ['Channel %s' % (i) for i in range(0, num_channels)]

            meta_data = meta_data.drop_duplicates('segments.id')

            study_id = meta_data['id'].iloc[0]
            channel_groups_id = meta_data['channelGroups.id'].iloc[0]

            data_chunks = self.createDataChunkUrls(meta_data, segmentUrls, fromTime=fromTime,
                                                   toTime=toTime)
            meta_data = meta_data.merge(data_chunks, how='left', left_on='segments.id',
                                        right_on='segments.id', suffixes=('', '_y'))

            meta_data = meta_data[['dataChunks.url', 'dataChunks.time',
                                   'channelGroups.sampleEncoding', 'channelGroups.sampleRate',
                                   'channelGroups.samplesPerRecord',
                                   'channelGroups.recordsPerChunk',
                                   'channelGroups.compression', 'channelGroups.signalMin',
                                   'channelGroups.signalMax', 'channelGroups.exponent']]
            meta_data = meta_data.drop_duplicates()
            meta_data = meta_data.dropna(axis=0, how='any', subset=['dataChunks.url'])
            for r in range(meta_data.shape[0]):
                dataQ.append([meta_data.iloc[r, :], study_id, channel_groups_id, segment_id,
                              actual_channel_names])

        if threads > 1:
            pool = Pool(processes=threads)
            data_list = list(pool.map(downloadLink, dataQ))
            pool.close()
            pool.join()
        else:
#            dataList = list(map(downloadLink, dataQ))
            data_list = [downloadLink(dataQ[i]) for i in range(len(dataQ))]

        if data_list:
            data = pd.concat(data_list)
            data = data.loc[(data['time'] >= fromTime) & (data['time'] < toTime)]
            data = data.sort_values(['id', 'channelGroups.id', 'segments.id', 'time'], axis=0,
                                    ascending=True, na_position='last')
        else:
            data = None

        return data

    @staticmethod
    def makeLabel(label, times, timezone=None):
        if timezone is None:
            timezone = int(int(strftime("%z", gmtime()))/100)
        labels = []
        labelOn = 0
        labelStart = 0.0
        labelEnd = 0.0
        for i in range(label.shape[0]):
            if labelOn == 0 and label[i] > 0.5:
                labelStart = times[i]
                labelOn = 1
            if labelOn == 1 and label[i] < 0.5:
                labelEnd = times[i]
                labelOn = 0
                labels.append([labelStart, labelEnd - labelStart, timezone])
        if labelOn == 1:
            labels.append([labelStart, labelEnd - labelStart, timezone])
        return labels

    @staticmethod
    def applyMovAvg(x, w):
        if len(x.shape) == 1:
            x = x.reshape(-1, 1)
        wn = int(w / 2.0)
        xn = np.zeros(x.shape, dtype=np.float32)
        for i in range(wn, x.shape[0] - wn):
            xn[i, :] = np.mean(np.abs(x[i-wn:i+wn, :]), axis=0)
        return xn
