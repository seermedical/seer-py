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


class SeerConnect:

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
        self.apiLimit = 245

    def login(self, email=None, password=None):
        self.seerAuth = SeerAuth(self.apiUrl, email, password)
        cookie = self.seerAuth.cookie
        header = {'Cookie': list(cookie.keys())[0] + '=' + cookie['seer.sid']}
        self.graphqlClient = GQLClient(
            transport=RequestsHTTPTransport(
                url=self.apiUrl + '/api/graphql',
                headers=header,
                use_json=True,
                timeout=60
            )
        )

    def execute_query(self, queryString, invocations=0):
        rate_limit_errors = ['503 Server Error', '502 Server Error']

        try:
            time.sleep(max(0, (self.apiLimitExpire/self.apiLimit)-(time.time()-self.lastQueryTime)))
            return self.graphqlClient.execute(gql(queryString))
        except Exception as e:
            if invocations > 4:
                print('Too many failed query invocations. raising error')
                raise
            error_string = str(e)
            if any(rate_limit_error in error_string for rate_limit_error in rate_limit_errors):
                print(error_string + ' raised, trying again after a short break')
                time.sleep(30 * (invocations+1)**2)
                invocations += 1
                return self.execute_query(queryString, invocations=invocations)

            if 'NOT_AUTHENTICATED' in str(e):
                self.seerAuth.destroyCookie()
                self.login()
                invocations += 1
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

    def addLabelGroup(self, studyId, name, description):
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
        queryString = graphql.get_add_label_group_mutation_string(studyId, name, description)
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
        queryString = graphql.get_add_labels_mutation_string(groupId, labels)
        return self.execute_query(queryString)

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
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(study_ids)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def getChannelGroups(self, studyID):
        queryString = graphql.get_channel_groups_query_string(studyID)
        response = self.execute_query(queryString)
        return response['study']['channelGroups']

    def getSegmentUrls(self, segmentIds, limit=10000):
        segmentUrls = pd.DataFrame()
        counter = 0
        while int(counter*limit) < len(segmentIds):
            segmentIdsBatch = segmentIds[int(counter*limit):int((counter+1)*limit)]
            queryString = graphql.get_segment_urls_query_string(segmentIdsBatch)
            response = self.execute_query(queryString)
            response = response['studyChannelGroupSegments']
            response = [i for i in response if i is not None]
            response = pd.DataFrame(response)
            segmentUrls = segmentUrls.append(response)
            counter += 1
        segmentUrls = segmentUrls.rename(columns={'id': 'segments.id'})
        return segmentUrls

    def getLabels(self, studyId, labelGroupId, fromTime=0, toTime=9e12, limit=200, offset=0):

        labelResults = None

        while True:
            while True:
                queryString = graphql.get_labels_query_string(studyId, labelGroupId, fromTime, toTime,
                                                           limit, offset)
                print("queryString", queryString)
                response = self.execute_query(queryString)['study']
                labelGroup = json_normalize(response)
                labels = self.pandas_flatten(labelGroup, 'labelGroup.', 'labels')
                break
            if not labels:
                break

            tags = self.pandas_flatten(labels, 'labels.', 'tags')

            labelGroup.drop('labelGroup.labels', inplace=True, errors='ignore')
            labels.drop('labels.tags', inplace=True, errors='ignore')

            labelGroup = labelGroup.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
            labelGroup = labelGroup.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

            offset += limit

            if labelResults is None:
                labelResults = labelGroup
            else:
                labelResults = labelResults.append(labelGroup, ignore_index=True,
                                                   verify_integrity=False)
        return labelResults

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
        views = pd.DataFrame(columns=['createdAt', 'duration', 'id', 'startTime', 'updatedAt', 'user', 'viewTimes'])
        for i in range(len(response)):
            view = json_normalize(response.loc[i,'views'])
            view['user'] = response.loc[i,'user.fullName']
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

    def pandas_flatten(self, parent, parentName, childName):
        childList = []
        for i in range(len(parent)):
            parentId = parent[parentName+'id'][i]
            child = json_normalize(parent[parentName+childName][i])
            child.columns = [childName+'.' + str(col) for col in child.columns]
            child[parentName+'id'] = parentId
            childList.append(child)

        if childList:
            child = pd.concat(childList)
            child.reset_index(drop=True, inplace=True)
        if not childList or child.empty:
            columns = [parentName + 'id', childName + '.id']
            child = pd.DataFrame(columns=columns)
        return child

    def createMetaData(self, study=None):
        dataUrlsAll     = self.getAllMetaData(study)
        allData         = json_normalize(dataUrlsAll['studies'])
        channelGroups   = self.pandas_flatten(allData, '', 'channelGroups')
        channels        = self.pandas_flatten(channelGroups, 'channelGroups.', 'channels')
        segments        = self.pandas_flatten(channelGroups, 'channelGroups.', 'segments')

        segments.drop(columns='segments.dataChunks', inplace=True, errors='ignore')
        channelGroups.drop(columns=['channelGroups.segments', 'channelGroups.channels'],
                           inplace=True, errors='ignore')
        allData.drop(columns=['channelGroups', 'labelGroups'], inplace=True, errors='ignore')

        channelGroupsM  = channelGroups.merge(segments, how='left', on='channelGroups.id', suffixes=('', '_y'))
        channelGroupsM  = channelGroupsM.merge(channels, how='left', on='channelGroups.id', suffixes=('', '_y'))
        allData         = allData.merge(channelGroupsM, how='left', on='id', suffixes=('', '_y'))

        return allData

    def createDataChunkUrls(self, metaData, segmentUrls, fromTime=0, toTime=9e12):
        chunkPattern = '00000000000.dat'
        dataChunks = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])
        metaData = metaData.drop_duplicates('segments.id')
        for index, row in metaData.iterrows():
            segBaseUrl = segmentUrls.loc[segmentUrls['segments.id']==row['segments.id'],'baseDataChunkUrl'].iloc[0]
            numOfChunks = int(np.ceil(row['segments.duration'] / row['channelGroups.chunkPeriod'] / 1000.))
            for i in range(numOfChunks):
                if (row['channelGroups.chunkPeriod']* 1000 * i + row['segments.startTime'] <= toTime and
                    row['channelGroups.chunkPeriod']* 1000 * (i + 1) + row['segments.startTime'] >= fromTime):
                    dataChunkName = str(i).zfill(len(chunkPattern)-4) + chunkPattern[-4:]
                    dataChunk = pd.DataFrame(columns=['segments.id', 'dataChunks.url', 'dataChunks.time'])
                    dataChunk['dataChunks.url'] = [segBaseUrl.replace(chunkPattern, dataChunkName)]
                    dataChunk['dataChunks.time'] = [row['channelGroups.chunkPeriod']* 1000 * i + row['segments.startTime']]
                    dataChunk['segments.id'] = [row['segments.id']]
                    dataChunks = dataChunks.append(dataChunk)
        return dataChunks


    def getLinks(self, allData, segmentUrls=None, threads=None, fromTime=0, toTime=9e12):
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
        for studyID in allData['id'].copy().drop_duplicates().tolist():
            for channelGroupsID in allData['channelGroups.id'].copy().drop_duplicates().tolist():
                for segmentsID in allData['segments.id'].copy().drop_duplicates().tolist():
                    metaData = allData[(allData['id']==studyID) & (allData['channelGroups.id']==channelGroupsID) & (allData['segments.id']==segmentsID)].copy()

                    numChannels = len(metaData['channels.id'].copy().drop_duplicates().tolist())
                    channelNames = metaData['channels.name'].copy().drop_duplicates().tolist()
                    actualChannelNames = channelNames if len(channelNames) == numChannels else ['Channel %s' % (i) for i in range(0, numChannels)]

                    metaData = metaData.drop_duplicates('segments.id')

                    dataChunks = self.createDataChunkUrls(metaData, segmentUrls, fromTime=fromTime, toTime=toTime)
                    metaData = metaData.merge(dataChunks, how='left', left_on='segments.id', right_on='segments.id', suffixes=('', '_y'))

                    metaData = metaData[['dataChunks.url', 'dataChunks.time', 'channelGroups.sampleEncoding', 'channelGroups.sampleRate', 'channelGroups.samplesPerRecord',
                                         'channelGroups.recordsPerChunk', 'channelGroups.compression', 'channelGroups.signalMin', 'channelGroups.signalMax', 'channelGroups.exponent']]
                    metaData = metaData.drop_duplicates()
                    metaData = metaData.dropna(axis=0, how='any', subset=['dataChunks.url'])
                    for r in range(metaData.shape[0]):
                        dataQ.append([metaData.iloc[r, :], studyID, channelGroupsID, segmentsID, actualChannelNames])

        if threads > 1:
            pool = Pool(processes=threads)
            dataList = list(pool.map(downloadLink, dataQ))
            pool.close()
            pool.join()
        else:
#            dataList = list(map(downloadLink, dataQ))
            dataList = [downloadLink(dataQ[i]) for i in range(len(dataQ))]
        if len(dataList)>0:
            data = pd.concat(dataList)
            data = data.loc[(data['time']>=fromTime) & (data['time']<toTime)]
            data = data.sort_values(['id', 'channelGroups.id', 'segments.id', 'time'], axis=0, ascending=True,
                                    inplace=False, kind='quicksort', na_position='last')
        else:
            data = None
        return data

    def makeLabel(self, label, time, timezone=None):
        if timezone is None:
            timezone = int(int(strftime("%z", gmtime()))/100)
        labels = []
        labelOn = 0
        labelStart = 0.0
        labelEnd = 0.0
        for i in range(label.shape[0]):
            if labelOn==0 and label[i]>0.5:
                labelStart = time[i]
                labelOn = 1
            if labelOn==1 and label[i]<0.5:
                labelEnd = time[i]
                labelOn = 0
                labels.append([labelStart, labelEnd-labelStart, timezone])
        if labelOn==1:
            labels.append([labelStart, labelEnd-labelStart, timezone])
        return labels

    def applyMovAvg(self, x, w):
        if len(x.shape) == 1:
            x = x.reshape(-1,1)
        wn=int(w/2.0)
        xn = np.zeros(x.shape, dtype=np.float32)
        for i in range(wn, x.shape[0]-wn):
            xn[i, :] = np.mean(np.abs(x[i-wn:i+wn, :]), axis=0)
        return xn
