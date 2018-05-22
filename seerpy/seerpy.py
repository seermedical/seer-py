# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from .auth import SeerAuth
from .utils import downloadLink
from . import graphql
import os

from multiprocessing import Pool
import time



class SeerConnect:

    def __init__(self):
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
        apiUrl = 'https://api.seermedical.com'
        cookie = SeerAuth(apiUrl).cookie
        header = {'Cookie': list(cookie.keys())[0] + '=' + cookie['seer.sid']}
        self.graphqlClient = GQLClient(
            transport=RequestsHTTPTransport(
                url=apiUrl + '/api/graphql',
                headers=header,
                use_json=True,
                timeout=60
            )
        )

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
        queryString = graphql.addLabelGroupMutationString(studyId, name, description)
        response = self.graphqlClient.execute(gql(queryString))
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
        queryString = graphql.removeLabelGroupMutationString(groupId)
        return self.graphqlClient.execute(gql(queryString))

    def addLabel(self, groupId, startTime, duration, timezone):
        """Add label to label group

        Parameters
        ----------
        groupID : string
                Seer group ID
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

        Example
        -------
        alarm = np.array([[1000, 200], [3000, 500], [5000, 400]])
        for i in range(alarm.shape[0]):
            addLabel(labelGroup, alarm[i,0], alarm[i,1]-alarm[i,0])

        """
        queryString = graphql.addLabelMutationString(groupId, startTime, duration, timezone)
        return self.graphqlClient.execute(gql(queryString))

    def getStudies(self):
        queryString = graphql.studyListQueryString()
        response = self.graphqlClient.execute(gql(queryString))
        return response['studies']

    def getStudy(self, studyID):
        queryString = graphql.studyQueryString(studyID)
        response = self.graphqlClient.execute(gql(queryString))
        return response['study']

    def getChannelGroups(self, studyID):
        queryString = graphql.channelGroupsQueryString(studyID)
        response = self.graphqlClient.execute(gql(queryString))
        return response['channelGroups']

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
        studies = self.getStudies()
        studiesToGet = []

        for s in studies:
            if study is not None:
                if s['name'] == study:
                    studiesToGet.append(s['id'])
            else:
                studiesToGet.append(s['id'])

        result = []

        for sdy in studiesToGet:
            t = time.time()
            queryString = graphql.studyWithDataQueryString(sdy)
            result.append(self.graphqlClient.execute(gql(queryString))['study'])
            # print('study query time: ', round(time.time()-t,2))

        return {'studies' : result}

    def pandasFlatten(self, parent, parentName, childName):
        childList = []
        for i in range(len(parent)):
            parentId = parent[parentName+'id'][i]
            child = json_normalize(parent[parentName+childName][i])
            child.columns = [childName+'.' + str(col) for col in child.columns]
            child[parentName+'id'] = parentId
            childList.append(child)

        if len(childList)==1:
            child = childList[0]
            child.reset_index(drop=True, inplace=True)
        elif len(childList)>0:
            child = pd.concat(childList)
            child.reset_index(drop=True, inplace=True)
        if len(childList) == 0 or len(child) == 0:
            if parentName == '':
                columns = ['id', childName + '.id']
            else:
                columns = [parentName + 'id']
            child = pd.DataFrame(columns=columns)
        return child

    def createMetaData(self, study=None):
        dataUrlsAll     = self.getAllMetaData(study)
        allData         = json_normalize(dataUrlsAll['studies'])
        channelGroups   = self.pandasFlatten(allData, '', 'channelGroups')
        channels        = self.pandasFlatten(channelGroups, 'channelGroups.', 'channels')
        segments        = self.pandasFlatten(channelGroups, 'channelGroups.', 'segments')
        dataChunks      = self.pandasFlatten(segments, 'segments.', 'dataChunks')
        labelGroups     = self.pandasFlatten(allData, '', 'labelGroups')
        labels          = self.pandasFlatten(labelGroups, 'labelGroups.', 'labels')

        if 'labelGroups.labels' in labelGroups.columns: del labelGroups['labelGroups.labels']
        if 'segments.dataChunks' in segments.columns: del segments['segments.dataChunks']
        if 'channelGroups.segments' in channelGroups.columns: del channelGroups['channelGroups.segments']
        if 'channelGroups.channels' in channelGroups.columns: del channelGroups['channelGroups.channels']
        if 'channelGroups' in allData.columns: del allData['channelGroups']
        if 'labelGroups' in allData.columns: del allData['labelGroups']

#        print('dataframes created')

#        labelGroupsM    = labelGroups.merge(labels, how='left', on='labelGroups.id', suffixes=('', '_y'))
        segmentsM       = segments.merge(dataChunks, how='left', on='segments.id', suffixes=('', '_y'))
        channelGroupsM  = channelGroups.merge(segmentsM, how='left', on='channelGroups.id', suffixes=('', '_y'))
        channelGroupsM  = channelGroupsM.merge(channels, how='left', on='channelGroups.id', suffixes=('', '_y'))
        allData         = allData.merge(channelGroupsM, how='left', on='id', suffixes=('', '_y'))
#        allData         = allData.merge(labelGroupsM, how='left', on='id', suffixes=('', '_y'))
#        print('dataframes merged')

        return [allData, channelGroups, segments, channels, dataChunks, labelGroups, labels]

    def getLinks(self, allData, threads=None):
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
        
        
        dataQ = []
#        uniqueUrls = allData['dataChunks.url'].copy().drop_duplicates()
        for studyID in allData['id'].copy().drop_duplicates().tolist():
            for channelGroupsID in allData['channelGroups.id'].copy().drop_duplicates().tolist():
                for segmentsID in allData['segments.id'].copy().drop_duplicates().tolist():
                    metaData = allData[(allData['id']==studyID) & (allData['channelGroups.id']==channelGroupsID) & (allData['segments.id']==segmentsID)].copy()
                    numChannels = len(metaData['channels.id'].copy().drop_duplicates().tolist())
                    channelNames = metaData['channels.name'].copy().drop_duplicates().tolist()
                    actualChannelNames = channelNames if len(channelNames) == numChannels else ['Channel %s' % (i) for i in range(0, numChannels)]
                    metaData = metaData[['dataChunks.url', 'dataChunks.time', 'channelGroups.sampleEncoding', 'channelGroups.sampleRate', 'channelGroups.samplesPerRecord',
                                         'channelGroups.recordsPerChunk', 'channelGroups.signalMin', 'channelGroups.signalMax']]
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
            dataList = list(map(downloadLink, dataQ))
        if len(dataList)>0:
            data = pd.concat(dataList)
            data = data.sort_values(['id', 'channelGroups.id', 'segments.id', 'time'], axis=0, ascending=True,
                                    inplace=False, kind='quicksort', na_position='last')
        else:
            data = None
        return data

    def makeLabel(self, label, time):
        labels = []
        labelOn = 0
        labelStart = 0.0
        labelEnd = 0.0
        for i in range(label.shape[0]):
            if labelOn==0 and label[i]!=0:
                labelStart = time[i]
                labelOn = 1
            if labelOn==1 and label[i]==0:
                labelEnd = time[i]
                labelOn = 0
                labels.append([labelStart, labelEnd])
        if labelOn==1:
            labels.append([labelStart, labelEnd])
        return np.asarray(labels, dtype=np.float64)

    def applyMovAvg(self, x, w):
        if len(x.shape) == 1:
            x = x.reshape(-1,1)
        wn=int(w/2.0)
        xn = np.zeros(x.shape, dtype=np.float32)
        for i in range(wn, x.shape[0]-wn):
            xn[i, :] = np.mean(np.abs(x[i-wn:i+wn, :]), axis=0)
        return xn
