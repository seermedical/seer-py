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
from time import gmtime, strftime



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
        
        self.lastQueryTime = 0
        self.apiLimitExpire = 300
        self.apiLimit = 240

    def execute_query(self, queryString, invocations=0):
        rate_limit_errors = ['503 Server Error', '502 Server Error']

        try:
            time.sleep(max(0, (self.apiLimitExpire/self.apiLimit)-(time.time()-self.lastQueryTime)))
            response = self.graphqlClient.execute(gql(queryString))
            self.lastQueryTime = time.time()
            return response
        except Exception as e:
            if invocations > 6:
                print('Too many failed query invocations. raising error')
                raise
            error_string = str(e)
            if any(rate_limit_error in error_string for rate_limit_error in rate_limit_errors):
                print(error_string + ' raised, trying again after a short break')
                time.sleep(30 * (invocations+1)**2)
                invocations += 1
                return self.execute_query(queryString, invocations=invocations)
            raise

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
        queryString = graphql.removeLabelGroupMutationString(groupId)
        return self.execute_query(queryString)

    def addLabel(self, groupId, startTime, duration, timezone, confidence = None):
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
        [optional] confidence : float (between 0 and 1)
                optional value for confidence in label

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
        queryString = graphql.addLabelMutationString(groupId, startTime, duration, timezone, confidence)
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
                [optional] confidence : float (between 0 and 1)
                        value for confidence in label 
        Returns
        -------
        None

        Notes
        -----

        """
        queryString = graphql.addLabelsMutationString(groupId, labels)
        return self.execute_query(queryString)

    def getStudies(self, limit=50, offset=0, searchTerm=''):
        studies = []
        while True:
            queryString = graphql.studyListQueryString(limit, offset, searchTerm)
            response = self.execute_query(queryString)['studies']
            if len(response) == 0:
                break
            else:
                studies = studies + response
            offset += limit
        return studies

    def getStudy(self, studyID):
        queryString = graphql.studyQueryString(studyID)
        response = self.execute_query(queryString)
        return response['study']

    def getChannelGroups(self, studyID):
        queryString = graphql.channelGroupsQueryString(studyID)
        response = self.execute_query(queryString)
        return response['study']['channelGroups']
    
    def getDataChunks(self, studyId, channelGroupId, fromTime=0, toTime=9e12):
        queryString = graphql.dataChunksQueryString(studyId, channelGroupId, fromTime, toTime)
        response = self.execute_query(queryString)['study']['channelGroup']
        response = json_normalize(response['segments'])
        dataChunks = self.pandasFlatten(response, '', 'dataChunks')
        return dataChunks
    
    def getLabels(self, studyId, labelGroupId, fromTime=0, toTime=9e12,
                  limit=200, offset=0):
        
        labelResults = None
        
        while True:
            queryString = graphql.getLabesQueryString(studyId, labelGroupId, fromTime,
                                                      toTime, limit, offset)
            response = self.execute_query(queryString)['study']
            labelGroup = json_normalize(response)
            labels = self.pandasFlatten(labelGroup, 'labelGroup.', 'labels')
            if len(labels) == 0:
                break
            tags = self.pandasFlatten(labels, 'labels.', 'tags')
#            tagType = self.pandasFlatten(tags, 'tags.', 'tagType')
#            category = self.pandasFlatten(tagType, 'tagType.', 'category')
            
            if 'labelGroup.labels' in labelGroup.columns: del labelGroup['labelGroup.labels']
            if 'labels.tags' in labels.columns: del labels['labels.tags']
#            if 'tags.tagType' in tags.columns: del tags['tags.tagType']
#            if 'tagType.category' in tagType.columns: del tagType['tagType.category']
            
            try:
                labelGroup  = labelGroup.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
                labelGroup  = labelGroup.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))
#                labelGroup  = labelGroup.merge(tagType, how='left', on='tags.id', suffixes=('', '_y'))
#                labelGroup  = labelGroup.merge(category, how='left', on='tagType.id', suffixes=('', '_y'))
            except Exception as e:
    #            print(e)
                pass
            
            offset += limit
            
            if labelResults is None:
                labelResults = labelGroup.copy()
            else:
                labelResults = labelResults.append(labelGroup, ignore_index=True, verify_integrity=False)
        return labelResults
    
    def getLabelGroups(self, studyID):
        queryString = graphql.labelGroupsQueryString(studyID)
        response = self.execute_query(queryString)
        return response['study']['labelGroups']

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
        studies = self.getStudies(searchTerm=searchTerm)
        studiesToGet = []

        for s in studies:
            if study is not None:
                if s['name'] == study:
                    studiesToGet.append(s['id'])
            else:
                studiesToGet.append(s['id'])

        result = []

        for sdy in studiesToGet:
#            t = time.time()
            queryString = graphql.studyWithDataQueryString(sdy)
            result.append(self.execute_query(queryString)['study'])
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

        if 'segments.dataChunks' in segments.columns: del segments['segments.dataChunks']
        if 'channelGroups.segments' in channelGroups.columns: del channelGroups['channelGroups.segments']
        if 'channelGroups.channels' in channelGroups.columns: del channelGroups['channelGroups.channels']
        if 'channelGroups' in allData.columns: del allData['channelGroups']
        if 'labelGroups' in allData.columns: del allData['labelGroups']

        channelGroupsM  = channelGroups.merge(segments, how='left', on='channelGroups.id', suffixes=('', '_y'))
        channelGroupsM  = channelGroupsM.merge(channels, how='left', on='channelGroups.id', suffixes=('', '_y'))
        allData         = allData.merge(channelGroupsM, how='left', on='id', suffixes=('', '_y'))

        return allData

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

                    metaData = metaData.drop_duplicates('segments.id')
                    
                    fromTime = metaData['segments.startTime'].min()
                    toTime = fromTime + metaData['segments.duration'].sum()
                    dataChunks = self.getDataChunks(studyID, channelGroupsID, fromTime, toTime)
                    metaData = metaData.merge(dataChunks, how='left', left_on='segments.id', right_on='id', suffixes=('', '_y'))
                    
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
