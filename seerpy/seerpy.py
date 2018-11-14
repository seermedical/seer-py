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

err502 = '503 Server Error: Service Unavailable for url: https://api.seermedical.com/api/graphql'
err503 = '502 Server Error: Bad Gateway for url: https://api.seermedical.com/api/graphql'

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
        
        cookie = SeerAuth(apiUrl, email, password).cookie
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
        self.apiLimit = 250
    
    def executeQuery(self, queryString, invocations=0):
        try:
            time.sleep(max(0, (self.apiLimitExpire/self.apiLimit)-(time.time()-self.lastQueryTime)))
            return self.graphqlClient.execute(gql(queryString))
        except Exception as e:
            if invocations > 2:
                raise
            if str(e) in [err502, err503]:
                print(e + ' raised, trying again after a short break')
                time.sleep(30)
                invocations += 1
                self.executeQuery(queryString, invocations=invocations)
                
            else:
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
        response = self.executeQuery(queryString)
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
        return self.executeQuery(queryString)

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
        return self.executeQuery(queryString)
    
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
        queryString = graphql.addLabelsMutationString(groupId, labels)
        return self.executeQuery(queryString)

    def getStudies(self, limit=50, offset=0, searchTerm=''):
        studies = []
        while True:
            queryString = graphql.studyListQueryString(limit, offset, searchTerm)
            response = self.executeQuery(queryString)['studies']
            if len(response) == 0:
                break
            else:
                studies = studies + response
            offset += limit
        studyList = []
        for s in studies:
            study = {}
            study['id'] = s['id']
            study['name'] = s['name']
            if s['patient'] is not None:
                study['patient.id'] = s['patient']['id']
            studyList.append(study)
        studies = pd.DataFrame(studyList)
        return studies

    def getStudy(self, studyID):
        queryString = graphql.studyQueryString(studyID)
        response = self.executeQuery(queryString)
        return response['study']

    def getChannelGroups(self, studyID):
        queryString = graphql.channelGroupsQueryString(studyID)
        response = self.executeQuery(queryString)
        return response['study']['channelGroups']
    
    def getSegmentUrls(self, segmentIds, limit=10000):
        segmentUrls = pd.DataFrame()
        counter = 0
        while int(counter*limit) < len(segmentIds):
            segmentIdsBatch = segmentIds[int(counter*limit):int((counter+1)*limit)]
            queryString = graphql.segmentUrlsQueryString(segmentIdsBatch)
            response = self.executeQuery(queryString)
            response = response['studyChannelGroupSegments']
            response = [i for i in response if i is not None]
            response = pd.DataFrame(response)
            segmentUrls = segmentUrls.append(response)
            counter += 1
        segmentUrls = segmentUrls.rename(columns={'id': 'segments.id'})
        return segmentUrls
    
    def getDataChunks(self, studyId, channelGroupId, fromTime=0, toTime=9e12):
        queryString = graphql.dataChunksQueryString(studyId, channelGroupId, fromTime, toTime)
        response = self.executeQuery(queryString)['study']['channelGroup']
        response = json_normalize(response['segments'])
        dataChunks = self.pandasFlatten(response, '', 'dataChunks')
        return dataChunks
    
    def getLabels(self, studyId, labelGroupId, fromTime=0, toTime=9e12,
                  limit=200, offset=0):
        
        labelResults = None
        
        while True:
            while True:
                try:
                    queryString = graphql.getLabesQueryString(studyId, labelGroupId, fromTime,
                                                              toTime, limit, offset)
                    response = self.executeQuery(queryString)['study']
                    labelGroup = json_normalize(response)
                    labels = self.pandasFlatten(labelGroup, 'labelGroup.', 'labels')
                    break
                except Exception as e:
                    raise
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
    
    def getLabelGroup(self, studyID):
        queryString = graphql.labelGroupQueryString(studyID)
        response = self.executeQuery(queryString)
        return response['study']['labelGroups']
    
    def getLabelGroups(self, studyIDs, limit=50, offset=0):
        if type(studyIDs) == str:
            studyIDs = [studyIDs]
        studies = []
        while True:
            queryString = graphql.labelGroupsQueryString(limit, offset, studyIDs)
            response = self.executeQuery(queryString)['studies']
            if len(response) == 0:
                break
            else:
                studies = studies + response
            offset += limit
            print('lg len: ' + str(len(studies)))
        response = studies
        labelGroups = []
        for r in response:
            studyId = r['id']
            studyName = r['name']
            for l in r['labelGroups']:
                labelGroupId = l['id']
                labelGroupName = l['name']
                lg = {}
                lg['id'] = studyId
                lg['name'] = studyName
                lg['labelGroup.id'] = labelGroupId
                lg['labelGroup.name'] = labelGroupName
                labelGroups.append(lg)
#        response = self.pandasFlatten(response[0], '', 'labelGroups')
#        response = [self.pandasFlatten(r, '', 'labelGroups') for r in response]
        response = pd.DataFrame(labelGroups)
        return response

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
            result.append(self.executeQuery(queryString)['study'])
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
