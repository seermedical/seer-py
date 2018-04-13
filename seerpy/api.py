# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import requests
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from . import auth

from multiprocessing import Process, Queue
from multiprocessing import Manager
import time


def downloadLink(dataQ, dataList):
    while True:
        try:
            d = dataQ.get()
            if d is None:
                break
            else:
                metaData, studyID, channelGroupsID, segmentsID, channelNames = d
#                    t = time.time()
            data = requests.get(metaData['dataChunks.url'])
#                    print('chunk download time: ',round(time.time()-t,2))
            dataType = 'i' + str(int(metaData['channelGroups.bytesPerSample']))
#                    print(data.content)
#                        print(metaData['dataChunks.url'])
            data = np.fromstring(data.content, dtype=np.dtype(dataType))
            data = data.astype(np.float32)
            data = data.reshape(int(metaData['channelGroups.chunkPeriod']), -1, int(metaData['channelGroups.sampleRate']))
            data = np.transpose(data, (0, 2, 1))
            data = data.reshape(-1, data.shape[2])
            chanMin = metaData['channelGroups.signalMin'].astype(np.float64)
            chanMax = metaData['channelGroups.signalMax'].astype(np.float64)
            chanDiff = abs(chanMin) + abs(chanMax)
            digMin = np.iinfo(dataType).min
            digMax = np.iinfo(dataType).max
            digDiff = abs(digMin) + abs(digMax)
            with np.errstate(divide='ignore', invalid='ignore'):
#                                data = np.where(data>0.0, data/np.nanmax(data, axis=0)*chanMax, data/np.nanmin(data, axis=0)*chanMin)
                data = (data - digMin) / digDiff * chanDiff + chanMin
            data = np.nan_to_num(data).astype(np.float32)
            data = pd.DataFrame(data=data, index=None, columns=channelNames)
            timeLine = np.arange(data.shape[0]) * (1000.0/metaData['channelGroups.sampleRate']) + metaData['dataChunks.time']
            data['time'] = timeLine
            data['id'] = studyID
            data['channelGroups.id'] = channelGroupsID
            data['segments.id'] = segmentsID
            dataCols = ['time', 'id', 'channelGroups.id', 'segments.id'] + channelNames
            data = data[dataCols]
            dataList.append(data)
        except Exception as e:
            print(e)
            raise


class SeerConnect:
    
    def __init__(self):
        """Creates a graphQL client able to interact with 
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
        cookie = auth.seerAuth(apiUrl).cookie
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
        queryString = '''
            mutation {
                addLabelGroupToStudy(studyId: "%s", name: "%s", description: "%s") {
                                                                                                       id
                                                                                                       }
            }
        ''' % (studyId, name, description)
        
        labelGroup = self.graphqlClient.execute(gql(queryString))
        return labelGroup['addLabelGroupToStudy']['id']
    
    
    def delLabelGroup(self, GroupId):
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
        queryString = '''
            mutation {
                removeLabelGroupFromStudy(groupId: "%s")
            }
        ''' % (GroupId)
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
    
        queryString = '''
            mutation {
                addLabelsToLabelGroup(groupId: "%s", labels: [{ startTime: %f, 
                                                               duration: %f, 
                                                               timezone: %f }]) {id}
            }
        ''' % (groupId, startTime, duration, timezone)
        self.graphqlClient.execute(gql(queryString))
    
    
    def getLabelTypes(self):
        """Get label types
        
        Parameters
        ----------
                
        Returns
        -------
        labelTypeID : string
                ID of label type
        name : string
                label type name
        category : string
                label type category
        description : string
                label type description
        
        Notes
        -----
        
        Example
        -------
        labelTypes = pd.io.json_normalize(getLabelTypes()['labelTypes'])
        
        """
        queryString = '''
            query {
                labelTypes {id
                            name
                            category
                            description
                }
            }
        ''' % ()
        labelTypes = json_normalize(self.graphqlClient.execute(gql(queryString))['labelTypes'])
        return labelTypes
    
    def getStudies(self):
        queryString = '''
            query {
                studies {
                    id
                    patient {id}
                    name
                }
            }
        ''' % ()
        return self.graphqlClient.execute(gql(queryString))
    
    
    
    def getStudy(self, studyID):
        queryString = '''
            query {
                study(id: "%s") {
                    id
                    patient {id}
                    name
                }
            }
        ''' % (studyID)
        return self.graphqlClient.execute(gql(queryString)) 

    def getStudyId(self):
        queryString = '''
            query {
                studies {
                    id
                    patient {id}
                    name
                }
            }
        ''' % ()
        return self.graphqlClient.execute(gql(queryString))
        
    def getChannelGroup(self, studyID):
        queryString = '''
        query {
                study(id: "%s") {
                    id
                    patient {id}
                    name
                    channelGroups {name
                                   sampleRate
                                   segments {id}
                                   }
                    }
            }
    
        ''' % (studyID)
        return self.graphqlClient.execute(gql(queryString))
    
    def getDataLinks(self, studyID):
        queryString = '''
        query {
                study(id: "%s") {
                    id
                    patient {id}
                    name
                    channelGroups {name
                                   sampleRate
                                   segments {id
                                             startTime
                                             duration
                                             dataChunks {time
                                                         length
                                                         url}
                                             }
                                   }
                    }
            }
    
        ''' % (studyID)
        return self.graphqlClient.execute(gql(queryString))
    
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
        studies = self.getStudyId()['studies']
        studiesToGet = []
        
        for s in studies:
            if study is not None:
                if s['name'] == study:
                    studiesToGet.append(s['id'])
            else:
                studiesToGet.append(s['id'])
        
        
        result = []
        
        for sdy in studiesToGet:
            queryString = '''
            query { 
                study (id: "%s") {
                        id
                        patient {id}
                        name
                        channelGroups {id
                                       name
                                       sampleRate
                                       chunkPeriod
                                       bytesPerSample
                                       signalMin
                                       signalMax
                                       units
                                       exponent
                                       segments (fromTime: 1.0, toTime: 9000000000000)  {id
                                                 startTime
                                                 duration
                                                 dataChunks {time
                                                             length
                                                             url}
                                                 }
                                       channels {id
                                                 name
                                                 channelType {name
                                                              category}
                                                 }
                                       }
                        labelGroups   {id
                                       name
                                       labelType
                                       description
                                       labels {id
                                               note
                                               startTime
                                               duration
                                               timezone}
                                       }
                        }
                }
        
            ''' % (sdy)
            
            t = time.time()
            result.append(self.graphqlClient.execute(gql(queryString))['study'])
            print('study query time: ',round(time.time()-t,2))
            
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
    
    def getLinks(self, allData, threads=5):
        """Download data chunks and stich them together in one dataframe
        
        Parameters
        ----------
        allData : pandas DataFrame
                Dataframe containing metadata required for downloading and 
                processing raw data
                
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
        manager = Manager()

        dataList = manager.list([])
        dataQ = Queue(15)
        procs = [Process(target=downloadLink, args=(dataQ, dataList)) for i in range(threads)]
        for p in procs: p.start()
        
#        uniqueUrls = allData['dataChunks.url'].copy().drop_duplicates()
        for studyID in allData['id'].copy().drop_duplicates().tolist():
            for channelGroupsID in allData['channelGroups.id'].copy().drop_duplicates().tolist():
                for segmentsID in allData['segments.id'].copy().drop_duplicates().tolist():
                    metaData = allData[(allData['id']==studyID) & (allData['channelGroups.id']==channelGroupsID) & (allData['segments.id']==segmentsID)].copy()
                    channelNames = metaData['channels.name'].copy().drop_duplicates().tolist()
                    metaData = metaData[['dataChunks.url', 'dataChunks.time', 'channelGroups.bytesPerSample', 'channelGroups.sampleRate', 
                                         'channelGroups.chunkPeriod', 'channelGroups.signalMin', 'channelGroups.signalMax']]
                    metaData = metaData.drop_duplicates()
                    metaData = metaData.dropna(axis=0, how='any', subset=['dataChunks.url'])
                    for r in range(metaData.shape[0]):
                        dataQ.put([metaData.iloc[r, :], studyID, channelGroupsID, segmentsID, channelNames])
                    
                    for t in range(threads):
                        dataQ.put(None)
                    for p in procs: p.join()
                        
                        
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
    