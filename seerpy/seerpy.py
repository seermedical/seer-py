# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

from multiprocessing import Pool
import os
import time

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

from .auth import SeerAuth
from . import utils
from . import graphql


class SeerConnect:  # pylint: disable=too-many-public-methods

    def __init__(self, api_url='https://api.seermedical.com', email=None, password=None):
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

        self.api_url = api_url
        self.login(email, password)

        self.last_query_time = time.time()
        self.api_limit_expire = 300
        self.api_limit = 240

    def login(self, email=None, password=None):
        self.seer_auth = SeerAuth(self.api_url, email, password)
        cookie = self.seer_auth.cookie
        header = {'Cookie': list(cookie.keys())[0] + '=' + cookie['seer.sid']}
        self.graphql_client = GQLClient(
            transport=RequestsHTTPTransport(
                url=self.api_url + '/api/graphql',
                headers=header,
                use_json=True,
                timeout=30
            )
        )
        self.last_query_time = time.time()

    def execute_query(self, query_string, invocations=0):
        resolvable_api_errors = ['503 Server Error', '502 Server Error', 'Read timed out.',
                                 'NOT_AUTHENTICATED']

        try:
            time.sleep(max(0, ((self.api_limit_expire / self.api_limit)
                               - (time.time() - self.last_query_time))))
            response = self.graphql_client.execute(gql(query_string))
            self.last_query_time = time.time()
            return response
        except Exception as ex:
            if invocations > 4:
                print('Too many failed query invocations. raising error')
                raise
            error_string = str(ex)
            if any(api_error in error_string for api_error in resolvable_api_errors):
                if 'NOT_AUTHENTICATED' in error_string:
                    self.seer_auth.destroy_cookie()
                else:
                    print('"', error_string, '" raised, trying again after a short break')
                    time.sleep(min(30 * (invocations+1)**2,
                                   max(self.last_query_time + self.api_limit_expire - time.time(),
                                       0)))
                invocations += 1
                self.login()
                return self.execute_query(query_string, invocations=invocations)

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

    @staticmethod  # maybe this could move to a utility class
    def pandas_flatten(parent, parent_name, child_name):
        child_list = []
        for i in range(len(parent)):
            parent_id = parent[parent_name+'id'][i]
            child = json_normalize(parent[parent_name+child_name][i])
            child.columns = [child_name+'.' + str(col) for col in child.columns]
            child[parent_name+'id'] = parent_id
            child_list.append(child)

        if child_list:
            child = pd.concat(child_list).reset_index(drop=True)
        if not child_list or child.empty:
            columns = [parent_name + 'id', child_name + '.id']
            child = pd.DataFrame(columns=columns)
        return child

    def add_label_group(self, study_id, name, description, label_type=None):
        """Add Label Group to study

        Parameters
        ----------
        study_id : string
                Seer study ID
        name : string
                name of label
        description : string
                description of label
        label_type : string
                Seer label type ID

        Returns
        -------
        labelGroupID : string
                ID of label group

        Notes
        -----

        Example
        -------
        labelGroup = add_label_group(study_id, name, description)

        """
        query_string = graphql.get_add_label_group_mutation_string(study_id, name, description,
                                                                   label_type)
        response = self.execute_query(query_string)
        return response['addLabelGroupToStudy']['id']

    def del_label_group(self, group_id):
        """Delete Label Group from study

        Parameters
        ----------
        group_id : string
                Seer label group ID to delete

        Returns
        -------
        label_group_id : string
                ID of deleted label group

        Notes
        -----

        Example
        -------
        delLG = del_label_group(group_id)

        """
        query_string = graphql.get_remove_label_group_mutation_string(group_id)
        return self.execute_query(query_string)

    def add_labels(self, group_id, labels):
        """Add labels to label group

        Parameters
        ----------
        group_id : string
                Seer label group ID

        labels: list of:
                note: string
                        label note
                startTime : float
                        label start time in epoch time
                duration : float
                        duration of event in milliseconds
                timezone : float
                        local UTC timezone (eg. Melbourne = 11.0)
                tagIds: [String!]
                        list of tag ids
                confidence: float
                        Confidence given to label between 0 and 1

        Returns
        -------
        None

        Notes
        -----

        """
        if isinstance(labels, pd.DataFrame):
            labels = labels.to_dict('records')
        query_string = graphql.get_add_labels_mutation_string(group_id, labels)
        return self.execute_query(query_string)

    def get_tag_ids(self):
        query_string = graphql.get_tag_id_query_string()
        response = self.execute_query(query_string)
        return response['labelTags']

    def get_tag_ids_dataframe(self):
        tag_ids = self.get_tag_ids()
        tag_ids = json_normalize(tag_ids)
        return tag_ids

    def get_study_ids(self, limit=50, search_term=''):
        studies = self.get_studies(limit, search_term)
        return [study['id'] for study in studies]

    def get_studies(self, limit=50, search_term=''):
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(search_term)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def get_studies_dataframe(self, limit=50, search_term=''):
        studies = self.get_studies(limit, search_term)
        studies_dataframe = json_normalize(studies)
        return studies_dataframe.drop('patient', errors='ignore', axis='columns')

    def get_study_ids_from_names_dataframe(self, study_names):
        if isinstance(study_names, str):
            study_names = [study_names]
        studies = self.get_studies_dataframe()
        return studies[studies['name'].isin(study_names)][['name', 'id']].reset_index(drop=True)

    def get_study_ids_from_names(self, study_names):
        return self.get_study_ids_from_names_dataframe(study_names)['id'].tolist()

    def get_studies_by_id(self, study_ids, limit=50):
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        studies_query_string = graphql.get_studies_by_study_id_paged_query_string(study_ids)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def get_channel_groups(self, study_id):
        query_string = graphql.get_channel_groups_query_string(study_id)
        response = self.execute_query(query_string)
        return response['study']['channelGroups']

    def get_segment_urls(self, segment_ids, limit=10000):
        if not segment_ids:
            return pd.DataFrame(columns=['baseDataChunkUrl', 'segments.id'])

        segments = []
        counter = 0
        while int(counter * limit) < len(segment_ids):
            segment_ids_batch = segment_ids[int(counter * limit):int((counter + 1) * limit)]
            query_string = graphql.get_segment_urls_query_string(segment_ids_batch)
            response = self.execute_query(query_string)
            segments.extend([segment for segment in response['studyChannelGroupSegments']
                             if segment is not None])
            counter += 1
        segment_urls = pd.DataFrame(segments)
        segment_urls = segment_urls.rename(columns={'id': 'segments.id'})
        return segment_urls

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
        if label_results is None:
            return label_results
        label_group = json_normalize(label_results)
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_group = label_group.drop('labelGroup.labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')

        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

        return label_group

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

    def get_viewed_times_dataframe(self, study_id):
        query_string = graphql.get_viewed_times_query_string(study_id)
        response = self.execute_query(query_string)
        response = json_normalize(response['viewGroups'])

        views = []
        for i in range(len(response)):
            view = json_normalize(response.at[i, 'views'])
            view['user'] = response.at[i, 'user.fullName']
            views.append(view)
        views = pd.concat(views).reset_index(drop=True)

        views['createdAt'] = pd.to_datetime(views['createdAt'])
        views['updatedAt'] = pd.to_datetime(views['updatedAt'])
        return views

    def get_organisations(self):
        query_string = graphql.get_organisations_query_string()
        response = self.execute_query(query_string)['organisations']
        return response

    def get_organisations_dataframe(self):
        orgs = self.get_organisations()
        if orgs is None:
            return orgs
        return pd.DataFrame(orgs)

    def get_patients(self, party_id=""):
        query_string = graphql.get_patients_query_string(party_id)
        response = self.execute_query(query_string)['patients']
        return response

    def get_patients_dataframe(self, party_id=""):
        patients = self.get_patients(party_id)
        if patients is None:
            return patients
        return json_normalize(patients)

    def get_diary_labels(self, patient_id):
        query_string = graphql.get_diary_labels_query_string(patient_id)
        response = self.execute_query(query_string)['patient']['diary']['labelGroups']
        return response

    def get_diary_labels_dataframe(self, patient_id):
        label_results = self.get_diary_labels(patient_id)
        if label_results is None:
            return label_results

        label_groups = json_normalize(label_results)
        labels = self.pandas_flatten(label_groups, '', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_groups = label_groups.drop('labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')
        label_groups = label_groups.merge(labels, how='left', on='id', suffixes=('', '_y'))
        label_groups = label_groups.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))
        label_groups = label_groups.rename({'id':'labelGroups.id'})
        label_groups['id'] = patient_id
        return label_groups

    def get_all_study_metadata_by_names(self, study_names=None):
        """Get all the metadata available about named studies

        Parameters
        ----------
        study_names (Optional) : a list of study names. If not provided, data will be returned for
        all studies

        Returns
        -------
        allData : dict
                a dictionary with a single key 'studies' with a list of studies as it's value

        Example
        -------
        studies = get_all_study_metadata_by_names()['studies']
        """
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names)
        return self.get_all_study_metadata_by_ids(study_ids)

    def get_all_study_metadata_by_ids(self, study_ids=None):
        """Get all the metadata available about studies with the suppled ids

        Parameters
        ----------
        study_ids (Optional) : a list of study ids. If not provided, data will be returned for
        all studies

        Returns
        -------
        allData : dict
                a dictionary with a single key 'studies' with a list of studies as it's value

        Example
        -------
        studies = get_all_study_metadata_by_ids()['studies']
        """
        if study_ids is None:
            study_ids = self.get_study_ids()
        elif not study_ids:  # treat empty list as asking for nothing, not everything
            return {'studies' : []}

        result = [self.execute_query(graphql.get_study_with_data_query_string(study_id))['study']
                  for study_id in study_ids]

        return {'studies' : result}

    def get_all_study_metadata_dataframe_by_names(self, study_names=None):
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names)
        return self.get_all_study_metadata_dataframe_by_ids(study_ids)

    def get_all_study_metadata_dataframe_by_ids(self, study_ids=None):
        metadata = self.get_all_study_metadata_by_ids(study_ids)
        all_data = json_normalize(metadata['studies'])
        channel_groups = self.pandas_flatten(all_data, '', 'channelGroups')
        channels = self.pandas_flatten(channel_groups, 'channelGroups.', 'channels')
        segments = self.pandas_flatten(channel_groups, 'channelGroups.', 'segments')

        segments = segments.drop('segments.dataChunks', errors='ignore', axis='columns')
        channel_groups = channel_groups.drop(['channelGroups.segments', 'channelGroups.channels'],
                                             errors='ignore', axis='columns')
        all_data = all_data.drop(['channelGroups', 'labelGroups'], errors='ignore', axis='columns')

        channel_groups = channel_groups.merge(segments, how='left', on='channelGroups.id',
                                              suffixes=('', '_y'))
        channel_groups = channel_groups.merge(channels, how='left', on='channelGroups.id',
                                              suffixes=('', '_y'))
        all_data = all_data.merge(channel_groups, how='left', on='id', suffixes=('', '_y'))

        return all_data

    # pylint:disable=too-many-locals
    @staticmethod
    def create_data_chunk_urls(metadata, segment_urls, from_time=0, to_time=9e12):
        chunk_pattern = '00000000000.dat'

        data_chunks = []
        metadata = metadata.drop_duplicates('segments.id').reset_index(drop=True)
        for index in range(len(metadata.index)):
            row = metadata.iloc[index]

            seg_base_urls = segment_urls.loc[segment_urls['segments.id'] == row['segments.id'],
                                             'baseDataChunkUrl']
            if seg_base_urls.empty:
                continue
            seg_base_url = seg_base_urls.iloc[0]

            chunk_period = row['channelGroups.chunkPeriod']
            num_chunks = int(np.ceil(row['segments.duration'] / chunk_period / 1000.))
            start_time = row['segments.startTime']

            for i in range(num_chunks):
                chunk_start_time = chunk_period * 1000 * i + start_time
                next_chunk_start_time = chunk_period * 1000 * (i + 1) + start_time
                if (chunk_start_time <= to_time and next_chunk_start_time >= from_time):
                    data_chunk_name = str(i).zfill(len(chunk_pattern) - 4) + chunk_pattern[-4:]
                    data_chunk_url = seg_base_url.replace(chunk_pattern, data_chunk_name)
                    data_chunk = [row['segments.id'], data_chunk_url, chunk_start_time]
                    data_chunks.append(data_chunk)

        return pd.DataFrame.from_records(data_chunks, columns=['segments.id', 'dataChunks.url',
                                                               'dataChunks.time'])

    # pylint:disable=too-many-locals
    def get_links(self, all_data, segment_urls=None,  # pylint:disable=too-many-arguments
                  threads=None, from_time=0, to_time=9e12):
        """Download data chunks and stich them together in one dataframe

        Parameters
        ----------
        all_data : pandas DataFrame
                metadata required for downloading and processing raw data
        segment_urls : list
                if None, these will be retrieved for each segment i all_data
        threads : int
                number of threads to use. If > 1 then will use multiprocessing
                if None (default), it will use 1 on Windows and 5 on Linux/MacOS

        Returns
        -------
        data : pandas DataFrame
                dataframe containing studyID, channelGroupIDs, semgmentIDs, time, and raw data

        Example
        -------
        data = get_links(all_data.copy())

        """
        if threads is None:
            if os.name != 'nt':
                threads = 1
            else:
                threads = 5

        segment_ids = all_data['segments.id'].drop_duplicates().tolist()

        if segment_urls is None:
            segment_urls = self.get_segment_urls(segment_ids)

        data_q = []

        for segment_id in segment_ids:
            metadata = all_data[all_data['segments.id'].values == segment_id]

            num_channels = len(metadata['channels.id'].drop_duplicates())
            channel_names = metadata['channels.name'].drop_duplicates().tolist()
            actual_channel_names = channel_names
            if len(channel_names) != num_channels:
                actual_channel_names = ['Channel %s' % (i) for i in range(0, num_channels)]

            metadata = metadata.drop_duplicates('segments.id')

            study_id = metadata['id'].iloc[0]
            channel_groups_id = metadata['channelGroups.id'].iloc[0]

            data_chunks = self.create_data_chunk_urls(metadata, segment_urls, from_time=from_time,
                                                      to_time=to_time)
            metadata = metadata.merge(data_chunks, how='left', left_on='segments.id',
                                      right_on='segments.id', suffixes=('', '_y'))

            metadata = metadata[['dataChunks.url', 'dataChunks.time',
                                 'channelGroups.sampleEncoding', 'channelGroups.sampleRate',
                                 'channelGroups.samplesPerRecord', 'channelGroups.recordsPerChunk',
                                 'channelGroups.compression', 'channelGroups.signalMin',
                                 'channelGroups.signalMax', 'channelGroups.exponent']]
            metadata = metadata.drop_duplicates()
            metadata = metadata.dropna(axis=0, how='any', subset=['dataChunks.url'])
            for i in range(len(metadata.index)):
                data_q.append([metadata.iloc[i], study_id, channel_groups_id, segment_id,
                               actual_channel_names])

        if threads > 1:
            pool = Pool(processes=min(threads, len(data_q)))
            data_list = list(pool.map(utils.download_link, data_q))
            pool.close()
            pool.join()
        else:
            data_list = [utils.download_link(data_q_item) for data_q_item in data_q]

        if data_list:
            data = pd.concat(data_list)
            data = data.loc[(data['time'] >= from_time) & (data['time'] < to_time)]
            data = data.sort_values(['id', 'channelGroups.id', 'segments.id', 'time'], axis=0,
                                    ascending=True, na_position='last')
            data = data.reset_index(drop=True)
        else:
            data = None

        return data
