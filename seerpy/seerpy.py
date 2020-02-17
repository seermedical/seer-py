# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

import math
import time
import json

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from pandas.io.json import json_normalize
import requests

from .auth import SeerAuth
from . import utils
from . import graphql


class SeerConnect:  # pylint: disable=too-many-public-methods

    def __init__(self, api_url='https://api.seermedical.com/api', email=None, password=None, dev=False):
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
        self.dev = dev

        self.login(email, password)

        self.last_query_time = time.time()
        self.api_limit_expire = 300
        self.api_limit = 580

    def login(self, email=None, password=None):
        self.seer_auth = SeerAuth(self.api_url, email, password, self.dev)
        cookie = self.seer_auth.cookie

        header =  {}

        if self.dev:
            header['Cookie'] = f'seerdev.sid={cookie["seerdev.sid"]}'
        else:
            header['Cookie'] = f'seer.sid={cookie["seer.sid"]}'

        def graphql_client(party_id=None):
            url_suffix = '?partyId=' + party_id if party_id else ''
            url = self.api_url + '/graphql' + url_suffix
            return GQLClient(
                transport=RequestsHTTPTransport(
                    url=url,
                    headers=header,
                    use_json=True,
                    timeout=30
                )
            )

        self.graphql_client = graphql_client
        self.last_query_time = time.time()

    def execute_query(self, query_string, party_id=None, invocations=0):
        resolvable_api_errors = ['502 Server Error', '503 Server Error', '504 Server Error'
                                 'Read timed out.', 'NOT_AUTHENTICATED']

        try:
            time.sleep(max(0, ((self.api_limit_expire / self.api_limit)
                               - (time.time() - self.last_query_time))))
            response = self.graphql_client(party_id).execute(gql(query_string))
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
                return self.execute_query(query_string, party_id, invocations=invocations)

            raise

    def get_paginated_response(self, query_string, object_name, limit=250, party_id=None):
        offset = 0
        objects = []
        while True:
            formatted_query_string = query_string.format(limit=limit, offset=offset)
            response = self.execute_query(formatted_query_string, party_id)[object_name]
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
            child = json_normalize(parent[parent_name+child_name][i]).sort_index(axis=1)
            child.columns = [child_name+'.' + str(col) for col in child.columns]
            child[parent_name+'id'] = parent_id
            child_list.append(child)

        if child_list:
            child = pd.concat(child_list).reset_index(drop=True)
        if not child_list or child.empty:
            columns = [parent_name + 'id', child_name + '.id']
            child = pd.DataFrame(columns=columns)
        return child

    def add_label_group(self, study_id, name, description, label_type=None, party_id=None):
        """Add Label Group to study

        Parameters
        ----------
        study_id : string
                Seer study ID
        name : string
                name of label
        description : string
                description of label
        label_type (Optional) : string
                Seer label type ID
        party_id (Optional) : string, the party id of the context for the query (e.g. organisation)

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
        response = self.execute_query(query_string, party_id)
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

    def add_labels_batched(self, label_group_id, labels, batch_size=500):
        """Add labels to label group in batches

        Parameters
        ----------
        label_group_id : string
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

        batch_size: int
                number of labels to add in a batch. Optional, defaults to 500.

        Returns
        -------
        None

        Notes
        -----

        """
        number_of_batches = math.ceil(len(labels) / batch_size)
        for i in range(number_of_batches):
            start = i * batch_size
            end = start + batch_size
            self.add_labels(label_group_id, labels[start:end])

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

    def add_document(self, study_id, document_name, document_path):
        query_string = graphql.get_add_document_mutation_string(study_id, document_name)
        response_add = self.execute_query(query_string)['createStudyDocuments'][0]
        with open(document_path, 'rb') as f:
            response_put = requests.put(response_add['uploadFileUrl'], data=f)
        if response_put.status_code == 200:
            query_string = graphql.get_confirm_document_mutation_string(study_id,
                                                                    response_add['id'])
            response_confirm = self.execute_query(query_string)
            return response_confirm['confirmStudyDocuments'][0]['downloadFileUrl']
        else:
            raise RuntimeError('Error uploading document: status code ' +
                               str(response_put.status_code))

    def get_tag_ids(self):
        query_string = graphql.get_tag_id_query_string()
        response = self.execute_query(query_string)
        return response['labelTags']

    def get_tag_ids_dataframe(self):
        tag_ids = self.get_tag_ids()
        tag_ids = json_normalize(tag_ids).sort_index(axis=1)
        return tag_ids

    def get_study_ids(self, limit=50, search_term='', party_id=None):
        studies = self.get_studies(limit, search_term, party_id)
        return [study['id'] for study in studies]

    def get_studies(self, limit=50, search_term='', party_id=None):
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(search_term)
        return self.get_paginated_response(studies_query_string, 'studies', limit, party_id)

    def get_studies_dataframe(self, limit=50, search_term='', party_id=None):
        studies = self.get_studies(limit, search_term, party_id)
        studies_dataframe = json_normalize(studies).sort_index(axis=1)
        return studies_dataframe.drop('patient', errors='ignore', axis='columns')

    def get_study_ids_from_names_dataframe(self, study_names, party_id=None):
        if isinstance(study_names, str):
            study_names = [study_names]

        studies = json_normalize([
            study for study_name in study_names
            for study in self.get_studies(search_term=study_name, party_id=party_id)
        ])

        if studies.empty:
            return studies.assign(id=None)

        return studies[['name', 'id']].reset_index(drop=True)

    def get_study_ids_from_names(self, study_names, party_id=None):
        return self.get_study_ids_from_names_dataframe(study_names, party_id)['id'].tolist()

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

    def get_data_chunk_urls(self, study_metadata, s3_urls=True, from_time=0, to_time=9e12,
                            limit=10000):
        if study_metadata.empty:
            return pd.DataFrame(columns=['segments.id', 'chunkIndex', 'chunk_start', 'chunk_end',
                                         'chunk_url'])

        study_metadata = study_metadata.drop_duplicates('segments.id')
        study_metadata = study_metadata[study_metadata['segments.startTime'] <= to_time]
        study_metadata = study_metadata[study_metadata['segments.startTime'] +
                                        study_metadata['segments.duration'] >= from_time]

        data_chunks = []
        chunk_metadata = []
        for row in zip(study_metadata['channelGroups.chunkPeriod'],
                       study_metadata['segments.duration'], study_metadata['segments.startTime'],
                       study_metadata['segments.id']):
            chunk_period = row[0]
            num_chunks = int(math.ceil(row[1] / chunk_period / 1000.))
            for i in range(num_chunks):
                chunk_start = row[2] + chunk_period * i
                chunk_end = chunk_start + chunk_period
                if chunk_start >= from_time and chunk_end <= to_time:
                    data_chunks.append({'segmentId': row[3], 'chunkIndex': i})
                    chunk_metadata.append({'segments.id': row[3], 'chunkIndex': i,
                                           'chunk_start': chunk_start, 'chunk_end': chunk_end})
        if not data_chunks:
            return pd.DataFrame(columns=['segments.id', 'chunkIndex', 'chunk_start', 'chunk_end',
                                         'chunk_url'])
        chunks = []
        counter = 0
        while int(counter * limit) < len(data_chunks):
            data_chunks_batch = data_chunks[int(counter * limit):int((counter + 1) * limit)]
            query_string = graphql.get_data_chunk_urls_query_string(data_chunks_batch, s3_urls)
            response = self.execute_query(query_string)
            chunks.extend([chunk for chunk in response['studyChannelGroupDataChunkUrls']
                           if chunk is not None])
            counter += 1
        data_chunk_urls = pd.DataFrame(chunk_metadata)
        data_chunk_urls['chunk_url'] = chunks

        return data_chunk_urls

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
        label_group = json_normalize(label_results).sort_index(axis=1)
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_group = label_group.drop('labelGroup.labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')

        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

        return label_group

    def get_labels_string(self, study_id, label_group_id, from_time=0, to_time=9e12):
        query_string = graphql.get_labels_string_query_string(study_id, label_group_id, from_time,
                                                           to_time)
        response = self.execute_query(query_string)['study']
        return response

    def get_labels_string_dataframe(self, study_id, label_group_id, from_time=0,  # pylint:disable=too-many-arguments
                   to_time=9e12):
        label_results = self.get_labels_string(study_id, label_group_id, from_time=from_time,
                                               to_time=to_time)
        if label_results is None:
            return label_results
        label_group = json_normalize(label_results).sort_index(axis=1)
        label_group['labelGroup.labelString'] = (label_group['labelGroup.labelString']
                                                    .apply(json.loads))
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labelString')
        label_group = label_group.drop('labelGroup.labelString', errors='ignore', axis='columns')
        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group=label_group.rename(columns = {'labelString.d': 'labels.duration',
                                                  'labelString.id': 'labels.id',
                                                  'labelString.s': 'labels.startTime'})
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
                label_group['labelGroup.labelType'] = label_group.pop('labelType')
                label_group['labelGroup.numberOfLabels'] = label_group.pop('numberOfLabels')
                label_group['id'] = study['id']
                label_group['name'] = study['name']
                label_groups.append(label_group)
        return pd.DataFrame(label_groups)

    def get_viewed_times_dataframe(self, study_id, limit=250, offset=0):
        views = []
        while True:
            query_string = graphql.get_viewed_times_query_string(study_id, limit, offset)
            response = self.execute_query(query_string)
            response = json_normalize(response['viewGroups']).sort_index(axis=1)
            non_empty_views = False
            for i in range(len(response)):
                view = json_normalize(response.at[i, 'views']).sort_index(axis=1)
                view['user'] = response.at[i, 'user.fullName']
                if not view.empty:
                    non_empty_views = True
                    views.append(view)
            if not non_empty_views:
                break
            offset += limit
        if views:
            views = pd.concat(views).reset_index(drop=True)
            views['createdAt'] = pd.to_datetime(views['createdAt'])
            views['updatedAt'] = pd.to_datetime(views['updatedAt'])
        else:
            views = None
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

    def get_patients(self, party_id=None):
        query_string = graphql.get_patients_query_string()
        response = self.execute_query(query_string, party_id)['patients']
        return response

    def get_patients_dataframe(self, party_id=None):
        patients = self.get_patients(party_id)
        if patients is None:
            return patients
        return json_normalize(patients).sort_index(axis=1)

    def get_documents_for_studies(self, study_ids, limit=50):
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        documents_query_string = graphql.get_documents_for_study_ids_paged_query_string(study_ids)
        return self.get_paginated_response(documents_query_string, 'studies', limit)

    def get_documents_for_studies_dataframe(self, study_ids, limit=50):
        documents = []
        for study in self.get_documents_for_studies(study_ids, limit):
            for document in study['documents']:
                document['document.id'] = document.pop('id')
                document['document.name'] = document.pop('name')
                document['id'] = study['id']
                document['name'] = study['name']
                documents.append(document)
        return pd.DataFrame(documents)

    def get_diary_labels(self, patient_id, offset=0, limit=100):
        label_results = None
        # set true if we need to fetch labels
        query_flag = True

        while True:
            if not query_flag:
                break

            query_string = graphql.get_diary_labels_query_string(patient_id, limit, offset)
            response = self.execute_query(query_string)['patient']['diary']
            label_groups = response['labelGroups']

            query_flag = False
            for idx, group in enumerate(label_groups):
                labels = group['labels']

                if not labels:
                    continue

                # we need to fetch more labels
                if len(labels) >= limit:
                    query_flag = True

                if label_results is None:
                    label_results = response
                    if any([index['numberOfLabels'] for index in response['labelGroups'] if index['numberOfLabels'] >= limit]):
                        query_flag = True
                    break

                else:
                    label_results['labelGroups'][idx]['labels'].extend(labels)

            offset += limit

        return label_results

    def get_diary_labels_dataframe(self, patient_id):

        label_results = self.get_diary_labels(patient_id)
        if label_results is None:
            return label_results

        label_groups = json_normalize(label_results['labelGroups']).sort_index(axis=1)
        labels = self.pandas_flatten(label_groups, '', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_groups = label_groups.drop('labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')
        label_groups = label_groups.merge(labels, how='left', on='id', suffixes=('', '_y'))
        label_groups = label_groups.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))
        label_groups = label_groups.rename({'id':'labelGroups.id'})
        label_groups['id'] = patient_id
        return label_groups


    def get_all_study_metadata_by_names(self, study_names=None, party_id=None):
        """Get all the metadata available about named studies

        Parameters
        ----------
        study_names (Optional) : a list of study names. If not provided, data will be returned for
        all studies
        party_id (Optional) : string, the party id of the context for the query (e.g. organisation)

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
            study_ids = self.get_study_ids_from_names(study_names, party_id)
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
        all_data = json_normalize(metadata['studies']).sort_index(axis=1)
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
    def get_channel_data(self, all_data, segment_urls=None,  # pylint:disable=too-many-arguments
                         download_function=requests.get, threads=None, from_time=0, to_time=9e12):
        """Download data chunks and stich them together in one dataframe

        Parameters
        ----------
        all_data : pandas DataFrame
                metadata required for downloading and processing raw data
        segment_urls : pandas DataFrame
                columns=['segments.id', 'baseDataChunkUrl']
                if None, these will be retrieved for each segment in all_data
        download_function: function
                the function used to download the channel data. defaults to requests.get
        threads : int
                number of threads to use. If > 1 then will use multiprocessing
                if None (default), it will use 1 on Windows and 5 on Linux/MacOS

        Returns
        -------
        data : pandas DataFrame
                dataframe containing studyID, channelGroupIDs, semgmentIDs, time, and raw data

        Example
        -------
        data = get_channel_data(all_data)

        """
        if segment_urls is None:
            segment_ids = all_data['segments.id'].drop_duplicates().tolist()
            segment_urls = self.get_segment_urls(segment_ids)

        return utils.get_channel_data(all_data, segment_urls, download_function, threads, from_time,
                                      to_time)

    def get_all_bookings(self, organisation_id, start_time, end_time):
        query_string = graphql.get_bookings_query_string(organisation_id, start_time, end_time)
        response = self.execute_query(query_string)
        return response['organisation']['bookings']

    def get_all_bookings_dataframe(self, organisation_id, start_time, end_time):
        bookings_response = self.get_all_bookings(organisation_id, start_time, end_time)
        bookings = json_normalize(bookings_response).sort_index(axis=1)
        studies = self.pandas_flatten(bookings, 'patient.', 'studies')
        equipment = self.pandas_flatten(bookings, '', 'equipmentItems')
        bookings = bookings.drop('patient.studies', errors='ignore', axis='columns')
        bookings = bookings.drop('equipmentItems', errors='ignore', axis='columns')
        bookings = bookings.merge(studies, how='left', on='patient.id')
        bookings = bookings.merge(equipment, how='left', on='id')
        return bookings.drop_duplicates().reset_index(drop=True)

    # DIARY STUDY (FITBIT) ANALYSIS
    def get_diary_data_groups(self, patient_id, limit=20, offset=0):
        # TODO use limit/offset for pagination (unlikely to be more than 20 label groups for a while)
        query_string = graphql.get_diary_study_label_groups_string(patient_id, limit, offset)
        response = self.execute_query(query_string)['patient']['diaryStudy']
        label_groups = response['labelGroups']
        return label_groups

    def get_diary_data_groups_dataframe(self, patient_id, limit=20, offset=0):
        """Get a list of label groups present in a patient's diary study

        Parameters
        ----------
        patient_id : The patient ID (string)

        Returns
        -------
        label_groups : pandas DataFrame
                dataframe containing labelGroupID, labelGroupName, numberOfLabels in labelGroup

        Example
        -------
        label_groups = get_diary_study_label_groups_dataframe("some_id")

        """
        label_group_results = self.get_diary_data_groups(patient_id, limit, offset)
        if label_group_results is None:
            return label_group_results
        label_groups = json_normalize(label_group_results).sort_index(axis=1)
        return label_groups

    def get_diary_data_labels(self, patient_id, label_group_id, from_time=0,  # pylint:disable=too-many-arguments
                   to_time=9e12, limit=200, offset=0):
        label_results = None

        while True:
            query_string = graphql.get_labels_for_diary_study_query_string(patient_id, label_group_id, from_time,
                                                           to_time, limit, offset)
            response = self.execute_query(query_string)['patient']['diaryStudy']
            labels = response['labelGroup']['labels']
            if not labels:
                break

            if label_results is None:
                label_results = response
            else:
                label_results['labelGroup']['labels'].extend(labels)

            offset += limit

        return label_results

    def get_diary_data_labels_dataframe(self, patient_id, label_group_id,  # pylint:disable=too-many-arguments
                             from_time=0, to_time=9e12, limit=200, offset=0):
        """Get labels from a patient's diary study

        Parameters
        ----------
        patient_id : The patient ID (string)
        label_group_id: The label group ID
        from_time: min start time for labels (UTC time in milliseconds)
        to_time: max start time for labels (UTC time in milliseconds)

        Returns
        -------
        label_group : pandas DataFrame
                dataframe containing labelGroup info, labels (startTime, timeZone, duration) and tags

        Example
        -------
        label_groups = get_diary_study_labels_dataframe(patient_id, label_group_id)

        """
        label_results = self.get_diary_data_labels(patient_id, label_group_id, from_time, to_time, limit, offset)
        if label_results is None:
            return label_results
        label_group = json_normalize(label_results).sort_index(axis=1)
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_group = label_group.drop('labelGroup.labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')

        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

        return label_group


    def get_diary_channel_groups(self, patient_id, from_time, to_time):
        query_string = graphql.get_diary_study_channel_groups_query_string(patient_id, from_time, to_time)
        response = self.execute_query(query_string)
        return response['patient']['diaryStudy']['channelGroups']

    def get_diary_channel_groups_dataframe(self, patient_id, from_time=0, to_time=90000000000000):
        metadata = self.get_diary_channel_groups(patient_id, from_time, to_time)
        channel_groups = json_normalize(metadata).sort_index(axis=1)
        if channel_groups.empty:
            return None

        segments = self.pandas_flatten(channel_groups, '', 'segments')
        data_chunks = self.pandas_flatten(segments, 'segments.', 'dataChunks')

        channel_groups = channel_groups.drop('segments', errors='ignore', axis='columns')
        segments = segments.drop('segments.dataChunks', errors='ignore', axis='columns')

        channel_groups = channel_groups.merge(segments, how='left', on='id', suffixes=('', '_y'))
        channel_groups = channel_groups.merge(data_chunks, how='left', on='segments.id', suffixes=('', '_y'))

        return channel_groups


    def get_diary_fitbit_data(self, segments):
        """Get fitbit data from a patient's diary study

        Parameters
        ----------
        segments: pandas DataFrame as returned by get_diary_channel_groups_dataframe

        Returns
        -------
        data: pandas DataFrame containing timestamp (adjusted), value, and group name

        """
        segment_urls = segments['dataChunks.url']
        group_names = segments['name']
        start_times = segments['segments.startTime']

        data_list = []
        for idx, url in enumerate(segment_urls):
            new_data = utils.get_diary_fitbit_data(url)
            new_data['timestamp'] = new_data['timestamp'] + start_times[idx]
            new_data['name'] = group_names[idx]
            data_list.append(new_data)

        if data_list:
            data = pd.concat(data_list)
        else:
            data = None

        return data

    def get_mood_survey_results(self, survey_template_ids, limit=200, offset=0):
        """Gets a list of dictionaries containing mood survey results

        Parameters
        ----------
        survey_template_ids : A list of survey_template_ids to retrieve results for

        Returns
        -------
        mood_survey_results : a list of dictionaries
                a list of dictionaries containing survey result data

        Example
        -------
        survey_results = get_mood_survey_results("some_id")
        """

        current_offset = offset
        results = []

        while True:
            query_string = graphql.get_mood_survey_results_query_string(
                survey_template_ids, limit, current_offset)
            current_offset += limit

            response = self.execute_query(query_string)['surveys']

            if not response:
                break

            results += response

        return results


    def get_mood_survey_results_dataframe(self, survey_template_ids, limit=200, offset=0):
        """Gets a dataframe containing mood survey results

        Parameters
        ----------
        survey_template_ids : A list of survey_template_ids to retrieve results for

        Returns
        -------
        mood_survey_results : pandas DataFrame
            dataframe with survey.id, survey.lastSubmittedAt, surveyField.key, surveyField.value

        Example
        -------
        survey_results = get_mood_survey_results_dataframe("some_id")
        """

        results = self.get_mood_survey_results(survey_template_ids, limit, offset)

        if results is None or len(results) == 0:
            return pd.DataFrame()

        surveys = json_normalize(results)
        fields = self.pandas_flatten(surveys, '', 'fields')
        surveys = surveys.drop('fields', errors='ignore', axis='columns')
        surveys = surveys.merge(fields, how='left', on='id', suffixes=('', '_y'))

        return surveys

    def get_study_ids_in_study_cohort(self, study_cohort_id, limit=200, offset=0):
        """Gets the IDs of studies in the given StudyCohort

        Parameters
        ----------
        study_cohort_id: the id of StudyCohort to retrieve
        page_size: the number of records to return per page (optional)
        offset: the query offset

        Returns
        -------
        data: a list of Study ids that are in the StudyCohort
        """

        current_offset = offset
        results = []
        while True:
            query_string = graphql.get_study_ids_in_study_cohort_query_string(
                study_cohort_id, limit, current_offset)
            response = self.execute_query(query_string)['studyCohort']['studies']

            if not response:
                break

            results += [study['id'] for study in response]
            current_offset += limit

        return results

    def create_study_cohort(self, name, description=None, key=None, study_ids=None):
        """Creates a new study cohort

        Parameters
        ----------
        name: string
            The name of the study cohort to create
        description: string, optional
            An optional description of the study cohort
        key: string, optional
            An optional key to describe the cohort. Defaults to the ID
        study_ids: list of strings
            A list of study Ids to add to the study cohort

        Returns
        -------
        The study cohort id
        """
        query_string = graphql.create_study_cohort_mutation_string(
            name, description, key, study_ids)
        return self.execute_query(query_string)

    def add_studies_to_study_cohort(self, study_cohort_id, study_ids):
        """Add studies to a study cohort by ID

        Parameters
        ----------
        study_cohort_id: string
            The ID of the study cohort to modify
        study_ids: list of strings
            A list of study IDs to add to the study cohort

        Returns
        -------
        The study cohort id
        """
        query_string = graphql.add_studies_to_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)


    def remove_studies_from_study_cohort(self, study_cohort_id, study_ids):
        """Remove studies from a study cohort by ID

        Parameters
        ----------
        study_cohort_id: string
            The ID of the study cohort to modify
        study_ids: list of strings
            A list of study IDs to remove from the study cohort

        Returns
        -------
        The study cohort id
        """
        query_string = graphql.remove_studies_from_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)
