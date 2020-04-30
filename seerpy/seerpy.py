"""
Define a client class for interacting with the GraphQL API endpoint.

Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

Concepts
--------
- study: A period of time monitoring a patient, often with EEG/ECG
- channel group: A type of monitoring data in a study, e.g. EEG, ECG, video
- channel: A specific channel of a channel group, e.g. for EEG: Fz, C4, Fp1 channels
- label group: Categories of labels relevant to a study, e.g. Abnormal / Epileptiform,
    Normal / Routine, Sleep, Suspect - Low/Medium.
- label: An instance of a label group. Labels typically involve the following
    fields: id, startTime, duration, timezone, note, tags, confidence,
    createdAt, createdBy
- tag: An ontology of "attributes" that may be atached to a label to provide
    info or clarifications, e.g. Jaw clenching, Beta, Exemplar, Generalised, Sleep.
    Tags are arranged into categories, e.g. Band, Brain area, Channel, Seizure type, Sleep
- segment: A duration of recording for a given channel group. Segments lengths
    are variable, though generally capped at 135 minutes (at least for EEG)
- data chunk: Segments are saved to disk as 10-second data chunks, which must be
    reassembled to yield a complete segment
- party ID: The ID associated with e.g. an organisation, which will filter the
    values returned
- API response: Data returned from the GraphQL endpoint. Returned as JSON format,
    so get a dictionary with string keys and values that may be strings, numbers,
    bools, dictionaries, lists of dicts etc.
"""
import math
import time
import json

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from pandas.io.json import json_normalize
import requests
from datetime import datetime

from .auth import SeerAuth, COOKIE_KEY_DEV, COOKIE_KEY_PROD
from . import utils
from . import graphql


class SeerConnect:  # pylint: disable=too-many-public-methods

    def __init__(self, api_url='https://api.seermedical.com/api', email=None, password=None, dev=False):
        """Creates a GraphQL client able to interact with
            the Seer database, handling login and authorisation
        Parameters
        ----------
        api_url : str, optional
            Base URL of API endpoint
        email : str, optional
            The email address for a user's Seer account
        password : str, optional
            User password associated with Seer account
        dev : bool, optional
            dev: Flag to query the development rather than production endpoint
        """

        self.api_url = api_url
        self.dev = dev

        self.login(email, password)

        self.last_query_time = time.time()
        self.api_limit_expire = 300
        self.api_limit = 580

    def login(self, email=None, password=None):
        """
        Authenticate with the API endpoint and set up the GraphQL client with
        the correct URL address and cookie value headers.
        """
        self.seer_auth = SeerAuth(self.api_url, email, password, self.dev)
        cookie = self.seer_auth.cookie

        key = COOKIE_KEY_DEV if self.dev else COOKIE_KEY_PROD
        header =  {
            'Cookie': f'{key}={cookie[key]}'
        }

        def graphql_client(party_id=None):
            """
            Return a GraphQL client with parameters configured for the correct
            URL and cookie header.
            """
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
        """
        Execute a GraphQL query and return response. Handle retrying upon
        failure and rate limiting requests.

        Parameters
        ----------
        query_string: str
            The formatted GraphQL query
        party_id : str, optional
            The organisation/entity to specify for the query
        invocations : int, optional
            Used for recursive calls; don't set directly

        Returns
        -------
        graphql_results : dict
            Query results as a dictionary matching the structure of the query

        Notes
        -----
        See queries in graphql.py for structure of results returned
        """
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
        """
        For queries expecting a large number of objects returned, split query
        into iterative calls to `execute_query()`.

        Parameters
        ----------
        query_string : str
            The formatted GraphQL query
        object_name : str
            Key to retrieve from the response object, e.g. 'studies'
        limit : int
            Batch size for repeated API calls
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        responses: list of dict
            List of query result dictionaries
        """
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
        """
        Take a DataFrame with at least 2 columns:
        - A column named like f"{parent_name}id"
        - A `child_name` column, where each cell is a list of dicts.

        Return a new DataFrame that retains the ID column and creates new
        columns from the dictionary keys.

        Parameters
        ----------
        parent : pd.DataFrame
            A DataFrame with f"{parent_name}id" and `child_name` cols
        parent_name : str
            Any prefix to the 'id' and `child_name` cols in the parent DataFrame
        child_name : str
            The name of the column with list of dict values

        Returns
        -------
        expanded_df : pd.DataFrame
            DataFrame wih columns derived from the `child_name` dicts

        Example
        -------
        >>> df
           start.id                           start.nested
        0         A  [{'key1': 5, 'key2': 6}, {'key1': 7}]
        1         B                          [{'key2': 8}]
        >>> pandas_flatten(df, 'top.', 'nested')
           nested.key1  nested.key2  start.id
        0          5.0          6.0         A
        1          7.0          NaN         A
        2          NaN          8.0         B
        """
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
        """
        Add a new label group to a study.

        Parameters
        ----------
        study_id : str
            Seer study ID
        name : str
            Name of the new label
        description : str
            Free text explanation of the label group
        label_type : str, optional
            Seer label type ID
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        label_group_id : str
            ID of the newly created label group
        """
        query_string = graphql.get_add_label_group_mutation_string(study_id, name, description,
                                                                   label_type)
        response = self.execute_query(query_string, party_id)
        return response['addLabelGroupToStudy']['id']

    def del_label_group(self, group_id):
        """
        Delete a label group from a study.

        Parameters
        ----------
        group_id : str
            Seer label group ID to delete

        Returns
        -------
        label_group_id : str
            ID of the deleted label group
        """
        query_string = graphql.get_remove_label_group_mutation_string(group_id)
        return self.execute_query(query_string)

    def add_labels_batched(self, label_group_id, labels, batch_size=500):
        """
        Add labels to label group in batches.

        Parameters
        ----------
        label_group_id : str
            Seer label group ID
        labels: pd.DateFrame or list of dict
            Should include columns/keys as per `add_labels()`
        batch_size: int, optional
            Number of labels to include per batch

        Returns
        -------
        None
        """
        number_of_batches = math.ceil(len(labels) / batch_size)
        for i in range(number_of_batches):
            start = i * batch_size
            end = start + batch_size
            self.add_labels(label_group_id, labels[start:end])

    def add_labels(self, group_id, labels):
        """
        Add labels to label group.

        Parameters
        ----------
        group_id : str
            Seer label group ID
        labels : pd.DateFrame or list of dict
            Should include the following columns/keys:
            - note : str
                Label note
            - startTime : float
                Label start time in epoch time
            - duration : float
                Duration of event in milliseconds
            - timezone : float
                Offset from UTC time in hours (eg. Melbourne = 11.0)
            - tagIds : list of str
                Tag IDs
            - confidence : float
                Confidence given to label between 0 and 1

        Returns
        -------
        labels_added : dict
            A dict with a single key, 'addLabelsToLabelGroup', that indexes to
            a list of dicts, each with an 'id' key indicating an added label
        """
        if isinstance(labels, pd.DataFrame):
            labels = labels.to_dict('records')
        query_string = graphql.get_add_labels_mutation_string(group_id, labels)
        return self.execute_query(query_string)

    def add_document(self, study_id, document_name, document_path):
        """
        Upload a local document and associate it with a study.

        Parameters
        ----------
        study_id : str
            Seer study ID
        document_name : str
            Name to assign document after upload
        document_path : str
            Path to document on local device

        Returns
        -------
        url : str
            URL of the uploaded document.
        """
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
        """
        Get details of all tag types.

        Returns
        -------
        tags : list of dict
            Descriptions of each tag, with keys:
            - id
            - value
            - category
            - forDiary
            - forStudy
        """
        query_string = graphql.get_tag_id_query_string()
        response = self.execute_query(query_string)
        return response['labelTags']

    def get_tag_ids_dataframe(self):
        """
        Get details of all tag types as a DataFrame. See `get_tag_ids()` for
        details.

        Returns
        -------
        tags_df : pd.DataFrame
            DataFrame with tag details
        """
        tag_ids = self.get_tag_ids()
        tag_ids = json_normalize(tag_ids).sort_index(axis=1)
        return tag_ids

    def get_study_ids(self, limit=50, search_term='', party_id=None):
        """
        Get the IDs of all available studies.

        Parameters
        ----------
        limit : int, optional
            The number of studies to retrieve per API call
        search_term : str, optional
            Filter results to study names matching this string
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_ids : list of str
            A list of all study IDs
        """
        studies = self.get_studies(limit, search_term, party_id)
        return [study['id'] for study in studies]

    def get_studies(self, limit=50, search_term='', party_id=None):
        """
        Get a list of study dicts, with each having keys: 'id', 'name' and 'patient'.

        Parameters
        ----------
        limit : int
            Batch size for repeated API calls
        search_term : str
            A string used to filter the studies returned
        party_id : str
            The organisation/entity to specify for the query

        Returns
        -------
        studies : list of dict
            Study details, each having keys:
            - id
            - name
            - patient
        """
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(search_term)
        return self.get_paginated_response(studies_query_string, 'studies', limit, party_id)

    def get_studies_dataframe(self, limit=50, search_term='', party_id=None):
        """
        Get details of study IDs, names and patient info as a DataFrame. See
        `get_studies()` for details.

        Parameters
        ----------
        limit : int, optional
            Batch size for repeated API calls
        search_term : str, optional
            A string used to filter the studies returned
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_df: pd.DataFrame
            DataFrame with details of all matching studies
        """
        studies = self.get_studies(limit, search_term, party_id)
        studies_dataframe = json_normalize(studies).sort_index(axis=1)
        return studies_dataframe.drop('patient', errors='ignore', axis='columns')

    def get_study_ids_from_names_dataframe(self, study_names, party_id=None):
        """
        Get the IDs of all available studies as a DataFrame. See `get_studies()`
        for details.

        Parameters
        ----------
        study_names : list of str
            Seer study names to retrieve
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_ids_df : pd.DataFrame
            A DataFrarme wihth study names and IDs
        """
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
        """
        Get the IDs of studies corresponding to given study names.
        See `get_studies()` for details.

        Parameters
        ----------
        study_names : str or list of str
            Seer study name or names to look up
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_ids: list of str
            IDs matching the provided study
        """
        return self.get_study_ids_from_names_dataframe(study_names, party_id)['id'].tolist()

    def get_studies_by_id(self, study_ids, limit=50):
        """
        Get a dict of study details for each study ID provided.

        Parameters
        ----------
        study_ids : str or list of str
            Seer study IDs to get details for
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        study_dicts: list of dict
            Details for each study (name, ID etc)
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        studies_query_string = graphql.get_studies_by_study_id_paged_query_string(study_ids)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def get_channel_groups(self, study_id):
        """
        Get details of each channel group for a given study.

        Parameters
        ----------
        study_id: Seer study ID

        Returns
        -------
        study_id : list of dict
            Details for each channel group, with dicts including keys:
            - name
            - sampleRate
            - segments
        """
        query_string = graphql.get_channel_groups_query_string(study_id)
        response = self.execute_query(query_string)
        return response['study']['channelGroups']

    def get_segment_urls(self, segment_ids, limit=10000):
        """
        Get a DataFrame with segment IDs and URLs from which to download them.

        Parameters
        ----------
        segment_ids : list of str
            Iterable of segment IDs
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        segment_url_df : pd.DataFrame
            DataFrame with columns 'baseDataChunkUrl' and 'segments.id'
        """
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
        """
        Get a DataFrame containing download details of all data chunks that
        comprise the segments in a provided metadata DataFrame.

        Parameters
        ----------
        study_metadata : pd.DataFrame
            Study metadata as returned by `get_all_study_metadata_dataframe_by_*()`
        s3_urls : bool, optional
            Return download URLs for S3 (otherwise return URLs for Cloudfront)
        from_time : int, optional
            Timestamp in msec - only retrieve data after this point
        to_time : int, optional
            Timestamp in msec - only retrieve data before this point
        limit : int, options
            Batch size for repeated API calls

        Returns
        -------
        data_chunk_df : pd.DataFrame
            The returned DataFrame has cols:
            - segments.id
            - chunkIndex
            - chunk_start
            - chunk_end
            - chunk_url
        """
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
        """
        Get labels for a given study and label group.

        Parameters
        ----------
        study_id : str
            Seer study ID
        label_group_id : str
            Label group ID string
        from_time : int, optional
            Timestamp in msec - only retrieve data after this point
        to_time : int, optional
            Timestamp in msec - only retrieve data before this point
        limit : int, optional
            Batch size for repeated API calls
        offset : int, optional
            Index of first label to retrieve

        Returns
        -------
        labels : dict
            Has a 'labelGroup' key which indexes to a nested dict with a 'labels' key
        """
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
        """
        Get all labels for a given study and label group as a DataFrame.
        See `get_labels()` for details.

        Returns
        -------
        labels_df : pd.DataFrame
            Details of all matching labels
        """
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
        """
        Get all labels for a given study and label group as an abridged string
        representation.

        Parameters
        ----------
        study_id : str
            Seer study ID
        label_group_id : str
            Label group ID string
        from_time : int, optional
            Timestamp in msec - only retrieve data after this point
        to_time : int, optional
            Timestamp in msec - only retrieve data before this point

        Returns
        -------
        labels_str : dict
            Has a key 'labelString' which indexes a JSON-like string with only
            3 keys per label: 'id', 's' (for startTime), and 'd' (for duration)
        """
        query_string = graphql.get_labels_string_query_string(study_id, label_group_id, from_time,
                                                           to_time)
        response = self.execute_query(query_string)['study']
        return response

    def get_labels_string_dataframe(self, study_id, label_group_id, from_time=0,  # pylint:disable=too-many-arguments
                   to_time=9e12):
        """
        Get all labels for a given study and label group in an abridged string
        representation, as a DataFrame. See `get_labels_string()` for details.

        Returns
        -------
        labels_str_df : pd.DataFrame
            Cols include 'labels.id', 'labels.startTime' and 'labels.duration'
        """
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
        """
        Get label group information for all provided study IDs.

        Parameters
        ----------
        study_ids : str
            Seer study IDs to retrieve label groups for
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        label_groups : list of dict
            Keys included: 'id', 'labelGroups' and 'name'
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]

        labels_query_string = graphql.get_label_groups_for_study_ids_paged_query_string(study_ids)
        return self.get_paginated_response(labels_query_string, 'studies', limit)

    def get_label_groups_for_studies_dataframe(self, study_ids, limit=50):
        """
        Get label group information for all provided study IDs as a DataFrame.
        See `get_label_groups_for_studies()`.

        Returns
        -------
        label_groups_df : pd.DataFrame
            Columns with details on name, id, type, and number of labels, as
            well as study ID and name
        """
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
        """
        Get timestamp info about all parts of a study that have been viewed by
        various users.

        Parameters
        ----------
        study_id : str
            Seer study ID
        limit : int, optional
            Batch size for repeated API calls
        offset : int, optional
            Index of first record to return

        Returns
        -------
        times_df : pd.DataFrame
            Includes cols 'id', 'startTime', 'duration' and 'user'
        """
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

    def get_diary_labels(self, patient_id, label_type='all', offset=0, limit=100, from_time=0, to_time=9e12, from_duration=0, to_duration=9e12):
        """
        Retrieves diary label groups and labels for a given patient.

        Parameters
        ----------
        patient_id: str
                Seer patient ID
        label_type: str
                The type of label to retrieve. Default = 'all'. Options = 'seizure',
                'medications', 'cardiac'.
        offset: int
                Index of first record to return
        limit: int
                Batch size for repeated API calls
        from_time: int
                UTC timestamp to apply a range filter on label start times.
                Retrieves labels after the given from_time
        to_time: int
                UTC timestamp to apply a range filter label start times.
                Retrieves labels before the given to_time
        from_duration: int
                Time in millseconds to apply a range filter on the duration of labels.
                Retrieves labels of duration > from_duration
        to_duration: int
                Time in milliseconds to apply a range filter on the duration of labels.
                Retrieves labels of duration < to_duration

        Returns
        -------
        label_results: dict
                Returns a dict with keys 'id' and 'labelGroups'; 'labelGroups' indexes
                to a list of dict with keys ['id', 'labelType', 'name', 'labels',
                'numberOfLabels', 'labelSourceType']
        """
        label_results = None
        # set true if we need to fetch labels
        query_flag = True

        while True:
            if not query_flag:
                break

            query_string = graphql.get_diary_labels_query_string(patient_id, label_type, limit, offset, from_time, to_time, from_duration, to_duration)
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

    def get_diary_labels_dataframe(self, patient_id, label_type='all', offset=0, limit=100, from_time=0, to_time=9e12, from_duration=0, to_duration=9e12):

        label_results = self.get_diary_labels(patient_id, label_type, offset, limit, from_time, to_time, from_duration, to_duration)
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

    def get_diary_medication_alerts(self, patient_id, from_time=0, to_time=9e12):
        query_string = graphql.get_diary_medication_alerts_query_string(patient_id, from_time, to_time)
        response = self.execute_query(query_string)['patient']['diary']
        return response

    def get_diary_medication_alerts_dataframe(self, patient_id, from_time=0, to_time=9e12):
        results = self.get_diary_medication_alerts(patient_id, from_time, to_time)
        if results is None:
            return results
        alerts = json_normalize(results['alerts']).sort_index(axis=1)
        labels = self.pandas_flatten(alerts, '', 'labels')
        return labels

    def get_diary_medication_compliance(self, patient_id, from_time=0, to_time=0):
        query_string = graphql.get_diary_medication_compliance_query_string(patient_id, from_time, to_time)
        response = self.execute_query(query_string)
        return response

    def get_diary_medication_compliance_dataframe(self, patient_id, from_time=0, to_time=0):
        results = self.get_diary_medication_compliance(patient_id, from_time, to_time)
        if results is None:
            return results

        medication_compliance = json_normalize(results['patient']['diary']['medicationCompliance']).sort_index(axis=1)
        medication_compliance['id'] = patient_id
        return medication_compliance

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
        timezones = segments['segments.timezone']

        data_list = []
        for idx, url in enumerate(segment_urls):
            # timestamps are returned in their utc time
            start_time = datetime.utcfromtimestamp(start_times[idx]/1000)
            new_data = utils.get_diary_fitbit_data(url)
            # convert timestamps to true utc datetime
            new_data['timestamp'] = start_time + pd.to_timedelta(new_data['timestamp'], unit='ms')
            new_data['name'] = group_names[idx]
            new_data['timezone'] = timezones[idx]
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

    def get_user_ids_in_user_cohort(self, user_cohort_id, limit=200, offset=0):
        """Gets the IDs of users in the given UserCohort

        Parameters
        ----------
        user_cohort_id: the id of UserCohort to retrieve
        page_size: the number of records to return per page (optional)
        offset: the query offset

        Returns
        -------
        data: a list of User ids that are in the UserCohort
        """

        current_offset = offset
        results = []
        while True:
            query_string = graphql.get_user_ids_in_user_cohort_query_string(
                user_cohort_id, limit, current_offset)
            response = self.execute_query(query_string)['userCohort']['users']

            if not response:
                break

            results += [user['id'] for user in response]
            current_offset += limit

        return results

    def create_user_cohort(self, name, description=None, key=None, user_ids=None):
        """Creates a new UserCohort

        Parameters
        ----------
        name: string
            The name of the user cohort to create
        description: string, optional
            An optional description of the user cohort
        key: string, optional
            An optional key to describe the cohort. Defaults to the ID
        user_ids: list of strings
            A list of user Ids to add to the user cohort

        Returns
        -------
        The user cohort id
        """
        query_string = graphql.get_create_user_cohort_mutation_string(
            name, description, key, user_ids)
        return self.execute_query(query_string)

    def add_users_to_user_cohort(self, user_cohort_id, user_ids):
        """Add users to a user cohort by ID

        Parameters
        ----------
        user_cohort_id: string
            The ID of the user cohort to modify
        user_ids: list of strings
            A list of user IDs to add to the user cohort

        Returns
        -------
        The user cohort id
        """
        query_string = graphql.get_add_users_to_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)


    def remove_users_from_user_cohort(self, user_cohort_id, user_ids):
        """Remove users from a user cohort by ID

        Parameters
        ----------
        user_cohort_id: string
            The ID of the user cohort to modify
        user_ids: list of strings
            A list of user IDs to remove from the user cohort

        Returns
        -------
        The user cohort id
        """
        query_string = graphql.get_remove_users_from_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)
