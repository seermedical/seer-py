"""
Single-class module to define a GraphQL client for interacting with the API.

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
from datetime import datetime
import json
import math
import time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Union

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from pandas.io.json import json_normalize
import requests

from .auth import SeerAuth, COOKIE_KEY_DEV, COOKIE_KEY_PROD
from . import utils
from . import graphql

# Custom datatypes. See module docstring for details
Labels = Union[pd.DataFrame, Iterable[Mapping[str, Any]]]
ApiResponse = Dict[str, Union[str, bool, int, float, None, List[Any], Dict[str, Any]]]


class SeerConnect:
    """Client to handle making calls to the GraphQL API"""

    # pylint: disable=too-many-public-methods

    def __init__(self, api_url: str = 'https://api.seermedical.com/api', email: str = None,
                 password: str = None, dev: bool = False):
        """
        Create a GraphQL client able to interact with the Seer API endpoint,
        handling login and authorisation.

        Parameters
        ----------
        api_url: The base URL of the API endpoint
        email: The email address for a user's https://app.seermedical.com account
        password: The password for a user's https://app.seermedical.com account
        dev: Flag to query the dev rather than production endpoint
        """
        self.api_url = api_url
        self.dev = dev
        self.seer_auth = None
        self.graphql_client = None
        self.last_query_time = None

        self.login(email, password)

        self.api_limit_expire = 300
        self.api_limit = 580

    def login(self, email: str = None, password: str = None) -> None:
        """
        Authenticate with the API endpoint and set up the GraphQL client with
        the correct URL address and cookie value headers.
        """
        self.seer_auth = SeerAuth(self.api_url, email, password, self.dev)
        cookie = self.seer_auth.cookie

        key = COOKIE_KEY_DEV if self.dev else COOKIE_KEY_PROD
        header = {'Cookie': f'{key}={cookie[key]}'}

        def graphql_client(party_id: str = None) -> GQLClient:
            """
            Return a GraphQL client with parameters configured for the correct
            URL and cookie header.
            """
            url_suffix = '?partyId=' + party_id if party_id else ''
            url = self.api_url + '/graphql' + url_suffix
            return GQLClient(
                transport=RequestsHTTPTransport(url=url, headers=header, use_json=True, timeout=30))

        self.graphql_client = graphql_client
        self.last_query_time = time.time()

    def execute_query(self, query_string: str, party_id: str = None,
                      invocations: int = 0) -> ApiResponse:
        """
        Execute a GraphQL query and return response. Handle retrying upon
        failure and rate limiting requests.

        Parameters
        ----------
        query_string: The formatted GraphQL query
        party_id: The organisation/entity to specify for the query
        invocations: Used for recursive calls; don't set directly

        Returns: Dictionary of str: API result, e.g. dict, str, list of dict...
        """
        resolvable_api_errors = [
            '502 Server Error', '503 Server Error', '504 Server Error'
            'Read timed out.', 'NOT_AUTHENTICATED'
        ]

        try:
            time.sleep(
                max(0, ((self.api_limit_expire / self.api_limit) -
                        (time.time() - self.last_query_time))))
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
                    time.sleep(
                        min(30 * (invocations + 1)**2,
                            max(self.last_query_time + self.api_limit_expire - time.time(), 0)))
                invocations += 1
                self.login()
                return self.execute_query(query_string, party_id, invocations=invocations)

            raise

    def get_paginated_response(self, query_string: str, object_name: str, limit: int = 250,
                               party_id: str = None) -> List[ApiResponse]:
        """
        For queries expecting a large number of matching objects, divide query
        over iterative calls to `execute_query()`.

        Parameters
        ----------
        query_string: The formatted GraphQL query
        object_name: Key to retrieve from the response object, e.g. 'studies'
        limit: Batch size for repeated API calls
        party_id: The organisation/entity to specify for the query

        Returns: List of dict of str: API result, e.g. dict, str, list of dict...
        """
        offset = 0
        objects = []
        while True:
            formatted_query_string = query_string.format(limit=limit, offset=offset)
            response = self.execute_query(formatted_query_string, party_id)[object_name]
            if not response:
                break
            objects = objects + response
            offset += limit
        return objects

    @staticmethod
    def pandas_flatten(parent: pd.DataFrame, parent_name: str, child_name: str) -> pd.DataFrame:
        """
        Expand a 'nested' column of a DataFrame into a new DataFrame.
        The DataFrame should have:
        - A `child_name` column, where each cell is a list of dict. These dict
            keys will become the columns of the new DataFrame
            will also be included in the returned DataFrame
        - An `id` column, which may have a `parent_name` prefix. This column

        Parameters
        ----------
        parent: The DataFrame with a `parent_name`'id' col, and `child_name` col of
        parent_name: The prefix to the 'id' column in the DataFrame
        child_name: The name of the column with nest

        Returns: DataFrame wih cols expanded from parent DataFrame nested col
        """
        child_list = []
        for i in range(len(parent)):
            parent_id = parent[parent_name + 'id'][i]
            child = json_normalize(parent[parent_name + child_name][i]).sort_index(axis=1)
            child.columns = [child_name + '.' + str(col) for col in child.columns]
            child[parent_name + 'id'] = parent_id
            child_list.append(child)

        if child_list:
            child = pd.concat(child_list, sort=True).reset_index(drop=True)
        if not child_list or child.empty:
            columns = [parent_name + 'id', child_name + '.id']
            child = pd.DataFrame(columns=columns)
        return child

    def add_label_group(self, study_id: str, name: str, description: str, label_type: str = None,
                        party_id: str = None) -> str:
        """
        Add a new label group to a study, returning the ID of the newly created
        label group.

        Parameters
        ----------
        study_id: Seer study ID
        name: Name of the new label
        description: Free text explanation/notes on the label group
        label_type: Seer label type ID
        party_id: The organisation/entity to specify for the query
        """
        query_string = graphql.get_add_label_group_mutation_string(study_id, name, description,
                                                                   label_type)
        response = self.execute_query(query_string, party_id)
        return response['addLabelGroupToStudy']['id']

    def del_label_group(self, group_id: str) -> str:
        """
        Delete a label group from a study, returning the ID of the deleted
        label group.

        Parameters
        ----------
        group_id: Seer label group ID to delete.
        """
        query_string = graphql.get_remove_label_group_mutation_string(group_id)
        return self.execute_query(query_string)

    def add_labels_batched(self, label_group_id: str, labels: Labels,
                           batch_size: int = 500) -> None:
        """
        Add labels to a label group in batches.

        Parameters
        ----------
        label_group_id: Seer label group ID
        labels: list of LabelData. See `add_labels()` for details
        batch_size (Optional): Number of labels to add in a batch
        """
        number_of_batches = math.ceil(len(labels) / batch_size)
        for i in range(number_of_batches):
            start = i * batch_size
            end = start + batch_size
            self.add_labels(label_group_id, labels[start:end])

    def add_labels(self, group_id: str, labels: Labels) -> ApiResponse:
        """
        Add labels to a label group. Returns a dict with a single key,
        'addLabelsToLabelGroup', that indexes to a list of dicts, each having
        an 'id' key indicating an added label.

        Parameters
        ----------
        group_id: Seer label group ID

        labels: Iterable of dict or pd.DataFrame with following values:
            - note (str): label note
            - startTime (float): label start time in epoch time
            - duration (float): duration of event in milliseconds
            - timezone (float): offset from UTC time in hours (eg. Melbourne = 11.0)
            - tagIds (List[str]): tag IDs
            - confidence (float): Confidence given to label between 0 and 1
        """
        if isinstance(labels, pd.DataFrame):
            labels = labels.to_dict('records')
        query_string = graphql.get_add_labels_mutation_string(group_id, labels)
        return self.execute_query(query_string)

    def add_document(self, study_id: str, document_name: str, document_path: str) -> str:
        """
        Upload a local document and associate with a study. Returns download
        URL for the uploaded document.

        Parameters
        ----------
        study_id: Seer study ID
        document_name: Name to assign document after upload
        document_path: Path to document on local device
        """
        query_string = graphql.get_add_document_mutation_string(study_id, document_name)
        response_add = self.execute_query(query_string)['createStudyDocuments'][0]
        with open(document_path, 'rb') as f:
            response_put = requests.put(response_add['uploadFileUrl'], data=f)
        if response_put.status_code == 200:
            query_string = graphql.get_confirm_document_mutation_string(
                study_id, response_add['id'])
            response_confirm = self.execute_query(query_string)
            return response_confirm['confirmStudyDocuments'][0]['downloadFileUrl']
        raise RuntimeError('Error uploading document: status code ' + str(response_put.status_code))

    def get_tag_ids(self) -> List[ApiResponse]:
        """
        Get details of all tag types as a list of dict.
        Keys included: ['id', 'value', 'category', 'forDiary', 'forStudy']
        """
        query_string = graphql.get_tag_id_query_string()
        response = self.execute_query(query_string)
        return response['labelTags']

    def get_tag_ids_dataframe(self) -> pd.DataFrame:
        """
        Get details of all tag types as a DataFrame. See `get_tag_ids()` for details.
        """
        tag_ids = self.get_tag_ids()
        tag_ids = json_normalize(tag_ids).sort_index(axis=1)
        return tag_ids

    def get_study_ids(self, limit: int = 50, search_term: str = '',
                      party_id: str = None) -> List[str]:
        """
        Get the IDs of all available studies.

        Parameters
        ----------
        limit: The number of studies to retrieve per API call
        search_term: A string used to filter the studies returned
        party_id: The organisation/entity to specify for the query
        """
        studies = self.get_studies(limit, search_term, party_id)
        return [study['id'] for study in studies]

    def get_studies(self, limit: int = 50, search_term: str = '',
                    party_id: str = None) -> List[ApiResponse]:
        """
        Get a list of study dicts, with each having keys: 'id', 'name' and 'patient'.

        Parameters
        ----------
        limit: Batch size for repeated API calls
        search_term: A string used to filter the studies returned
        party_id: The organisation/entity to specify for the query
        """
        studies_query_string = graphql.get_studies_by_search_term_paged_query_string(search_term)
        return self.get_paginated_response(studies_query_string, 'studies', limit, party_id)

    def get_studies_dataframe(self, limit: int = 50, search_term: str = '',
                              party_id: str = None) -> pd.DataFrame:
        """
        Get details of study IDs, names and patient info as a DataFrame. See
        `get_studies()` for details.

        Parameters
        ----------
        limit: Batch size for repeated API calls
        search_term: A string used to filter the studies returned
        party_id: The organisation/entity to specify for the query
        """
        studies = self.get_studies(limit, search_term, party_id)
        studies_dataframe = json_normalize(studies).sort_index(axis=1)
        return studies_dataframe.drop('patient', errors='ignore', axis='columns')

    def get_study_ids_from_names_dataframe(self, study_names: Union[str, Iterable[str]],
                                           party_id: str = None) -> pd.DataFrame:
        """
        Get the IDs of all available studies as a DataFrame. See `get_studies()`
        for details.

        Parameters
        ----------
        study_names: Iterable of Seer study names
        party_id: The organisation/entity to specify for the query
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

    def get_study_ids_from_names(self, study_names: Union[str, Iterable[str]],
                                 party_id: str = None) -> List[str]:
        """
        Get the IDs of studies corresponding to given study names.
        See `get_studies()` for details.

        Parameters
        ----------
        study_names: Iterable of Seer study names
        party_id: The organisation/entity to specify for the query
        """
        return self.get_study_ids_from_names_dataframe(study_names, party_id)['id'].tolist()

    def get_studies_by_id(self, study_ids: Union[str, Iterable[str]],
                          limit: int = 50) -> List[ApiResponse]:
        """
        Get a list of study dicts corresponding to a list of study IDs.

        Parameters
        ----------
        study_ids: Seer study IDs to get details for
        limit: Batch size for repeated API calls
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        studies_query_string = graphql.get_studies_by_study_id_paged_query_string(study_ids)
        return self.get_paginated_response(studies_query_string, 'studies', limit)

    def get_channel_groups(self, study_id: str) -> List[ApiResponse]:
        """
        Get a list of channel group dicts for a given study. Channel group dicts
        may include keys such as ['name', 'sampleRate', 'segments'].

        Parameters
        ----------
        study_id: Seer study ID
        """
        query_string = graphql.get_channel_groups_query_string(study_id)
        response = self.execute_query(query_string)
        return response['study']['channelGroups']

    def get_segment_urls(self, segment_ids: Iterable[str], limit: int = 10000) -> pd.DataFrame:
        """
        Get a DataFrame matching segment IDs an URLs to download them from.
        DataFrame will have columns ['baseDataChunkUrl', 'segments.id']

        Parameters
        ----------
        segment_ids: Iterable of segment IDs
        limit: Batch size for repeated API calls
        """
        if not segment_ids:
            return pd.DataFrame(columns=['baseDataChunkUrl', 'segments.id'])

        segments = []
        counter = 0
        while int(counter * limit) < len(segment_ids):
            segment_ids_batch = segment_ids[int(counter * limit):int((counter + 1) * limit)]
            query_string = graphql.get_segment_urls_query_string(segment_ids_batch)
            response = self.execute_query(query_string)
            segments.extend([
                segment for segment in response['studyChannelGroupSegments'] if segment is not None
            ])
            counter += 1
        segment_urls = pd.DataFrame(segments)
        segment_urls = segment_urls.rename(columns={'id': 'segments.id'})
        return segment_urls

    def get_data_chunk_urls(self, study_metadata: pd.DataFrame, s3_urls: bool = True,
                            from_time: int = 0, to_time: int = 9e12,
                            limit: int = 10000) -> pd.DataFrame:
        """
        Get a DataFrame containing download details of all data chunks that
        comprise segments in a metadata DataFrame. The returned DataFrame has cols:
        ['segments.id', 'chunkIndex', 'chunk_start', 'chunk_end', 'chunk_url']

        Parameters
        ----------
        study_metadata: Study metadata, as returned by `get_all_study_metadata_dataframe_by_*()`
        s3_urls: Return download URLs for S3 (otherwise return URLs for Cloudfront)
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        limit: Batch size for repeated API calls
        """
        if study_metadata.empty:
            return pd.DataFrame(
                columns=['segments.id', 'chunkIndex', 'chunk_start', 'chunk_end', 'chunk_url'])

        study_metadata = study_metadata.drop_duplicates('segments.id')
        study_metadata = study_metadata[study_metadata['segments.startTime'] <= to_time]
        study_metadata = study_metadata[study_metadata['segments.startTime']
                                        + study_metadata['segments.duration'] >= from_time]

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
                    chunk_metadata.append({
                        'segments.id': row[3],
                        'chunkIndex': i,
                        'chunk_start': chunk_start,
                        'chunk_end': chunk_end
                    })
        if not data_chunks:
            return pd.DataFrame(
                columns=['segments.id', 'chunkIndex', 'chunk_start', 'chunk_end', 'chunk_url'])
        chunks = []
        counter = 0
        while int(counter * limit) < len(data_chunks):
            data_chunks_batch = data_chunks[int(counter * limit):int((counter + 1) * limit)]
            query_string = graphql.get_data_chunk_urls_query_string(data_chunks_batch, s3_urls)
            response = self.execute_query(query_string)
            chunks.extend([
                chunk for chunk in response['studyChannelGroupDataChunkUrls'] if chunk is not None
            ])
            counter += 1
        data_chunk_urls = pd.DataFrame(chunk_metadata)
        data_chunk_urls['chunk_url'] = chunks

        return data_chunk_urls

    def get_labels(self, study_id: str, label_group_id: str, from_time: int = 0,
                   to_time: int = 9e12, limit: int = 200, offset: int = 0) -> ApiResponse:
        """
        Get all labels for a given study and label group. The returned dict has
        key 'labelGroup' which indexes to a dictionary with a 'labels' key

        Parameters
        ----------
        study_id: Seer study ID
        label_group_id: Label group ID string
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        limit: Batch size for repeated API calls
        offset: Index of first label to retrieve
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

    def get_labels_dataframe(self, study_id: str, label_group_id: str, from_time: int = 0,
                             to_time: int = 9e12, limit: int = 200,
                             offset: int = 0) -> Union[pd.DataFrame, None]:
        """
        Get all labels for a given study and label group as a DataFrame.
        See `get_labels()` for details.

        Parameters
        ----------
        study_id: Seer study ID
        label_group_id: Label group ID string
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        limit: Batch size for repeated API calls
        offset: Index of first label to retrieve
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

    def get_labels_string(self, study_id: str, label_group_id: str, from_time: int = 0,
                          to_time: int = 9e12) -> ApiResponse:
        """
        Get all labels for a given study and label group in an abridged format.
        Instead of a list of label dicts as returned by `get_labels()`, the
        'labelString' key indexes a stringified JSON representation with only 3
        keys per label: 'id', 's' (for startTime), and 'd' (for duration)

        Parameters
        ----------
        study_id: Seer study ID
        label_group_id: Label group ID string
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        query_string = graphql.get_labels_string_query_string(study_id, label_group_id, from_time,
                                                              to_time)
        response = self.execute_query(query_string)['study']
        return response

    def get_labels_string_dataframe(self, study_id: str, label_group_id: str, from_time: int = 0,
                                    to_time: int = 9e12) -> pd.DataFrame:
        """
        Get all labels for a given study and label group in an abridged format,
        as a DataFrame. Cols include ['labels.id', 'labels.startTime', 'labels.duration']

        Parameters
        ----------
        study_id: Seer study ID
        label_group_id: Label group ID string
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        label_results = self.get_labels_string(study_id, label_group_id, from_time=from_time,
                                               to_time=to_time)
        if label_results is None:
            return label_results
        label_group = json_normalize(label_results).sort_index(axis=1)
        label_group['labelGroup.labelString'] = (
            label_group['labelGroup.labelString'].apply(json.loads))
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labelString')
        label_group = label_group.drop('labelGroup.labelString', errors='ignore', axis='columns')
        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.rename(
            columns={
                'labelString.d': 'labels.duration',
                'labelString.id': 'labels.id',
                'labelString.s': 'labels.startTime'
            })
        return label_group

    def get_label_groups_for_studies(self, study_ids: Union[str, Iterable[str]],
                                     limit: int = 50) -> List[ApiResponse]:
        """
        Get label group information for all provided study IDs. Keys returned:
        ['id', 'labelGroups', 'name'].

        Parameters
        ----------
        study_ids: Seer study IDs to retrieve label groups for
        limit: Batch size for repeated API calls
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]

        labels_query_string = graphql.get_label_groups_for_study_ids_paged_query_string(study_ids)
        return self.get_paginated_response(labels_query_string, 'studies', limit)

    def get_label_groups_for_studies_dataframe(self, study_ids: Union[str, Iterable[str]],
                                               limit: int = 50) -> pd.DataFrame:
        """
        Get label group information for all provided study IDs as a DataFrame:
        name, id, type, and number of labels, as well as study ID and name.

        Parameters
        ----------
        study_ids: Seer study IDs to retrieve label groups for
        limit: Batch size for repeated API calls
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

    def get_viewed_times_dataframe(self, study_id: str, limit: int = 250,
                                   offset: int = 0) -> pd.DataFrame:
        """
        Get timestamp info about all parts of a study that have been viewed by
        various users. DataFrame includes cols ['id', 'startTime', 'duration' 'user']

        Parameters
        ----------
        study_id: Seer study ID
        limit: Batch size for repeated API calls
        offset: Index of first record to return
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

    def get_organisations(self) -> List[ApiResponse]:
        """
        Get a list of dict of organisation details, including 'id' and 'name'.
        """
        query_string = graphql.get_organisations_query_string()
        response = self.execute_query(query_string)['organisations']
        return response

    def get_organisations_dataframe(self) -> pd.DataFrame:
        """
        Get a DataFrame of organisation details, including 'id' and 'name'.
        """
        orgs = self.get_organisations()
        if orgs is None:
            return orgs
        return pd.DataFrame(orgs)

    def get_patients(self, party_id: str = None) -> List[ApiResponse]:
        """
        Get a list of dict of all available patients. Keys are ['id', 'user']

        Parameters
        ----------
        party_id: The organisation/entity to specify for the query
        """
        query_string = graphql.get_patients_query_string()
        response = self.execute_query(query_string, party_id)['patients']
        return response

    def get_patients_dataframe(self, party_id: str = None) -> pd.DataFrame:
        """
        Get a DataFrame of all available patients, with cols ['id', 'user']

        Parameters
        ----------
        party_id: The organisation/entity to specify for the query
        """
        patients = self.get_patients(party_id)
        if patients is None:
            return patients
        return json_normalize(patients).sort_index(axis=1)

    def get_documents_for_studies(self, study_ids: Union[str, Iterable[str]],
                                  limit: int = 50) -> List[ApiResponse]:
        """
        Get list of dicts of details of all documents associated with provided
        study IDs. Fields include ['id', 'name', 'fileSize', 'downloadFileUrl']

        Parameters
        ----------
        study_id: Iterable of Seer study ID
        limit: Batch size for repeated API calls
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        documents_query_string = graphql.get_documents_for_study_ids_paged_query_string(study_ids)
        return self.get_paginated_response(documents_query_string, 'studies', limit)

    def get_documents_for_studies_dataframe(self, study_ids: Union[str, Iterable[str]],
                                            limit: int = 50):
        """
        Get DataFrame of details of all documents associated with provided
        study IDs. See `get_documents_for_studies()` for details.

        Parameters
        ----------
        study_id: Iterable of Seer study ID
        limit: Batch size for repeated API calls
        """
        documents = []
        for study in self.get_documents_for_studies(study_ids, limit):
            for document in study['documents']:
                document['document.id'] = document.pop('id')
                document['document.name'] = document.pop('name')
                document['id'] = study['id']
                document['name'] = study['name']
                documents.append(document)
        return pd.DataFrame(documents)

    def get_diary_labels(self, patient_id: str, offset: int = 0, limit: int = 100) -> ApiResponse:
        """
        Get all diary label groups and labels for a given patient.
        Returns a dict with keys 'id' and 'labelGroups'; 'labelGroups' indexes
        to a list of dict with keys ['id', 'labelType', 'name', 'labels',
        'numberOfLabels', 'labelSourceType'].

        Parameters
        ----------
        patient_id: The Seer patient ID to retrieve diary labels from
        offset: Index of first record to return
        limit: Batch size for repeated API calls
        """
        label_results = None
        query_flag = True  # Set True if need to fetch more labels

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

                if len(labels) >= limit:  # Need to fetch more labels
                    query_flag = True

                if label_results is None:
                    label_results = response
                    if any([
                            index['numberOfLabels']
                            for index in response['labelGroups']
                            if index['numberOfLabels'] >= limit
                    ]):
                        query_flag = True
                    break

                label_results['labelGroups'][idx]['labels'].extend(labels)
            offset += limit

        return label_results

    def get_diary_labels_dataframe(self, patient_id: str) -> pd.DataFrame:
        """
        Get all diary label groups and labels for a given patient as a DataFrame.
        See `get_diary_labels()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        """
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
        label_groups = label_groups.rename({'id': 'labelGroups.id'})
        label_groups['id'] = patient_id
        return label_groups

    def get_diary_medication_alerts(self, patient_id: str, from_time: int = 0,
                                    to_time: int = 9e12) -> ApiResponse:
        """
        Get diary medication alerts for a given patient as a dict with keys
        ['id', 'alerts']. 'alerts' indexes to a list of labels with keys
        ['id', 'name', 'labels']; 'labels' is a list of dict with 'doses', 'alert',
        'startTime', 'scheduledTime' etc.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        query_string = graphql.get_diary_medication_alerts_query_string(
            patient_id, from_time, to_time)
        response = self.execute_query(query_string)['patient']['diary']
        return response

    def get_diary_medication_alerts_dataframe(self, patient_id: str, from_time: int = 0,
                                              to_time: int = 9e12) -> pd.DataFrame:
        """
        Get diary medication alerts for a given patient as a DataFrame. See
        `get_diary_medication_alerts()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        results = self.get_diary_medication_alerts(patient_id, from_time, to_time)
        if results is None:
            return results
        alerts = json_normalize(results['alerts']).sort_index(axis=1)
        labels = self.pandas_flatten(alerts, '', 'labels')
        return labels

    def get_diary_medication_compliance(self, patient_id: str, from_time: int = 0,
                                        to_time: int = 0) -> ApiResponse:
        """
        Get all medication compliance records for a given patient. Returns a dict
        with a single key, 'patient', which indexes to a dict with keys ['id',
        'diary']. 'diary' indexes to a dict with a 'medicationCompliance' key.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        query_string = graphql.get_diary_medication_compliance_query_string(
            patient_id, from_time, to_time)
        response = self.execute_query(query_string)
        return response

    def get_diary_medication_compliance_dataframe(self, patient_id: str, from_time: int = 0,
                                                  to_time=0) -> pd.DataFrame:
        """
        Get all medication compliance records for a given patient as a DataFrame.
        See `get_diary_medication_compliance()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        results = self.get_diary_medication_compliance(patient_id, from_time, to_time)
        if results is None:
            return results

        medication_compliance = json_normalize(
            results['patient']['diary']['medicationCompliance']).sort_index(axis=1)
        medication_compliance['id'] = patient_id
        return medication_compliance

    def get_all_study_metadata_by_names(self, study_names: Union[str, Iterable[str]] = None,
                                        party_id: str = None) -> ApiResponse:
        """
        Get all metadata available about named studies. See
        `get_all_study_metadata_by_ids()` for details.

        Parameters
        ----------
        study_names: A list of study names. If not provided, data will be
            returned for all studies
        party_id: The organisation/entity to specify for the query
        """
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names, party_id)
        return self.get_all_study_metadata_by_ids(study_ids)

    def get_all_study_metadata_by_ids(self, study_ids: Iterable[str] = None) -> ApiResponse:
        """
        Get all metadata available about studies with supplied IDs. Returns a
        dict with one key, 'studies', which indexes to a list of dicts with keys
        ['id', 'name', 'description', 'patient' and 'channelGroups']. 'channelGroup'
        indexes to a dict with keys ['channels', 'segments', 'sampleRate'] etc.

        Parameters
        ----------
        study_ids: A list of study IDs. If not provided, data will be returned
            for all available studies.
        """
        if study_ids is None:
            study_ids = self.get_study_ids()
        elif not study_ids:  # Treat empty list as asking for nothing, not everything
            return {'studies': []}

        result = [
            self.execute_query(graphql.get_study_with_data_query_string(study_id))['study']
            for study_id in study_ids
        ]

        return {'studies': result}

    def get_all_study_metadata_dataframe_by_names(self, study_names: Iterable[str] = None
                                                  ) -> pd.DataFrame:
        """
        Get all metadata available about studies with the suppled names as a
        DataFrame. See `get_all_study_metadata_by_ids()` for details.

        Parameters
        ----------
        study_names: Iterable of study names
        """
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names)
        return self.get_all_study_metadata_dataframe_by_ids(study_ids)

    def get_all_study_metadata_dataframe_by_ids(self,
                                                study_ids: Iterable[str] = None) -> pd.DataFrame:
        """
        Get all metadata available about studies with the suppled IDs as a
        DataFrame. See `get_all_study_metadata_by_ids()` for more details.

        Parameters
        ----------
        study_ids: Iterable of study IDs
        """
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

    def get_channel_data(self, all_data: pd.DataFrame, segment_urls: pd.DataFrame = None,
                         download_function: Callable = requests.get, threads: int = None,
                         from_time: int = 0, to_time: int = 9e12) -> pd.DataFrame:
        """
        Download raw data chunks and return as a single DataFrame. Returns a
        DataFrame with ['time', 'id', 'channelGroups.id', 'segments.id'] cols,
        as well as a column for each data channel, e.g. each EEG electrode.

        Parameters
        ----------
        all_data: Study metadata, as returned by `get_all_study_metadata_dataframe_by_*()`
        segment_urls: DataFrame with columns ['segments.id', 'baseDataChunkUrl'].
            If None, these will be retrieved for each segment in `all_data`.
        download_function: The function used to download the channel data.
            Defaults to requests.get
        threads: Number of threads to use. If > 1 will use multiprocessing.
            If None (default), will use 1 on Windows and 5 on Linux/MacOS.
        from_time: Timestamp in msec - only retrieve data after this point
        to_time: Timestamp in msec - only retrieve data before this point
        """
        if segment_urls is None:
            segment_ids = all_data['segments.id'].drop_duplicates().tolist()
            segment_urls = self.get_segment_urls(segment_ids)

        return utils.get_channel_data(all_data, segment_urls, download_function, threads, from_time,
                                      to_time)

    def get_all_bookings(self, organisation_id: str, start_time: int,
                         end_time: int) -> List[Dict[str, ApiResponse]]:
        """
        Get all bookings for any studies that are active at any point between
        `start_time` and `end_time`, as a list of dict. Keys include ['id',
        'startTime', 'endTime', 'patient', 'referral', 'equipmentItems', 'location']

        Parameters
        ----------
        organisation_id: Organisation ID associated with patient bookings
        start_time: Timestamp in msec - find studies active after this point
        end_time: Timestamp in msec - find studies active before this point
        """
        query_string = graphql.get_bookings_query_string(organisation_id, start_time, end_time)
        response = self.execute_query(query_string)
        return response['organisation']['bookings']

    def get_all_bookings_dataframe(self, organisation_id: str, start_time: int,
                                   end_time: int) -> pd.DataFrame:
        """
        Get all bookings for any studies that are active at any point between
        `start_time` and `end_time` as a DataFrame. See `get_all_bookings()`.

        Parameters
        ----------
        organisation_id: Organisation ID associated with patient bookings
        start_time: Timestamp in msec - find studies active after this point
        end_time: Timestamp in msec - find studies active before this point
        """
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
    def get_diary_data_groups(self, patient_id: str, limit: int = 20,
                              offset: int = 0) -> List[ApiResponse]:
        """
        Get details of all label groups for a given patient's diary study.
        Keys returned: ['id', 'name', 'numberOfLabels']

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary data
        limit: The maximum number of results to return
        offset: The index of the first label group to return. Useful in
            conjunction with `limit` for repeated calls
        """
        # TODO use limit/offset for pagination -
        # Unlikely to be more than 20 label groups for a while)
        query_string = graphql.get_diary_study_label_groups_string(patient_id, limit, offset)
        response = self.execute_query(query_string)['patient']['diaryStudy']
        label_groups = response['labelGroups']
        return label_groups

    def get_diary_data_groups_dataframe(self, patient_id: str, limit: int = 20,
                                        offset: int = 0) -> pd.DataFrame:
        """
        Get details of all label groups for a given patient's diary study as a
        DataFrame. See `get_diary_data_groups()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        limit: The maximum number of results to return
        offset: The index of the first label group to return. Useful in
            conjunction with `limit` for repeated calls
        """
        label_group_results = self.get_diary_data_groups(patient_id, limit, offset)
        if label_group_results is None:
            return label_group_results
        label_groups = json_normalize(label_group_results).sort_index(axis=1)
        return label_groups

    def get_diary_data_labels(self, patient_id: str, label_group_id: str, from_time: int = 0,
                              to_time: int = 9e12, limit: int = 200,
                              offset: int = 0) -> ApiResponse:
        """
        Get all diary labels for a given patient and label group. Returns a
        dict with one key, 'labelGroup', which indexes to a dict with a 'labels'
        key. Labels include ['id', 'startTime', 'duration', 'tags', 'timezone']

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        label_group_id: The ID of the label group for which to retrieve labels
        from_time: Timestamp in msec - find diary labels after this point
        to_time: Timestamp in msec - find diary labels before this point
        limit: Batch size for repeated API calls
        offset: The index of the first label group to return
        """
        label_results = None

        while True:
            query_string = graphql.get_labels_for_diary_study_query_string(
                patient_id, label_group_id, from_time, to_time, limit, offset)
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

    def get_diary_data_labels_dataframe(self, patient_id: str, label_group_id: str,
                                        from_time: int = 0, to_time: int = 9e12, limit: int = 200,
                                        offset: int = 0) -> pd.DataFrame:
        """
        Get all diary labels for a given patient and label group as a DataFrame.
        See `get_diary_data_labels()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary labels
        label_group_id: The ID of the label group for which to retrieve labels
        from_time: Timestamp in msec - find diary labels after this point
        to_time: Timestamp in msec - find diary labels before this point
        limit: Batch size for repeated API calls
        offset: The index of the first label group to return
        """
        label_results = self.get_diary_data_labels(patient_id, label_group_id, from_time, to_time,
                                                   limit, offset)
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

    def get_diary_channel_groups(self, patient_id: str, from_time: int,
                                 to_time: int) -> List[ApiResponse]:
        """
        Get all channel groups and associated segment information for a given
        patient. Returns a list of dicts, each having keys ['id', 'name',
        'startTime', 'segments']

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary label groups
        from_time: Timestamp in msec - find diary labels after this point
        to_time: Timestamp in msec - find diary labels before this point
        """
        query_string = graphql.get_diary_study_channel_groups_query_string(
            patient_id, from_time, to_time)
        response = self.execute_query(query_string)
        return response['patient']['diaryStudy']['channelGroups']

    def get_diary_channel_groups_dataframe(self, patient_id: str, from_time: int = 0,
                                           to_time: int = 9e13) -> pd.DataFrame:
        """
        Get all channel groups and associated segment information for a given
        patient, as a DataFrame. See `get_diary_channel_groups()` for details.

        Parameters
        ----------
        patient_id: The Seer patient ID for which to retrieve diary label groups
        from_time: Timestamp in msec - find diary labels after this point
        to_time: Timestamp in msec - find diary labels before this point
        """
        metadata = self.get_diary_channel_groups(patient_id, from_time, to_time)
        channel_groups = json_normalize(metadata).sort_index(axis=1)
        if channel_groups.empty:
            return None

        segments = self.pandas_flatten(channel_groups, '', 'segments')
        data_chunks = self.pandas_flatten(segments, 'segments.', 'dataChunks')

        channel_groups = channel_groups.drop('segments', errors='ignore', axis='columns')
        segments = segments.drop('segments.dataChunks', errors='ignore', axis='columns')

        channel_groups = channel_groups.merge(segments, how='left', on='id', suffixes=('', '_y'))
        channel_groups = channel_groups.merge(data_chunks, how='left', on='segments.id',
                                              suffixes=('', '_y'))

        return channel_groups

    @staticmethod
    def get_diary_fitbit_data(segments: pd.DataFrame) -> pd.DataFrame:
        """
        Get Fitbit data from a patient's diary study. `segments` should be a
        DataFrame (as returned by `get_diary_channel_groups_dataframe()`) that
        includes columns ['dataChunks.url', 'name', 'segments.startTime'].
        Returns a DataFrame with adjusted timestamp, value, and group name.

        Parameters
        ----------
        segments: DataFrame with cols ['dataChunks.url', 'name', 'segments.startTime']
            as returned by `get_diary_channel_groups_dataframe()`
        """
        segment_urls = segments['dataChunks.url']
        group_names = segments['name']
        start_times = segments['segments.startTime']

        data_list = []
        for idx, url in enumerate(segment_urls):
            start_time = datetime.utcfromtimestamp(start_times[idx] / 1000)
            new_data = utils.get_diary_fitbit_data(url)
            # Convert timestamps to true UTC datetime
            new_data['timestamp'] = start_time + pd.to_timedelta(new_data['timestamp'], unit='ms')
            new_data['name'] = group_names[idx]
            data_list.append(new_data)

        if data_list:
            data = pd.concat(data_list)
        else:
            data = None

        return data

    def get_mood_survey_results(self, survey_template_ids: Union[str, Iterable[str]],
                                limit: int = 200, offset: int = 0) -> List[ApiResponse]:
        """
        Gets a list of dictionaries containing mood survey results, including
        keys ['id', 'completer', 'lastSubmittedAt', 'fields']

        Parameters
        ----------
        survey_template_ids: A list of survey_template_ids to retrieve results for
        limit: Batch size for repeated API calls
        offset: Index of the first result to return
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

    def get_mood_survey_results_dataframe(self, survey_template_ids: Union[str, List[str]],
                                          limit: int = 200, offset: int = 0) -> pd.DataFrame:
        """
        Get mood survey results as a DataFrame with cols ['survey.id',
        'survey.lastSubmittedAt', 'surveyField.key', 'surveyField.value']

        Parameters
        ----------
        survey_template_ids : A list of survey_template_ids to retrieve results for
        limit: Batch size for repeated API calls
        offset: Index of the first result to return
        """

        results = self.get_mood_survey_results(survey_template_ids, limit, offset)

        if results is None or len(results) == 0:
            return pd.DataFrame()

        surveys = json_normalize(results)
        fields = self.pandas_flatten(surveys, '', 'fields')
        surveys = surveys.drop('fields', errors='ignore', axis='columns')
        surveys = surveys.merge(fields, how='left', on='id', suffixes=('', '_y'))

        return surveys

    def get_study_ids_in_study_cohort(self, study_cohort_id: str, limit: int = 200,
                                      offset: int = 0) -> List[str]:
        """
        Get the IDs of studies in the given StudyCohort as a list of strings.

        Parameters
        ----------
        study_cohort_id: the id of StudyCohort to retrieve
        limit: Batch size for repeated API calls
        offset: Index of the first result to return
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

    def create_study_cohort(self, name: str, description: str = None, key: str = None,
                            study_ids: List[str] = None) -> str:
        """
        Create a new study cohort and return the associated ID.

        Parameters
        ----------
        name: The name of the study cohort to create
        description: An optional description of the study cohort
        key: An optional key to describe the cohort. Defaults to the ID
        study_ids: A list of study Ids to add to the study cohort
        """
        query_string = graphql.create_study_cohort_mutation_string(name, description, key,
                                                                   study_ids)
        return self.execute_query(query_string)

    def add_studies_to_study_cohort(self, study_cohort_id: str, study_ids: List[str]) -> str:
        """
        Add studies to a study cohort by ID. Return the study cohort ID.

        Parameters
        ----------
        study_cohort_id: The ID of the study cohort to modify
        study_ids: A list of study IDs to add to the study cohort
        """
        query_string = graphql.add_studies_to_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)

    def remove_studies_from_study_cohort(self, study_cohort_id: str, study_ids: List[str]) -> str:
        """
        Remove studies from a study cohort by ID. Return the study cohort ID.

        Parameters
        ----------
        study_cohort_id: The ID of the study cohort to modify
        study_ids: A list of study IDs to remove from the study cohort
        """
        query_string = graphql.remove_studies_from_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)

    def get_user_ids_in_user_cohort(self, user_cohort_id: str, limit: int = 200, offset: int = 0):
        """
        Get the IDs of users in the given UserCohort as a list of strings.

        Parameters
        ----------
        user_cohort_id: the id of UserCohort to retrieve
        limit: Batch size for repeated API calls
        offset: Index of the first result to return
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

    def create_user_cohort(self, name: str, description: str = None, key: str = None,
                           user_ids: List[str] = None) -> str:
        """
        Create a new UserCohort and return the associated ID.

        Parameters
        ----------
        name: The name of the user cohort to create
        description: An optional description of the user cohort
        key: An optional key to describe the cohort. Defaults to the ID
        user_ids: A list of user Ids to add to the user cohort
        """
        query_string = graphql.get_create_user_cohort_mutation_string(name, description, key,
                                                                      user_ids)
        return self.execute_query(query_string)

    def add_users_to_user_cohort(self, user_cohort_id: str, user_ids: List[str]) -> str:
        """
        Add users to a user cohort by ID. Return the user cohort ID.

        Parameters
        ----------
        user_cohort_id: The ID of the user cohort to modify
        user_ids: A list of user IDs to add to the user cohort
        """
        query_string = graphql.get_add_users_to_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)

    def remove_users_from_user_cohort(self, user_cohort_id: str, user_ids: List[str]):
        """
        Remove users from a user cohort by ID. Return the cohort ID.

        Parameters
        ----------
        user_cohort_id: The ID of the user cohort to modify
        user_ids: A list of user IDs to remove from the user cohort
        """
        query_string = graphql.get_remove_users_from_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)
