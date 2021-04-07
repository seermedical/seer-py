"""
Define a client class for interacting with the GraphQL API endpoint.

Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.

Concepts
--------
- study: A defined period of time monitoring a patient, typically with EEG-ECG-video.
    A patient may have multiple studies, and a given study may or may not be
    attached to a patient.
- diary: Use of the Seer app by a patient to record events such as seizures
    ("labels") and "alerts" for medication use
- diary study: A patient study which is not time-bound. May include data from
    devices such as smart phones or watches. A diary study must be attached to
    a patient, and there can only be one diary study per patient.
- channel group: A mode of monitoring data, dependent on the study type.
    Study: EEG, ECG, video
    Diary study: Wearable data, e.g. heart rate, step count
- channel: A channel group may have multiple channels. E.g. The different
    electrodes for EEG: Fz, C4, Fp1 etc.
- label group: Categories of labels relevant to a study. Depends on the study type.
    Study: clinical annotations, e.g. Abnormal / Epileptiform, Normal / Routine
    Diary: self-reported annotations of events, e.g. Seizure / Other
    Diary study: labels from a wearable device, e.g. Sleep annotations
- label: Belongs to a label group. Labels typically involve the following fields:
    id, startTime, duration, timezone, note, tags, confidence, createdAt, createdBy
- tag: An ontology of "attributes" that may be atached to a label to provide
    info or clarifications, e.g. Jaw clenching, Beta, Exemplar, Generalised, Sleep.
    Tags are arranged into categories, e.g. Band, Brain area, Channel, Seizure type, Sleep
- segment: A duration of recording for a given channel group. Segments lengths
    are variable, though generally capped at 135 minutes (at least for EEG)
- data chunk: Segments are saved to disk as 10-second data chunks, which must be
    reassembled to yield a complete segment
- party ID: The ID associated with e.g. an organisation, which will filter the
    values returned
- API response: Data returned from the GraphQL endpoint, as a dictionary with
    string-type keys, and values that may be strings, numbers, bools, dictionaries,
    lists of dicts etc.
"""
from datetime import datetime
import math
import time
import json
from copy import deepcopy

from gql import gql, Client as GQLClient
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from pandas.io.json import json_normalize
import requests

from . import auth
from . import utils
from . import graphql


class SeerConnect:  # pylint: disable=too-many-public-methods
    graphql_client = None

    def __init__(self, api_url=None, email=None, password=None, api_key_id=None, api_key_path=None,
                 seer_auth=None, use_email=None, region='au'):
        """
        Creates a GraphQL client able to interact with the Seer database, handling login and
        authorisation

        Parameters
        ----------
        api_url : str, optional
            Base URL of API endpoint
        email : str, optional
            The email address for a user's Seer account
        password : str, optional
            User password associated with Seer account
        api_key_id : str, optional
            The UUID for a Seer api key
        api_key_file : str, optional
            The path to a Seer api key file
        """

        self.seer_auth = auth.get_auth(api_key_id, api_key_path, region, api_url, seer_auth,
                                       use_email, email, password)

        self.create_client()

        self.last_query_time = time.time()
        self.api_limit_expire = 300
        self.api_limit = 580

    def create_client(self):
        """
        Create a GraphQL client with parameters from the current SeerAuth object.
        """
        def graphql_client(party_id=None):
            connection_params = self.seer_auth.get_connection_parameters(party_id)
            return GQLClient(transport=RequestsHTTPTransport(**connection_params))

        self.graphql_client = graphql_client
        self.last_query_time = time.time()

    def execute_query(self, query_string, party_id=None, invocations=0, variable_values=None):
        """
        Execute a GraphQL query and return response. Handle retrying upon
        failure and rate limiting requests.

        Parameters
        ----------
        query_string: str
            The GraphQL query
        party_id : str, optional
            The organisation/entity to specify for the query
        invocations : int, optional
            Used for recursive calls; don't set directly
        variable_values : dict, optional
            Values for GraphQL to substitute into the query

        Returns
        -------
        graphql_results : dict
            Query results as a dictionary matching the structure of the query

        Notes
        -----
        See queries in graphql.py for structure of results returned
        """
        resolvable_api_errors = [
            '502 Server Error', '503 Server Error', '504 Server Error', 'Read timed out.',
            'NOT_AUTHENTICATED'
        ]

        try:
            time.sleep(
                max(0, ((self.api_limit_expire / self.api_limit) -
                        (time.time() - self.last_query_time))))
            response = self.graphql_client(party_id).execute(gql(query_string),
                                                             variable_values=variable_values)
            self.last_query_time = time.time()
            return response
        except Exception as ex:
            if invocations > 4:
                print('Too many failed query invocations. raising error')
                raise
            error_string = str(ex)
            if any(api_error in error_string for api_error in resolvable_api_errors):
                if self.seer_auth.handle_query_error_pre_sleep(ex):
                    print('"', error_string, '" raised, trying again after a short break')
                    time.sleep(
                        min(30 * (invocations + 1)**2,
                            max(self.last_query_time + self.api_limit_expire - time.time(), 0)))

                invocations += 1

                self.seer_auth.handle_query_error_post_sleep(error_string)

                return self.execute_query(query_string, party_id, invocations=invocations,
                                          variable_values=variable_values)

            raise

    def get_paginated_response(self, query_string, variable_values, limit, object_path,
                               iteration_path=None, party_id=None, max_items=None):
        """
        For queries expecting a large number of objects returned, split query into iterative calls
        to `execute_query()`.
        The object_path parameter controls which part of the query response is returned, and the
        iteration_path parameter indicates where the response can vary for each iteration.

        Parameters
        ----------
        query_string : str
            The GraphQL query
        variable_values : dict, optional
            Values for GraphQL to substitute into the query
        limit : int
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        object_path : list of str
            One or more levels of key giving the path to the object to be returned
            e.g. ['userCohort', 'users'] for a query response of
            {"userCohort": {"users": [{"id": "user1"}, {"id": "user2"}]}}
            would give [{"id": "user1"}, {"id": "user2"}]
        iteration_path : list of str, optional
            None (default), one, or more levels of key giving the path to the
            node (relative to the `object_path` level) where the response can
            vary with each query iteration. If None then the response varies at
            the path given by object_path.

            Eg: We may be interested in the surveys for a user. The heirarchy
            will look something like ["user", "surveys"]. We are interested in
            keeping the user information, although this doesnt change with each
            iteration, so we set `object_path=["user"]`. But we actually want
            to iterate through the survey  information, which does change with
            each iteration, so we set `iteration_path=["surveys"]`
        party_id : str, optional
            The organisation/entity to specify for the query
        max_items: int, optional
            max number of items to return. In the case of queries containing
            nested lists of items it only limits the number of items specified
            on the `iteration_path` level of the heirarchy (this defaults to
            `object_path` if no argument is passed to `iteration_path` )

            eg: with `max_items=2, object_path=["users"]`, querying a list of
            users, and for each user, the list of surveys, then it limits the
            number of users to 2, irrespective of number of surveys per user).

            eg: with `max_items=10, object_path=["user"], iteration_path=["user", "surveys"]`
            querying a single user, and the list of surveys for this user, then
            it limits the number surveys to 10 for this user, while still
            returning the information at the `user` level.

        Returns
        -------
        responses: list of dict
            List of query result dictionaries
        """
        variable_values = deepcopy(variable_values) # prevent local changes affecting external one
        variable_values['limit'] = limit
        offset = variable_values.get('offset', 0) # Try get offset from variable values if it exists
        result = []
        total_items_returned = 0
        remaining_items = 0

        while True:
            # Update the number of items remaining
            # And set the limit for the final batch if needed
            if max_items is not None:
                remaining_items = max_items - total_items_returned
                limit = min(remaining_items, limit)
                variable_values['limit'] = limit
                if remaining_items <= 0:
                    break

            variable_values['offset'] = offset  # update pagination location
            response = self.execute_query(query_string, variable_values=variable_values,
                                          party_id=party_id)

            # select the part of the response we are interested in
            response = utils.get_nested_dict_item(response, *object_path)


            # select the part of the response which can vary. if iteration_path is None this will be
            # the same as the part of the response we are interested in
            response_increment = response
            if iteration_path:
                response_increment = utils.get_nested_dict_item(response_increment, *iteration_path)
            if not response_increment:
                # if the part of the response which varies is empty, we are finished iterating
                break

            # Update the number of items received
            total_items_returned += len(response)
            # print(f"items: {len(response)}  limit: {limit}  fetched: {total_items_returned} remain: {remaining_items}") # for debuging purposes

            if not result:
                # if this is the first response, save it
                result = response
            else:
                # otherwise add the response increment to the existing result at the correct level
                values_container = result
                if iteration_path:
                    values_container = utils.get_nested_dict_item(values_container, *iteration_path)
                values_container.extend(response_increment)

            offset += limit

        return result

    @staticmethod  # maybe this could move to a utility class
    def pandas_flatten(parent, parent_name, child_name):
        """
        Take a DataFrame with at least 2 columns:
        - A column named like f'{parent_name}id'
        - A `child_name` column, where each cell is a list of dicts.

        Return a new DataFrame that retains the ID column and creates new
        columns from the dictionary keys.

        Parameters
        ----------
        parent : pd.DataFrame
            A DataFrame with f'{parent_name}id' and `child_name` columns
        parent_name : str
            Any prefix to the 'id' and `child_name` columns in the parent DataFrame
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
            parent_id = parent[parent_name + 'id'][i]
            cell_to_flatten = parent[parent_name + child_name][i]
            if isinstance(cell_to_flatten, list):
                child = json_normalize(cell_to_flatten).sort_index(axis=1)
                child.columns = [child_name + '.' + str(col) for col in child.columns]
                child[parent_name + 'id'] = parent_id
                child_list.append(child)

        if child_list:
            child = pd.concat(child_list, sort=True).reset_index(drop=True)
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
            A unique ID identifying a study
        name : str
            Name of the new label group
        description : str
            Free text description of the label group
        label_type : str, optional
            Label type ID
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
            Label group ID to delete

        Returns
        -------
        label_group_id : str
            ID of the deleted label group
        """
        query_string = graphql.get_remove_label_group_mutation_string(group_id)
        return self.execute_query(query_string)

    def edit_study_label_group(self, group_id, name, description):
        """
        Edit a study label group.

        Parameters
        ----------
        group_id : str
            Label group ID to edit
        name : str
            Name of the label group
        description : str
            Free text description of the label group

        Returns
        -------
        label_group_id : str
            ID of the edited label group
        """
        variable_values = {"group_id": group_id, "name": name, "description": description}
        return self.execute_query(graphql.EDIT_STUDY_LABEL_GROUP, variable_values=variable_values)

    def add_labels_batched(self, label_group_id, labels, batch_size=500):
        """
        Add labels to label group in batches.

        Parameters
        ----------
        label_group_id : str
            Label group ID
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
            Label group ID
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
            A dict with a single key, 'addLabelsToLabelGroup', that maps to a
            list of dicts, each with an 'id' key indicating an added label
        """
        if isinstance(labels, pd.DataFrame):
            labels = labels.to_dict('records')
        variable_values = {'group_id': group_id, 'labels': labels}
        return self.execute_query(graphql.ADD_LABELS, variable_values=variable_values)

    def add_document(self, study_id, document_name, document_path):
        """
        Upload a document and associate it with a study.

        Parameters
        ----------
        study_id : str
            A unique ID identifying a study
        document_name : str
            Name to assign document after upload
        document_path : str
            Path to document

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
            query_string = graphql.get_confirm_document_mutation_string(
                study_id, response_add['id'])
            response_confirm = self.execute_query(query_string)
            return response_confirm['confirmStudyDocuments'][0]['downloadFileUrl']
        raise RuntimeError('Error uploading document: status code ' + str(response_put.status_code))

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
        response = self.execute_query(graphql.GET_TAG_IDS)
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
            Batch size for repeated API calls
        search_term : str, optional
            Filter results to studies that match this string on their study name,
            study description, study code, and/or patient name
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_ids : list of str
            Unique IDs, each identifying a study
        """
        studies = self.get_studies(limit, search_term, party_id)
        return [study['id'] for study in studies]

    def get_studies(self, limit=50, search_term='', party_id=None, max_items=None):
        """
        Get a list of study dicts, with each having keys: 'id', 'name' and 'patient'.

        Parameters
        ----------
        limit : int
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        search_term : str
            Filter results to studies including this string, either in the study
            name or patient name. Not case sensitive.
        party_id : str
            The organisation/entity to specify for the query
        max_items: int, optional
            max number of studies to return.

        Returns
        -------
        studies : list of dict
            Study details, each having keys:
            - id
            - name
            - patient
        """
        variable_values = {'search_term': search_term}
        return self.get_paginated_response(graphql.GET_STUDIES_BY_SEARCH_TERM_PAGED,
                                           variable_values=variable_values,
                                           limit=limit,
                                           object_path=['studies'],
                                           party_id=party_id,
                                           max_items=max_items
                                           )

    def get_studies_dataframe(self, limit=50, search_term='', party_id=None, max_items=None):
        """
        Get details of study IDs, names and patient info as a DataFrame. See
        `get_studies()` for details.

        Parameters
        ----------
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        search_term : str, optional
            A string used to filter the studies returned
        party_id : str, optional
            The organisation/entity to specify for the query
        max_items: int, optional
            max number of studies to return.

        Returns
        -------
        study_df: pd.DataFrame
            DataFrame with details of all matching studies
        """
        studies = self.get_studies(limit=limit, search_term=search_term, party_id=party_id,
            max_items=max_items)
        studies_dataframe = json_normalize(studies).sort_index(axis=1)
        return studies_dataframe.drop('patient', errors='ignore', axis='columns')

    def get_study_ids_from_names_dataframe(self, study_names, party_id=None):
        """
        Get the IDs of all available studies as a DataFrame. See `get_studies()`
        for details.

        Parameters
        ----------
        study_names : str or list of str
            Study names to retrieve
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
            Study name or names to look up
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        study_ids: list of str
            Unique IDs, each identifying a study
        """
        return self.get_study_ids_from_names_dataframe(study_names, party_id)['id'].tolist()

    def get_studies_by_id(self, study_ids, limit=50):
        """
        Get a dict of study details for each study ID provided.

        Parameters
        ----------
        study_ids : str or list of str
            One or more unique IDs, each identifying a study
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        study_dicts: list of dict
            Details for each study (name, ID etc)
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]
        variable_values = {'study_ids': study_ids}
        return self.get_paginated_response(graphql.GET_STUDIES_BY_STUDY_ID_PAGED, variable_values,
                                           limit, ['studies'])

    def get_channel_groups(self, study_id):
        """
        Get details of each channel group for a given study.

        Parameters
        ----------
        study_id : str
            A unique ID identifying a study

        Returns
        -------
        channel_groups : list of dict
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
            segments.extend([
                segment for segment in response['studyChannelGroupSegments'] if segment is not None
            ])
            counter += 1
        segment_urls = pd.DataFrame(segments)
        segment_urls = segment_urls.rename(columns={'id': 'segments.id'})
        return segment_urls

    def get_data_chunk_urls(self, study_metadata, s3_urls=True, from_time=0, to_time=9e12,
                            limit=10000):
        """
        Get a DataFrame containing download details of all data chunks that comprise the segments in
        a provided metadata DataFrame.

        Parameters
        ----------
        study_metadata : pd.DataFrame
            Study metadata as returned by `get_all_study_metadata_dataframe_by_*()`
        s3_urls : bool, optional
            If True (default), return download URLs for S3 (otherwise return URLs for Cloudfront).
            S3 should be preferred when running in the same AWS region as the data is stored.
        from_time : float, optional
            Timestamp in msec - only retrieve data from this point onward
        to_time : float, optional
            Timestamp in msec - only retrieve data up until this point
        limit : int, options
            Batch size for repeated API calls

        Returns
        -------
        data_chunk_df : pd.DataFrame
            The returned DataFrame has columns:
            - segments.id
            - dataChunks.index
            - dataChunks.time
            - dataChunks.end
            - dataChunks.url
        """
        columns = [
            'segments.id', 'dataChunks.index', 'dataChunks.time', 'dataChunks.end', 'dataChunks.url'
        ]
        if study_metadata.empty:
            return pd.DataFrame(columns=columns)

        study_metadata = study_metadata.drop_duplicates('segments.id')
        study_metadata = study_metadata[study_metadata['segments.startTime'] <= to_time]
        study_metadata = study_metadata[study_metadata['segments.startTime']
                                        + study_metadata['segments.duration'] >= from_time]

        data_chunks = []
        chunk_metadata = []
        for row in zip(study_metadata['channelGroups.chunkPeriod'],
                       study_metadata['segments.duration'], study_metadata['segments.startTime'],
                       study_metadata['segments.id']):
            chunk_period = row[0] * 1000.  # chunk period is in seconds, time is milliseconds
            num_chunks = int(math.ceil(row[1] / chunk_period))
            for i in range(num_chunks):
                chunk_start = row[2] + chunk_period * i
                chunk_end = chunk_start + chunk_period
                if chunk_end >= from_time and chunk_start <= to_time:
                    data_chunks.append({'segmentId': row[3], 'chunkIndex': i})
                    chunk_metadata.append({
                        'segments.id': row[3],
                        'dataChunks.index': i,
                        'dataChunks.time': chunk_start,
                        'dataChunks.end': chunk_end
                    })
        if not data_chunks:
            return pd.DataFrame(columns=columns)

        chunks = []
        counter = 0
        while int(counter * limit) < len(data_chunks):
            data_chunks_batch = data_chunks[int(counter * limit):int((counter + 1) * limit)]
            query_string = graphql.get_data_chunk_urls_query_string(data_chunks_batch, s3_urls)
            response = self.execute_query(query_string)
            chunks.extend(response['studyChannelGroupDataChunkUrls'])
            counter += 1
        data_chunk_urls = pd.DataFrame(chunk_metadata)
        data_chunk_urls['dataChunks.url'] = chunks

        # If there is an error in uploading a segment to the platform or in processing, you can get
        # a situation where the metadata for a segment exists but the data is missing, in which case
        # there will be no data chunk url.
        data_chunk_urls = data_chunk_urls[~data_chunk_urls['dataChunks.url'].isnull()]

        return data_chunk_urls

    # pylint:disable=too-many-arguments
    def get_labels(self, study_id, label_group_id, from_time=0, to_time=9e12, limit=200, offset=0,
        max_items=None):
        """
        Get labels for a given study and label group.

        Parameters
        ----------
        study_id : str
            A unique ID identifying a study
        label_group_id : str
            Label group ID
        from_time : int, optional
            Timestamp in msec - only retrieve data from this point onward
        to_time : int, optional
            Timestamp in msec - only retrieve data up until this point
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Index of first label to retrieve
        max_items: int, optional
            max number of labels to return.

        Returns
        -------
        labels : dict
            Has a 'labelGroup' key which indexes to a nested dict with a 'labels' key
        """
        variable_values = {
            'study_id': study_id,
            'label_group_id': label_group_id,
            'from_time': from_time,
            'to_time': to_time
        }
        return self.get_paginated_response(graphql.GET_LABELS_PAGED,
                                           variable_values=variable_values,
                                           limit=limit,
                                           object_path=['labelGroup', 'labels'],
                                           max_items=max_items
                                           )


    # pylint:disable=too-many-arguments
    def get_labels_dataframe(self, study_id, label_group_id, from_time=0, to_time=9e12, limit=200,
                             offset=0, max_items=None):
        """
        Get all labels for a given study and label group as a DataFrame.
        See `get_labels()` for details.

        Returns
        -------
        labels_df : pd.DataFrame
            Details of all matching labels
        """
        label_results = self.get_labels(study_id, label_group_id, from_time, to_time, limit, offset,
            max_items=max_items)
        if not label_results:
            return pd.DataFrame()
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
        representation. Because the GraphQL response is unvalidated, it can
        perform significantly faster for larger datasets.

        Parameters
        ----------
        study_id : str
            A unique ID identifying a study
        label_group_id : str
            Label group ID
        from_time : int, optional
            Timestamp in msec - only retrieve data from this point onward
        to_time : int, optional
            Timestamp in msec - only retrieve data up until this point

        Returns
        -------
        labels_str : dict
            Has a key 'labelString' which indexes a JSON-like string with only
            3 keys per label: 'id', 's' (for startTime), and 'd' (for duration)
        """
        variable_values = {
            'study_id': study_id,
            'label_group_id': label_group_id,
            'from_time': from_time,
            'to_time': to_time
        }
        response = self.execute_query(graphql.GET_LABELS_STRING, variable_values=variable_values)
        return response['study']

    # pylint:disable=too-many-arguments
    def get_labels_string_dataframe(self, study_id, label_group_id, from_time=0, to_time=9e12):
        """
        Get all labels for a given study and label group in an abridged string
        representation, as a DataFrame. See `get_labels_string()` for details.

        Returns
        -------
        labels_str_df : pd.DataFrame
            Columns include 'labels.id', 'labels.startTime' and 'labels.duration'
        """
        label_results = self.get_labels_string(study_id, label_group_id, from_time=from_time,
                                               to_time=to_time)
        if label_results is None:
            return label_results
        label_group = json_normalize(label_results).sort_index(axis=1)
        label_group['labelGroup.labelString'] = (label_group['labelGroup.labelString'].apply(
            json.loads))
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

    def get_label_groups_for_studies(self, study_ids, limit=50):
        """
        Get label group information for all provided study IDs.

        Parameters
        ----------
        study_ids : str or list of str
            One or more unique IDs, each identifying a study
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        label_groups : list of dict
            Keys included: 'id', 'labelGroups' and 'name'
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]

        variable_values = {'study_ids': study_ids}
        return self.get_paginated_response(graphql.GET_LABEL_GROUPS_FOR_STUDY_IDS_PAGED,
                                           variable_values, limit, ['studies'])

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
        # TODO: can we use json_normalize or pandas_flatten for this?
        label_groups = []
        for study in self.get_label_groups_for_studies(study_ids, limit):
            for label_group in study['labelGroups']:
                label_group['labelGroup.id'] = label_group.pop('id')
                label_group['labelGroup.name'] = label_group.pop('name')
                label_group['labelGroup.description'] = label_group.pop('description')
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
            A unique ID identifying a study
        limit : int, optional
            Batch size for repeated API calls
        offset : int, optional
            Index of first record to return

        Returns
        -------
        times_df : pd.DataFrame
            Includes columns 'id', 'startTime', 'duration' and 'user'
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
            views = pd.DataFrame(columns=['user', 'id', 'startTime', 'duration', 'createdAt',
                                          'updatedAt'])
        return views

    def get_organisations(self):
        """
        Get details of all available organisations.

        Returns
        -------
        organisations : list of dict
            Dictionaries with organisation 'id' and 'name' keys
        """
        response = self.execute_query(graphql.GET_ORGANISATIONS)
        return response['organisations']

    def get_organisations_dataframe(self):
        """
        Get details of all available organisations as a DataFrame.

        Returns
        -------
        orgs_df : pd.DataFrame
            Organisations DataFrame with 'id' and 'name' columns
        """
        orgs = self.get_organisations()
        if orgs is None:
            return orgs
        return pd.DataFrame(orgs)

    def get_user_from_patient(self, patient_id):
        """
        Get user ID and info from patient ID.
        Parameters
        ----------
        patient_id : str
            The patient ID
        Returns
        -------
        patient : dict
            Patient details, with keys 'id' and 'user'
        """
        query_string = graphql.get_user_from_patient_query_string(patient_id)
        response = self.execute_query(query_string)
        return response['patient']

    def get_user_from_patient_dataframe(self, patient_id):
        """
        Get user ID and info from patient ID.
        Parameters
        ----------
        patient_id : str
            The patient ID
        Returns
        -------
        patient : pd.DataFrame
            Patient details as pandas DataFrame
        """
        patient = self.get_user_from_patient(patient_id)
        if not patient:
            return pd.DataFrame()
        return json_normalize(patient).sort_index(axis=1)

    def get_patients(self, party_id=None, limit=50, max_items=None):
        """
        Get available patient IDs and user names.

        Parameters
        ----------
        party_id : str, optional
            The organisation/entity to specify for the query
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        max_items: int, optional
            max number of patients to return

        Returns
        -------
        patients : list of dict
            Patient details, with keys 'id' and 'user'
        """
        response = self.get_paginated_response(
                                    graphql.GET_PATIENTS_PAGED,
                                    variable_values=dict(),
                                    limit=limit,
                                    object_path=["patients"],
                                    party_id=party_id,
                                    max_items=max_items
                                    )
        return response

    def get_patients_dataframe(self, party_id=None, limit=50, max_items=None):
        """
        Get available patient IDs and user names as a DataFrame.

        Parameters
        ----------
        party_id : str, optional
            The organisation/entity to specify for the query
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        max_items: int, optional
            max number of patients to return

        Returns
        -------
        patient_df : pd.DataFrame
            Patient details, with columns 'id' and 'user'
        """
        patients = self.get_patients(party_id, limit=limit, max_items=max_items)
        if patients is None:
            return pd.DataFrame()
        return json_normalize(patients).sort_index(axis=1)

    def get_documents_for_studies(self, study_ids, limit=50):
        """
        Get details of all documents associated with given study ID(s).

        Parameters
        ----------
        study_id : str or list of str
            One or more unique IDs, each identifying a study
        limit : int, optional
            Batch size for repeated API calls

        Returns
        -------
        documents : list of dict
            Document details. Dict has key 'documents' that indexes a nested dict
            with keys including: 'id', 'name', 'fileSize', 'downloadFileUrl'
        """
        if isinstance(study_ids, str):
            study_ids = [study_ids]

        variable_values = {'study_ids': study_ids}
        return self.get_paginated_response(graphql.GET_DOCUMENTS_FOR_STUDY_IDS_PAGED,
                                           variable_values, limit, ['studies'])

    def get_documents_for_studies_dataframe(self, study_ids, limit=50):
        """
        Get details of all documents associated with given study ID(s) as a DataFrame.
        See `get_documents_for_studies()` for details.

        Returns
        -------
        documents_df : pd.DataFrame
            DataFrame with document details for a study
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

    def get_diary_insights(self, patient_id, limit=50, offset=0, max_items=None):
        """
        Retrieve patient insights JSON report.

        Parameters
        ----------
        patient_id : str
            The patient ID
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Optional index of first record to return
        max_items: int, optional
            max number of diary insights to return.

        Returns
        -------
        result : dict
            Returns a dictionary patient insights.
        """
        variable_values = {'patient_id': patient_id}
        return self.get_paginated_response(graphql.GET_DIARY_INSIGHTS_PAGED,
                                   variable_values=variable_values,
                                   limit=limit,
                                   object_path=['patient'],
                                   iteration_path=['insights'],
                                   max_items=max_items
                                   )

    def get_diary_created_at(self, patient_id):
        query_string = graphql.get_diary_created_at_query_string(patient_id)
        response = self.execute_query(query_string)
        return response['patient']['diary']['createdAt']

    def get_diary_labels(self, patient_id, label_type='all', offset=0, limit=100, from_time=0,
                         to_time=9e12, from_duration=0, to_duration=9e12):
        """
        Retrieve diary label groups and labels for a given patient.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary labels
        label_type : str, optional
            The type of label to retrieve. Default = 'all'. Options = 'seizure',
            'medications', 'cardiac'.
        offset : int, optional
            Index of first record to return
        limit : int, optional
            Batch size for repeated API calls
        from_time : int, optional
            Unix timestamp (in milliseconds) to apply a range filter on label start times.
            Retrieves labels from the given time onward
        to_time : int, optional
            Unix timestamp (in milliseconds) to apply a range filter label start times.
            Retrieves labels up until the given time
        from_duration : int, optional
            Time in millseconds to apply a range filter on the duration of labels.
            Retrieves labels of duration > from_duration
        to_duration : int, optional
            Time in milliseconds to apply a range filter on the duration of labels.
            Retrieves labels of duration < to_duration

        Returns
        -------
        label_results : dict
            Returns a dictionary with a 'labelGroups' key that indexes to a list
            of dictionaries including keys 'labelType', 'name', 'labels', 'numberOfLabels' etc.
        """
        # TODO: perhaps migrate to use `get_paginated_response()`
        label_results = {}
        # set true if we need to fetch labels
        query_flag = True
        variable_values = {
            'patient_id': patient_id,
            'value': label_type,
            'from_time': from_time,
            'to_time': to_time,
            'from_duration': from_duration,
            'to_duration': to_duration
        }
        while True:
            if not query_flag:
                break

            variable_values['limit'] = limit
            variable_values['offset'] = offset

            response = self.execute_query(graphql.GET_DIARY_LABELS,
                                          variable_values=variable_values)['patient']['diary']
            label_groups = response['labelGroups']

            query_flag = False
            for idx, group in enumerate(label_groups):
                labels = group['labels']

                if not labels:
                    continue

                # we need to fetch more labels
                if len(labels) >= limit:
                    query_flag = True

                if not label_results:
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

    def get_diary_labels_dataframe(self, patient_id, label_type='all', offset=0, limit=100,
                                   from_time=0, to_time=9e12, from_duration=0, to_duration=9e12):
        """
        Get all diary label groups and labels for a given patient as a DataFrame.
        See `get_diary_labels()` for details.

        Returns
        -------
        diary_labels_df : pd.DataFrame
            DataFrame with diary label information
        """
        label_results = self.get_diary_labels(patient_id, label_type, offset, limit, from_time,
                                              to_time, from_duration, to_duration)
        if not label_results:
            return pd.DataFrame()

        label_groups = json_normalize(label_results['labelGroups']).sort_index(axis=1)
        labels = self.pandas_flatten(label_groups, '', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_groups = label_groups.drop('labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')
        label_groups = label_groups.merge(labels, how='left', on='id', suffixes=('', '_y'))
        label_groups = label_groups.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))
        label_groups = label_groups.rename({'id': 'labelGroups.id'})
        label_groups['id'] = patient_id
        label_groups['createdAt'] = label_results['createdAt']
        return label_groups

    def get_diary_medication_alerts(self, patient_id, from_time=0, to_time=9e12):
        """
        Get diary medications ("alerts") for a given patient.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary medications
        from_time : int, optional
            Timestamp in msec - only retrieve alert labels with a startTime
            from this point onward
        to_time : int, optional
            Timestamp in msec - only retrieve alert labels with a startTime
            up until this point

        Returns
        -------
        medications : dict
            Medication information with key 'alerts', which indexes to a dictionary
             with a 'labels' key that indexes list of dict with keys 'doses',
             'alert', 'startTime', 'scheduledTime' etc.
        """
        variable_values = {'patient_id': patient_id, 'from_time': from_time, 'to_time': to_time}
        response = self.execute_query(graphql.GET_DIARY_MEDICATION_ALERTS,
                                      variable_values=variable_values)
        return response['patient']['diary']

    def get_diary_medication_alerts_dataframe(self, patient_id, from_time=0, to_time=9e12):
        """
        Get diary medication alerts for a given patient as a DataFrame. See
        `get_diary_medication_alerts()` for details.

        Returns
        -------
        medications_df : pd.DataFrame
            DataFrame with details of patient medication information
        """
        results = self.get_diary_medication_alerts(patient_id, from_time, to_time)
        if results is None:
            return results
        alerts = json_normalize(results['alerts']).sort_index(axis=1)
        labels = self.pandas_flatten(alerts, '', 'labels')
        return labels

    def get_diary_medication_alert_windows(self, patient_id, is_active=None):
        """
        Gets all medication alert windows for a given patient.
        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve medication compliance
        is_active : string, optional
            Filters alert windows to active, not active or both. Options: True,
            False, None. Default is None
        Returns
        -------
        medication_alert_windows : dict
            Medication information with key 'alerts', which indexes to a dictionary
            with a 'windows' key that indexes list of dict with keys 'startTime',
            'timezone', and 'endTime'.
        """
        filters = [{
            'name': 'isActive',
            'value': f'{str(is_active).lower()}'
        }] if is_active is not None else []
        variable_values = {'patient_id': patient_id, 'filters': filters}
        response = self.execute_query(graphql.GET_DIARY_MEDICATION_ALERT_WINDOWS,
                                      variable_values=variable_values)
        return response['patient']['diary']['alerts']

    def get_diary_medication_compliance(self, patient_id, from_time=0, to_time=0,
                                        timezone_string=None):
        """
        Gets all medication compliance records for a given patient.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve medication compliance
        from_time : int, optional
            Timestamp in msec - only retrieve data from this point onward
        to_time : int, optional
            Timestamp in msec - only retrieve data up until this point. The default value of 0 means
            up until this point in time for this query
        timezone: string, optional
            The timezone name to retrieve medication compliance for,
            (e.g. "Australia/Melbourne")

        Returns
        -------
        medication_compliance : dict
            Has a single key, 'patient', which indexes a nested dictionary with a
            'diary' key, which indexes a dictionary with a 'medicationCompliance' key.
        """
        variable_values = {'patient_id': patient_id, 'from_time': from_time, 'to_time': to_time}

        if timezone_string is not None:
            variable_values['timezone'] = timezone_string

        return self.execute_query(graphql.GET_DIARY_MEDICATION_COMPLIANCE,
                                  variable_values=variable_values)

    def get_diary_medication_compliance_dataframe(self, patient_id, from_time=0, to_time=0):
        """
        Get all medication compliance records for a given patient as a DataFrame.
        See `get_diary_medication_compliance()` for details.

        Returns
        -------
        medication_compliance_df : pd.DataFrame
            Dataframe with columns about medication compliance
        """
        results = self.get_diary_medication_compliance(patient_id, from_time, to_time)
        if results is None:
            return results

        medication_compliance = json_normalize(
            results['patient']['diary']['medicationCompliance']).sort_index(axis=1)
        medication_compliance['id'] = patient_id
        return medication_compliance

    def get_all_study_metadata_by_names(self, study_names=None, party_id=None):
        """
        Get all metadata available about provided study names. See
        `get_all_study_metadata_by_ids()` for details.

        Parameters
        ----------
        study_names : str or list of str, optional
            Study names. If not provided, data will be returned for all studies
        party_id : str, optional
            The organisation/entity to specify for the query

        Returns
        -------
        metadata : dict
            Nested dictionaries with information on patient, channel groups,
            channels and segments
        """
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names, party_id)
        return self.get_all_study_metadata_by_ids(study_ids)

    def get_all_study_metadata_by_ids(self, study_ids=None):
        """
        Get all metadata available about studies with supplied IDs.

        Parameters
        ----------
        study_ids : list of str, optional
            Unique IDs, each identifying a study. If not provided, data will be
            returned for all available studies.

        Returns
        -------
        metadata : dict
            A dictionary with a single key 'studies', which indexes a list of
            dictionaries with keys 'id', 'name', 'description', 'patient' and
            'channelGroups'. 'channelGroup' indexes a dictionary with keys
            'channels', 'segments', 'sampleRate' etc.
        """
        if study_ids is None:
            study_ids = self.get_study_ids()
        elif not study_ids:  # treat empty list as asking for nothing, not everything
            return {'studies': []}

        result = [
            self.execute_query(graphql.GET_STUDY_WITH_DATA,
                               variable_values={'study_id': study_id})['study']
            for study_id in study_ids
        ]

        return {'studies': result}

    def get_all_study_metadata_dataframe_by_names(self, study_names=None):
        """
        Get all metadata available about studies with the suppled names as a
        DataFrame. See `get_all_study_metadata_by_ids()` for details.

        Parameters
        ----------
        study_names : str or list of str, optional
            Study names. If not provided, data will be returned for all studies

        Returns
        -------
        metadata_df : pd.DataFrame
            DataFrame with information on patient, channel groups, channels and segments
        """
        study_ids = None
        if study_names:
            study_ids = self.get_study_ids_from_names(study_names)
        return self.get_all_study_metadata_dataframe_by_ids(study_ids)

    def get_all_study_metadata_dataframe_by_ids(self, study_ids=None):
        """
        Get all metadata available about studies with the suppled IDs as a
        DataFrame. See `get_all_study_metadata_by_ids()` for more details.

        Parameters
        ----------
        study_ids : list of str
            Unique IDs, each identifying a study

        Returns
        -------
        metadata_df : pd.DataFrame
            DataFrame with information on patient, channel groups, channels and segments
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

    # pylint:disable=too-many-locals,too-many-arguments
    def get_channel_data(self, all_data, segment_urls=None, download_function=requests.get,
                         threads=None, from_time=0, to_time=9e12, s3_urls=True):
        """
        Download raw data for all channel groups and segments listed in a given metadata DataFrame
        and return as a new DataFrame.

        Parameters
        ----------
        all_data : pd.DataFrame
            Study metadata, as returned by `get_all_study_metadata_dataframe_by_*()`
        segment_urls : pd.DataFrame, optional
            DataFrame with columns ['segments.id', 'baseDataChunkUrl'] as returned by
            `get_segment_urls`, or with columns ['segments.id', 'dataChunks.time', 'dataChunks.url']
            as returned by `get_data_chunk_urls`. If None, these will be retrieved for each segment
            in `all_data`.
        download_function : callable, optional
            The function used to download the channel data. Defaults to requests.get
        threads : int, optional
            Number of threads to use. If > 1 will use multiprocessing. If None (default), will use
            1 on Windows and 5 on Linux/MacOS.
        from_time : float, optional
            Timestamp in msec - only retrieve data from this point onward
        to_time : float, optional
            Timestamp in msec - only retrieve data up until this point
        s3_urls : bool, optional
            Note: this only has an effect if segment_urls is None. If True (default), download
            using S3 urls (otherwise download via Cloudfront). S3 should be preferred when running
            in the same AWS region as the data is stored.

        Returns
        -------
        data_df : pd.DataFrame
            DataFrame with 'time', 'id', 'channelGroups.id' and 'segments.id' columns, as well as a
            column for each data channel, e.g. each EEG electrode.

        Example
        -------
        Get all ECG data for a study of patient "Jane Doe":
        >>> study_id = get_study_ids(search_term='Jane Doe')[0]['id']
        >>> metadata_df = get_all_study_metadata_dataframe_by_ids(study_id)
        >>> ecg_metadata_df = metadata_df[metadata_df['channelGroups.name'] == 'ECG']
        >>> ecg_data_df = get_channel_data(ecg_metadata_df)
        """
        if segment_urls is None:
            segment_urls = self.get_data_chunk_urls(all_data, s3_urls=s3_urls, from_time=from_time,
                                                    to_time=to_time)

        return utils.get_channel_data(all_data, segment_urls, download_function, threads, from_time,
                                      to_time)

    def get_all_bookings(self, organisation_id, start_time, end_time, include_cancelled=False):
        """
        Get all bookings for any studies that are active at any point between
        `start_time` and `end_time`.

        Parameters
        ----------
        organisation_id : str
            Organisation ID associated with patient bookings
        start_time : int
            Timestamp in msec - find studies active after this point
        end_time : int
            Timestamp in msec - find studies active before this point
        include_cancelled: bool, optional
            Whether to include cancelled bookings
        Returns
        -------
        bookings : list of dict
            Booking information, with keys including 'id', 'startTime', 'endTime',
            'patient', 'referral', 'equipmentItems', and 'location'
        """
        # TODO: request changes to seer-api so we can perform pagination
        query_string = graphql.ORGANIZATION_BOOKINGS
        vars = dict(organization_id=organisation_id, startTime=start_time, endTime=end_time,
                    includeCancelled=include_cancelled)
        response = self.execute_query(query_string, variable_values=vars)

        # query_string = graphql.get_bookings_query_string(organisation_id, start_time, end_time)
        # response = self.execute_query(query_string)

        return response['organisation']['bookings']

    def get_all_bookings_dataframe(self, organisation_id, start_time, end_time,
        include_cancelled=False):
        """
        Get all bookings for any studies that are active at any point between
        `start_time` and `end_time` as a DataFrame. See `get_all_bookings()`.

        Parameters
        ----------
        organisation_id : str
            Organisation ID associated with patient bookings
        start_time : int
            Timestamp in msec - find studies active after this point
        end_time : int
            Timestamp in msec - find studies active before this point
        include_cancelled: bool, optional
            Whether to include cancelled bookings

        Returns
        -------
        bookings_df : pd.DataFrame
            DataFrame with details about all relevant bookings
        """
        bookings_response = self.get_all_bookings(organisation_id, start_time, end_time,
            include_cancelled=include_cancelled)
        bookings = json_normalize(bookings_response).sort_index(axis=1)
        studies = self.pandas_flatten(bookings, 'patient.', 'studies')
        equipment = self.pandas_flatten(bookings, '', 'equipmentItems')
        bookings = bookings.drop('patient.studies', errors='ignore', axis='columns')
        bookings = bookings.drop('equipmentItems', errors='ignore', axis='columns')
        bookings = bookings.merge(studies, how='left', on='patient.id')
        bookings = bookings.merge(equipment, how='left', on='id')
        return bookings.drop_duplicates().reset_index(drop=True)

    # DIARY STUDY (FITBIT) ANALYSIS
    def get_diary_study_label_groups(self, patient_id, limit=20, offset=0):
        """
        Get diary label groups (e.g. heart rate, steps) for a patient diary study.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary data
        limit : int, optional
            The maximum number of results to return
        offset : int optional
            The index of the first label group to return. Useful in conjunction
            with `limit` for repeated calls

        Returns
        -------
        label_groups : list of dict
            Diary study label groups, with keys 'id', 'name', 'numberOfLabels'
        """
        # TODO use limit/offset for pagination (unlikely to be more than 20 label groups for a while)
        query_string = graphql.get_diary_study_label_groups_string(patient_id, limit, offset)
        response = self.execute_query(query_string)
        return response['patient']['diaryStudy']

    def get_diary_study_label_groups_dataframe(self, patient_id, limit=20, offset=0):
        """
        Get diary label groups (e.g. heart rate, steps) for a patient diary
        study as a DataFrame. See `get_diary_data_groups()` for details.

        Returns
        -------
        label_groups : pd.DataFrame
            Dataframe of diary study label groups
        """
        label_group_results = self.get_diary_study_label_groups(patient_id, limit, offset)
        if not label_group_results:
            return pd.DataFrame()

        label_groups = json_normalize(label_group_results['labelGroups']).sort_index(axis=1)
        label_groups['startTime'] = label_group_results['startTime']
        return label_groups

    # pylint:disable=too-many-arguments
    def get_diary_study_labels(self, patient_id, label_group_id, from_time=0, to_time=9e12,
                               limit=200, offset=0, max_items=None):
        """
        Get all diary study labels for a given patient and diary label group,
        e.g. heart rate.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary labels
        label_group_id : str
            The ID of the diary study label group for which to retrieve labels
        from_time : int, optional
            Timestamp in msec - find diary labels from this point onward
        to_time : int, optional
            Timestamp in msec - find diary labels up until this point
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            The index of the first label group to return
        max_items: int, optional
            max number of study labels to return.

        Returns
        -------
        data_labels : dict
            A dict with one key, 'labelGroup', which indexes to a dict with
        a 'labels' key. Labels include ['id', 'startTime', 'duration', 'tags', 'timezone']
        """
        variable_values = {
            'patient_id': patient_id,
            'label_group_id': label_group_id,
            'from_time': from_time,
            'to_time': to_time
        }
        return self.get_paginated_response(graphql.GET_LABELS_FOR_DIARY_STUDY_PAGED,
                                           variable_values=variable_values,
                                           limit=limit,
                                           object_path=['patient', 'diaryStudy'],
                                           iteration_path=['labelGroup', 'labels'],
                                           max_items=max_items,
                                           )

    # pylint:disable=too-many-arguments
    def get_diary_study_labels_dataframe(self, patient_id, label_group_id, from_time=0,
                                         to_time=9e12, limit=200, offset=0, max_items=None):
        """
        Get all diary study labels for a given patient and diary label group,
        returned as a DataFrame. See `get_diary_data_labels()` for details.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary labels
        label_group_id : str
            The ID of the diary study label group for which to retrieve labels
        from_time : int, optional
            Timestamp in msec - find diary labels from this point onward
        to_time : int, optional
            Timestamp in msec - find diary labels up until this point
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            The index of the first label group to return
        max_items: int, optional
            max number of study labels to return.

        Returns
        -------
        data_labels_df : pd.DataFrame
            DataFrame with information about labels for a diary study
        """
        label_results = self.get_diary_study_labels(patient_id, label_group_id, from_time, to_time,
                                                    limit=limit, offset=offset, max_items=max_items)
        if not label_results:
            return pd.DataFrame()
        label_group = json_normalize(label_results).sort_index(axis=1)
        labels = self.pandas_flatten(label_group, 'labelGroup.', 'labels')
        tags = self.pandas_flatten(labels, 'labels.', 'tags')

        label_group = label_group.drop('labelGroup.labels', errors='ignore', axis='columns')
        labels = labels.drop('labels.tags', errors='ignore', axis='columns')

        label_group = label_group.merge(labels, how='left', on='labelGroup.id', suffixes=('', '_y'))
        label_group = label_group.merge(tags, how='left', on='labels.id', suffixes=('', '_y'))

        return label_group

    def get_diary_study_channel_groups(self, patient_id, from_time=0, to_time=9e12):
        """
        Get all diary study channel groups and associated segment information for a given patient.

        Parameters
        ----------
        patient_id : str
            The patient ID for which to retrieve diary channel groups
        from_time : int
            Timestamp in msec - find segments from this point onward
        to_time : int
            Timestamp in msec - find segments up until this point

        Returns
        -------
        diary_channel_groups : list of dicts
            Diary channel group details, with keys 'id', 'name', 'startTime', 'segments'
        """
        query_string = graphql.get_diary_study_channel_groups_query_string(
            patient_id, from_time, to_time)
        response = self.execute_query(query_string)
        return response['patient']['diaryStudy']['channelGroups']

    def get_diary_study_channel_groups_dataframe(self, patient_id, from_time=0, to_time=9e12):
        """
        Get all diary study channel groups and associated segment information for a given
        patient, as a DataFrame. See `get_diary_channel_groups()` for details.

        Returns
        -------
        diary_channel_groups_df : pd.DataFrame
            Diary channel groups with columns 'id', 'name', 'startTime', 'segments'
        """
        metadata = self.get_diary_study_channel_groups(patient_id, from_time, to_time)
        channel_groups = json_normalize(metadata).sort_index(axis=1)
        if channel_groups.empty:
            return pd.DataFrame()

        segments = self.pandas_flatten(channel_groups, '', 'segments')
        data_chunks = self.pandas_flatten(segments, 'segments.', 'dataChunks')

        channel_groups = channel_groups.drop('segments', errors='ignore', axis='columns')
        segments = segments.drop('segments.dataChunks', errors='ignore', axis='columns')

        channel_groups = channel_groups.merge(segments, how='left', on='id', suffixes=('', '_y'))
        channel_groups = channel_groups.merge(data_chunks, how='left', on='segments.id',
                                              suffixes=('', '_y'))

        return channel_groups

    def get_diary_fitbit_data(self, segments):
        """
        Get Fitbit data from a patient diary study. `segments` should be a
        DataFrame as returned by `get_diary_channel_groups_dataframe()` that
        includes columns ['dataChunks.url', 'name', 'segments.startTime'].

        Parameters
        ----------
        segments : pd.DataFrame
            DataFrame with columns 'dataChunks.url', 'name' & 'segments.startTime',
            as returned by `get_diary_channel_groups_dataframe()`

        Returns
        -------
        fitbit_data_df : pd.DataFrame
            Fitbit data DataFrame with timestamp (adjusted), value, and group name

        """
        if not 'dataChunks.url' in segments.columns:
            return pd.DataFrame(columns=['name', 'timestamp', 'value'])
        segment_urls = segments['dataChunks.url']
        group_names = segments['name']
        start_times = segments['segments.startTime']
        timezones = segments['segments.timezone']

        data_list = []
        for idx, url in enumerate(segment_urls):
            # timestamps are returned in their utc time
            start_time = datetime.utcfromtimestamp(start_times[idx] / 1000)
            new_data = utils.get_diary_fitbit_data(url)
            # convert timestamps to true utc datetime
            new_data['timestamp'] = start_time + pd.to_timedelta(new_data['timestamp'], unit='ms')
            new_data['name'] = group_names[idx]
            new_data['timezone'] = timezones[idx]
            data_list.append(new_data)

        if data_list:
            data = pd.concat(data_list)
        else:
            data = pd.DataFrame()

        return data

    def get_mood_survey_results(self, survey_template_ids, limit=200, offset=0,  max_items=None):
        """
        Get mood survey results for one or more survey template IDs.

        Parameters
        ----------
        survey_template_ids : str or list of str
            A list of survey_template_ids for which to retrieve results
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Index of the first result to return
        max_items: int, optional
            max number of surveys to return.

        Returns
        -------
        mood_survey_results : list of dict
            A list of dictionaries with survey result data, including keys
            'completer', 'lastSubmittedAt', and 'fields', which indexes to a
            list of dictionaries with keys 'key' and 'value'
        """
        variable_values = {'survey_template_ids': survey_template_ids}
        return self.get_paginated_response(graphql.GET_MOOD_SURVEY_RESULTS_PAGED,
                                           variable_values=variable_values,
                                           limit=limit,
                                           object_path=['surveys'],
                                           max_items=max_items
                                           )

    def get_mood_survey_results_dataframe(self, survey_template_ids, limit=200, offset=0,
        max_items=None):
        """
        Get mood survey results as a DataFrame. See `get_mood_survey_results()`
        for details.

        Parameters
        ----------
        survey_template_ids : str or list of str
            A list of survey_template_ids for which to retrieve results
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Index of the first result to return
        max_items: int, optional
            max number of surveys to return.

        Returns
        -------
        mood_survey_results : pd.DataFrame
            Dataframe with survey.id, survey.lastSubmittedAt, surveyField.key, surveyField.value
        """
        results = self.get_mood_survey_results(survey_template_ids, limit, offset,
            max_items=max_items)

        if results is None or len(results) == 0:
            return pd.DataFrame()

        surveys = json_normalize(results)
        fields = self.pandas_flatten(surveys, '', 'fields')
        surveys = surveys.drop('fields', errors='ignore', axis='columns')
        surveys = surveys.merge(fields, how='left', on='id', suffixes=('', '_y'))

        return surveys

    def get_study_ids_in_study_cohort(self, study_cohort_id, limit=200, offset=0, max_items=None):
        """
        Get the IDs of studies in a given study cohort.

        Parameters
        ----------
        study_cohort_id : str
            The study cohort ID to retrieve
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Index of the first result to return
        max_items: int, optional
            max number of study ids to return.

        Returns
        -------
        study_ids : list of str
            Unique IDs, each identifying a study
        """
        variable_values = {'study_cohort_id': study_cohort_id}
        results = self.get_paginated_response(graphql.GET_STUDY_IDS_IN_STUDY_COHORT_PAGED,
                                   variable_values=variable_values,
                                   limit=limit,
                                   object_path=['studyCohort', 'studies'],
                                   max_items=max_items,
                                   )

        return [study['id'] for study in results]

    def create_study_cohort(self, name, description=None, key=None, study_ids=None):
        """
        Create a new study cohort.

        Parameters
        ----------
        name : str
            The name of the study cohort to create
        description : str, optional
            An optional description of the study cohort
        key: str, optional
            An optional key to describe the cohort. Defaults to the ID
        study_ids: list of str, optional
            Unique IDs, each identifying a study to add to the study cohort

        Returns
        -------
        cohort_id : str
            The study cohort ID
        """
        query_string = graphql.create_study_cohort_mutation_string(name, description, key,
                                                                   study_ids)
        return self.execute_query(query_string)

    def add_studies_to_study_cohort(self, study_cohort_id, study_ids):
        """
        Add studies to a study cohort by ID.

        Parameters
        ----------
        study_cohort_id : str
            The ID of the study cohort to modify
        study_ids : list of str
            Unique IDs, each identifying a study to add to the study cohort

        Returns
        -------
        cohort_id : str
            The study cohort ID
        """
        query_string = graphql.add_studies_to_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)

    def remove_studies_from_study_cohort(self, study_cohort_id, study_ids):
        """
        Remove studies from a study cohort by ID.

        Parameters
        ----------
        study_cohort_id : str
            The ID of the study cohort to modify
        study_ids : list of str
            Unique IDs, each identifying a study to remove from the study cohort

        Returns
        -------
        cohort_id : str
            The study cohort ID
        """
        query_string = graphql.remove_studies_from_study_cohort_mutation_string(
            study_cohort_id, study_ids)
        return self.execute_query(query_string)

    def get_user_ids_in_user_cohort(self, user_cohort_id, limit=200, offset=0, max_items=None):
        """
        Get the IDs of users in the given user cohort.

        Parameters
        ----------
        user_cohort_id : str
            ID of the user cohort to retrieve
        limit : int, optional
            Batch size for repeated API calls. Does not affect the total number
            of items retrieved
        offset : int, optional
            Index of the first result to return
        max_items: int, optional
            max number of studies to return.

        Returns
        -------
        user_ids : list of str
            User IDs that are in the cohort
        """
        variable_values = {'user_cohort_id': user_cohort_id}
        results = self.get_paginated_response(graphql.GET_USER_IDS_IN_USER_COHORT_PAGED,
                                   variable_values=variable_values,
                                   limit=limit,
                                   object_path=['userCohort', 'users'],
                                   max_items=max_items
                                   )

        return [user['id'] for user in results]

    def create_user_cohort(self, name, description=None, key=None, user_ids=None):
        """
        Create a new user cohort.

        Parameters
        ----------
        name : str
            The name of the user cohort to create
        description : str, optional
            An optional description of the user cohort
        key : str, optional
            An optional key to describe the cohort. Defaults to the ID
        user_ids : list of str
            A list of user IDs to add to the user cohort

        Returns
        -------
        cohort_id : str
            The user cohort ID
        """
        query_string = graphql.get_create_user_cohort_mutation_string(name, description, key,
                                                                      user_ids)
        return self.execute_query(query_string)

    def add_users_to_user_cohort(self, user_cohort_id, user_ids):
        """
        Add users to a user cohort by ID.

        Parameters
        ----------
        user_cohort_id : str
            The ID of the user cohort to modify
        user_ids : list of str
            A list of user IDs to add to the user cohort

        Returns
        -------
        cohort_id : str
            The user cohort ID
        """
        query_string = graphql.get_add_users_to_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)

    def remove_users_from_user_cohort(self, user_cohort_id, user_ids):
        """
        Remove users from a user cohort by ID

        Parameters
        ----------
        user_cohort_id : str
            The ID of the user cohort to modify
        user_ids : list of str
            A list of user IDs to remove from the user cohort

        Returns
        -------
        cohort_id : str
            The user cohort ID
        """
        query_string = graphql.get_remove_users_from_user_cohort_mutation_string(
            user_cohort_id, user_ids)
        return self.execute_query(query_string)
