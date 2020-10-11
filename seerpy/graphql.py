"""
GraphQL queries used by various seerpy.SeerConnect methods.
"""
from . import utils


def get_json_list(list_of_strings, include_brackets=True):
    """
    Convert a list of strings into a single string representation, suitable for
    inclusion in GraphQL queries.

    Parameters
    ---------
    list_of_strings : list of str
        Strings to convert to single string
    include_backets : bool
        Whether to include square braces in the returned string

    Returns
    -------
    stringified_list : str
        String representation of the input list

    Example
    -------
    >>> get_json_list(['cat', 'dog'])
    '["cat", "dog"]'

    >>> get_json_list(['cat', 'dog'], include_brackets=False)
    '"cat", "dog"'
    """
    json_list = ', '.join('"%s"' % string for string in list_of_strings)
    if include_brackets:
        json_list = '[' + json_list + ']'
    return json_list


def get_string_from_list_of_dicts(list_of_dicts):
    """
    Convert a list of dicts into a flattened string representation.

    Parameters
    ---------
    list_of_dicts : list of dict
        Arbitrary dictionaries to convert to string

    Returns
    -------
    stringified_dicts : str
        String representation of the input list of dicts

    Example
    -------
    >>> dicts = [{'a': 'this', 'b': 'that', 'c': {'the other'}}, {'d': 'then'}]
    >>> get_string_from_list_of_dicts(dicts)
    ' { a: "this", b: "that", c: {\'the other\'}}, { d: "then"}'
    """
    labels_string = ''
    for d in list_of_dicts:
        labels_string += ' {'
        for k in d.keys():
            if d[k] is None:
                continue
            labels_string += ' ' + k + ': '
            if isinstance(d[k], str):
                labels_string += '"' + d[k] + '",'
            elif isinstance(d[k], dict):
                labels_string += get_string_from_list_of_dicts(list(d[k]))
            elif isinstance(d[k], list):
                if d[k]:
                    labels_string += (get_json_list(d[k]) + ",")
            else:
                labels_string += str(d[k]) + ','
        labels_string = labels_string[:-1]  # remove last comma
        labels_string += '},'
    labels_string = labels_string[:-1]  # remove last comma
    return labels_string


GET_STUDY_WITH_DATA = """
    query study_with_data($study_id: String!) {
        study (id: $study_id) {
            id
            patient {
                id
                user {
                    fullName
                }
            }
            name
            description
            startTime
            duration
            channelGroups {
                id
                name
                sampleRate
                samplesPerRecord
                recordLength
                chunkPeriod
                recordsPerChunk
                sampleEncoding
                compression
                signalMin
                signalMax
                units
                exponent
                timestamped
                channelGroupType {
                    id
                }
                segments {
                    id
                    startTime
                    duration
                    timezone
                }
                channels {
                    id
                    name
                    channelType {
                        name
                        category
                    }
                }
            }
        }
    }"""

GET_LABELS_PAGED = """
    query labels($study_id: String!,
                 $label_group_id: String!,
                 $limit: PaginationAmount,
                 $offset: Int,
                 $from_time: Float,
                 $to_time: Float) {
        study (id: $study_id) {
            id
            name
            startTime
            duration
            labelGroup (labelGroupId: $label_group_id) {
                id
                name
                labelType
                description
                numberOfLabels
                labels (limit: $limit, offset: $offset, fromTime: $from_time, toTime: $to_time) {
                    id
                    note
                    startTime
                    duration
                    timezone
                    confidence
                    createdBy {
                        fullName
                    }
                    updatedAt
                    createdAt
                    tags {
                        id
                        tagType {
                            id
                            category {
                                id
                                name
                                description
                            }
                            value
                        }
                    }
                }
            }
        }
    }"""

GET_LABELS_STRING = """
    query labels_string($study_id: String!,
                        $label_group_id: String!,
                        $from_time: Float!,
                        $to_time: Float!) {
        study (id: $study_id) {
            id
            name
            labelGroup (labelGroupId: $label_group_id) {
                id
                name
                labelType
                description
                numberOfLabels
                labelString (fromTime: $from_time, toTime: $to_time)
            }
        }
    }"""

GET_LABEL_GROUPS_FOR_STUDY_IDS_PAGED = """
    query studies($study_ids: [String],
                  $limit: PaginationAmount,
                  $offset: Int) {
        studies (studyIds: $study_ids, limit: $limit, offset: $offset) {
            id
            name
            labelGroups {
                id
                name
                description
                labelType
                numberOfLabels
            }
        }
    }"""


def get_channel_groups_query_string(study_id):
    return """
        query {
            study(id: "%s") {
                id
                patient {
                    id
                }
                name
                channelGroups {
                    name
                    sampleRate
                    segments {
                        id
                    }
                }
            }
        }""" % study_id


#    studyChannelGroupSegments
def get_segment_urls_query_string(segment_ids):
    segment_ids_string = get_json_list(segment_ids)

    return """
        query {
            studyChannelGroupSegments(segmentIds: %s) {
                id
                baseDataChunkUrl
            }
        }""" % segment_ids_string


def get_data_chunk_urls_query_string(data_chunks, s3_urls=True):
    chunk_keys = get_string_from_list_of_dicts(data_chunks)
    s3_urls = 'true' if s3_urls else 'false'
    return """
        query {
            studyChannelGroupDataChunkUrls(
                    chunkKeys: [%s],
                    s3Urls: %s
                    )
        }""" % (chunk_keys, s3_urls)


GET_STUDIES_BY_SEARCH_TERM_PAGED = """
    query studies($search_term: String,
                  $limit: PaginationAmount,
                  $offset: Int) {
        studies (searchTerm: $search_term, limit: $limit, offset: $offset) {
            id
            name
            patient {
                id
                user {
                    fullName
                }
            }
        }
    }"""

GET_STUDIES_BY_STUDY_ID_PAGED = """
    query studies($study_ids: [String],
                  $limit: PaginationAmount,
                  $offset: Int) {
        studies (studyIds: $study_ids, limit: $limit, offset: $offset) {
            id
            name
            patient {
                id
                user {
                    fullName
                }
            }
        }
    }"""

ADD_LABELS = """
    mutation addLabelsToLabelGroup($group_id: String!,
                                   $labels: [NewStudyLabel]!) {
        addLabelsToLabelGroup(groupId: $group_id, labels: $labels) {
            id
        }
    }"""

GET_TAG_IDS = """
    query {
        labelTags {
            id
            category {
                id
                name
                description
            }
            value
            forStudy
            forDiary
        }
    }"""


def get_add_label_group_mutation_string(study_id, name, description, label_type):
    if label_type is None:
        label_type_string = ''
    else:
        label_type_string = ', labelType: ' + label_type

    return """
        mutation {
            addLabelGroupToStudy(studyId: "%s", name: "%s", description: "%s"%s) {
                id
            }
        }""" % (study_id, name, description, label_type_string)


def get_remove_label_group_mutation_string(group_id):
    return """
        mutation {
            removeLabelGroupFromStudy(groupId: "%s")
        }""" % (group_id)


EDIT_STUDY_LABEL_GROUP = """
    mutation editStudyLabelGroup($group_id: String!,
                                 $name: String,
                                 $description: String) {
        editStudyLabelGroup(id: $group_id,
                            name: $name,
                            description: $description) {
            id
        }
    }"""


def get_viewed_times_query_string(study_id, limit, offset):
    return """
        query {
            viewGroups(studyId: "%s") {
                user {
                    fullName
                }
                views (limit: %.0f, offset: %.0f) {
                    id
                    startTime
                    duration
                    createdAt
                    updatedAt
                }
            }
        }""" % (study_id, limit, offset)


GET_ORGANISATIONS = """
    query {
        organisations {
            id
            partyId
            ownerId
            name
            description
            isPublic
            isDeleted
        }
    }"""


def get_user_from_patient_query_string(patient_id):
    return """
        query {
            patient (id: "%s") {
                id
                user {
                    id
                    fullName
                    shortName
                    email
                }
            }
        }""" % patient_id


GET_PATIENTS = """
    query getPatientList {
        patients {
            id
            user {
                id
                fullName
                shortName
                email
                lastActive
                lastInsightGenerated
                preferredTimezone
            }
        }
    }"""

GET_DIARY_INSIGHTS_PAGED = """
    query patient($patient_id: String!,
                  $limit: PaginationAmount,
                  $offset: Int) {
        patient (id: $patient_id) {
            id
            insights (limit: $limit, offset: $offset) {
                id
                report {
                    patientId
                    reportPeriod
                }
                reportDate
                emailNotificationSent
                emailLinkOpened
                createdAt
                updatedAt
            }
        }
    }"""


def get_diary_created_at_query_string(patient_id):
    return """
        query {
            patient (id: "%s") {
                diary {
                    createdAt
                }
            }
        }""" % patient_id


GET_DIARY_LABELS = """
    query getDiaryLabels($patient_id: String!,
                         $value: String!,
                         $limit: PaginationAmount,
                         $offset: Int,
                         $from_time: Float!,
                         $to_time: Float!,
                         $from_duration: Float!,
                         $to_duration: Float!) {
        patient (id: $patient_id) {
            id
            diary {
                id
                createdAt
                labelGroups (filters: [{name: "labelType" value: $value}]) {
                    id
                    labelType
                    labelSourceType
                    name
                    numberOfLabels
                    labels (limit: $limit, offset: $offset,
                            ranges: [{ from: $from_time to: $to_time },
                                     { from: $from_duration to: $to_duration }]) {
                        id
                        startTime
                        timezone
                        duration
                        note
                        tags {
                            id
                            tagType {
                                id
                                category  {
                                    id
                                    name
                                    description
                                }
                                value
                            }
                        }
                    }
                }
            }
        }
    }"""

GET_DIARY_MEDICATION_ALERTS = """
    query getDiaryAlertMedication($patient_id: String!,
                                  $from_time: Float!,
                                  $to_time: Float!) {
        patient (id: $patient_id) {
            diary {
                id
                alerts {
                    id
                    name
                    labels (ranges: [{ from: $from_time to: $to_time }]) {
                        id
                        startTime
                        scheduledTime
                        alert {
                            name
                        }
                        scheduledTime
                        startTime
                        doses {
                            doseValue
                            doseUnit
                            doseExponent
                            medication {
                                brandName
                                drugName
                            }
                        }
                        deleted
                    }
                }
            }
        }
    }"""

GET_DIARY_MEDICATION_ALERT_WINDOWS = """
    query getDiaryAlertWindow($patient_id: String!,
                              $filters: [SearchFilter!]) {
        patient (id: $patient_id) {
            diary {
                id
                alerts {
                    id
                    name
                    windows (filters: $filters) {
                        startTime
                        timezone
                        endTime
                    }
                }
            }
        }
    }"""

GET_DIARY_MEDICATION_COMPLIANCE = """
    query getMedicationCompliance($patient_id: String!,
                                  $from_time: Float!,
                                  $to_time: Float!) {
        patient (id: $patient_id) {
            id
            diary {
                id
                medicationCompliance (range: { from: $from_time, to: $to_time }) {
                    label
                    status
                    date
                }
            }
        }
    }"""

GET_DOCUMENTS_FOR_STUDY_IDS_PAGED = """
    query studies($study_ids: [String],
                  $limit: PaginationAmount,
                  $offset: Int) {
        studies (studyIds: $study_ids, limit: $limit, offset: $offset) {
            id
            name
            documents {
                id
                name
                uploaded
                fileSize
                downloadFileUrl
            }
        }
    }"""


def get_add_document_mutation_string(study_id, document):
    return """
        mutation {
            createStudyDocuments(
                studyId: "%s",
                documents: [{name: "%s"}]
            ) {
                id
                name
                uploadFileUrl
            }
        }""" % (study_id, document)


def get_confirm_document_mutation_string(study_id, document_id):
    return """
        mutation {
            confirmStudyDocuments(
                studyId: "%s",
                documentIds: ["%s"]
            ) {
                id
                name
                downloadFileUrl
            }
        }""" % (study_id, document_id)


def get_bookings_query_string(organisation_id, start_time, end_time):
    return """query {
                organisation(id: "%s") {
                    bookings(startTime: %.0f, endTime: %.0f) {
                        id
                        equipmentItems {
                            name
                            equipmentType {
                                type
                            }
                        }
                        bookingTemplate {
                            name
                        }
                        referral {
                            id
                        }
                        startTime {
                            datetime
                            timezone
                        }
                        endTime {
                            datetime
                            timezone
                        }
                        patient {
                            id
                            user {
                                fullName
                            }
                            studies {
                                id
                                name
                            }
                        }
                        location {
                                name
                                suburb
                                }
                    }
                }
            }""" % (organisation_id, start_time, end_time)


def get_diary_study_label_groups_string(patient_id, limit, offset):

    return """
        query {
            patient (id: "%s") {
                id
                diaryStudy {
                    startTime
                    labelGroups(limit: %.0f, offset: %.0f) {
                        id
                        name
                        numberOfLabels
                    }
                }
            }
        }
    """ % (patient_id, limit, offset)


GET_LABELS_FOR_DIARY_STUDY_PAGED = """
    query patient($patient_id: String!,
                  $label_group_id: String,
                  $limit: PaginationAmount,
                  $offset: Int,
                  $from_time: Float,
                  $to_time: Float) {
        patient (id: $patient_id) {
            id
            diaryStudy {
                labelGroup (labelGroupId: $label_group_id) {
                    id
                    labels (limit: $limit, offset: $offset, from: $from_time, to: $to_time) {
                        id
                        startTime
                        timezone
                        duration
                        tags {
                            tagType {
                                value
                            }
                        }
                    }
                }
            }
        }
    }"""


def get_diary_study_channel_groups_query_string(patient_id, from_time, to_time):

    return """
        query {
            patient(id: "%s") {
                id
                diaryStudy {
                    channelGroups {
                        id
                        name
                        startTime
                        segments (ranges: [{ from: %.0f, to: %.0f }]) {
                            id
                            startTime
                            duration
                            timezone
                            dataChunks {
                                url
                            }
                        }
                    }
                }
            }
        }""" % (patient_id, from_time, to_time)


GET_STUDY_IDS_IN_STUDY_COHORT_PAGED = """
    query studyCohort($study_cohort_id: String,
                      $limit: PaginationAmount,
                      $offset: Int) {
        studyCohort(id: $study_cohort_id) {
            id
            name
            studies(limit: $limit, offset: $offset) {
                id
            }
        }
    }"""


def create_study_cohort_mutation_string(name, description=None, key=None, study_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if study_ids is not None:
        args.append(('studyIds', get_json_list(study_ids)))

    return """
        mutation {
            createStudyCohort(input: {
                %s
            }) {
                studyCohort {
                    id
                }
            }
        }
    """ % (', '.join([f'{key}: {val}' for key, val in args]))


def add_studies_to_study_cohort_mutation_string(study_cohort_id, study_ids):
    return """
        mutation {
            addStudiesToStudyCohort(
                studyCohortId: "%s",
                studyIds: %s
            ) {
                studyCohort {
                    id
                }
            }
        }
    """ % (study_cohort_id, get_json_list(study_ids))


def remove_studies_from_study_cohort_mutation_string(study_cohort_id, study_ids):
    return """
        mutation {
            removeStudiesFromStudyCohort(
                studyCohortId: "%s",
                studyIds: %s
            ) {
                studyCohort {
                    id
                }
            }
        }
    """ % (study_cohort_id, get_json_list(study_ids))


GET_MOOD_SURVEY_RESULTS_PAGED = """
    query surveys($survey_template_ids: [String!],
                  $limit: PaginationAmount,
                  $offset: Int) {
        surveys(surveyTemplateIds: $survey_template_ids, limit: $limit, offset: $offset) {
            completer {
                id
            }
            id
            fields {
                key
                value
            }
            lastSubmittedAt
        }
    }"""

GET_USER_IDS_IN_USER_COHORT_PAGED = """
    query userCohort($user_cohort_id: String,
                     $limit: PaginationAmount,
                     $offset: Int) {
        userCohort(id: $user_cohort_id) {
            id
            name
            users(limit: $limit, offset: $offset) {
                id
            }
        }
    }"""


def get_create_user_cohort_mutation_string(name, description=None, key=None, user_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if user_ids is not None:
        args.append(('userIds', get_json_list(user_ids)))

    return """
        mutation {
            createUserCohort(input: {
                %s
            }) {
                userCohort {
                    id
                }
            }
        }
    """ % (', '.join([f'{key}: {val}' for key, val in args]))


def get_add_users_to_user_cohort_mutation_string(user_cohort_id, user_ids):
    return """
        mutation {
            addUsersToUserCohort(
                userCohortId: "%s",
                userIds: %s
            ) {
                userCohort {
                    id
                }
            }
        }
    """ % (user_cohort_id, get_json_list(user_ids))


def get_remove_users_from_user_cohort_mutation_string(user_cohort_id, user_ids):
    return """
        mutation {
            removeUsersFromUserCohort(
                userCohortId: "%s",
                userIds: %s
            ) {
                userCohort {
                    id
                }
            }
        }
    """ % (user_cohort_id, get_json_list(user_ids))
