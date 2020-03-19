from . import utils


def get_json_list(list_of_strings, include_brackets=True):
    json_list = ', '.join('"%s"' % string for string in list_of_strings)
    if include_brackets:
        json_list = '[' + json_list + ']'
    return json_list


def get_string_from_list_of_dicts(list_of_dicts):
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


def get_study_with_data_query_string(study_id):
    return """
        query {
            study (id: "%s") {
                id
                patient {
                    id
                    user {
                        fullName
                        }
                }
                name
                description
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
                    segments (fromTime: 1.0, toTime: 9000000000000) {
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
        }""" % study_id


def get_labels_query_string(study_id, label_group_id,  # pylint:disable=too-many-arguments
                            from_time, to_time, limit, offset):
    return """
        query {
            study (id: "%s") {
                id
                name
                labelGroup (labelGroupId: "%s") {
                    id
                    name
                    labelType
                    description
                    numberOfLabels
                    labels (limit: %.0f, offset: %.0f, fromTime: %.0f, toTime: %.0f) {
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
        }""" % (study_id, label_group_id, limit, offset, from_time, to_time)


def get_labels_string_query_string(study_id, label_group_id,  # pylint:disable=too-many-arguments
                                   from_time, to_time):
    return """
        query {
            study (id: "%s") {
                id
                name
                labelGroup (labelGroupId: "%s") {
                    id
                    name
                    labelType
                    description
                    numberOfLabels
                    labelString (fromTime: %.0f, toTime: %.0f)
                }
            }
        }""" % (study_id, label_group_id, from_time, to_time)


def get_label_groups_for_study_ids_paged_query_string(study_ids):
    study_ids_string = get_json_list(study_ids)

    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {study_ids_string}) {{{{
                id
                name
                labelGroups {{{{
                    id
                    name
                    labelType
                    numberOfLabels
                }}}}
            }}}}
        }}}}"""


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


def get_studies_by_search_term_paged_query_string(search_term):
    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, searchTerm: "{search_term}") {{{{
                id
                name
                patient {{{{
                    id
                    user {{{{
                        fullName
                        }}}}
                }}}}
            }}}}
        }}}}"""


def get_studies_by_study_id_paged_query_string(study_ids):
    study_ids_string = get_json_list(study_ids)

    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {study_ids_string}) {{{{
                id
                name
                patient {{{{
                    id
                    user {{{{
                        fullName
                        }}}}
                }}}}
            }}}}
        }}}}"""


def get_add_labels_mutation_string(group_id, labels):
    labels_string = get_string_from_list_of_dicts(labels)

    return """
        mutation {
            addLabelsToLabelGroup(
                groupId: "%s",
                labels: [%s]
            ) {
                id
            }
        }""" % (group_id, labels_string)


def get_tag_id_query_string():
    return """
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


def get_organisations_query_string():
    return """
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


def get_patients_query_string():
    return """
        query {
            patients {
                id
                user {
                    id
                    fullName
                    shortName
                    email
                }
            }
        }"""


def get_diary_labels_query_string(patient_id, limit, offset):
    return """
        query {
            patient (id: "%s") {
                id
                diary {
                    id
                    labelGroups {
                        id
                        labelType
                        labelSourceType
                        name
                        numberOfLabels
                        labels(limit: %.0f, offset: %.0f) {
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
        }""" % (patient_id, limit, offset)

def get_diary_medication_compliance_query_string(patient_id, from_time, to_time):

    return """
        query {
            patient (id: "%s") {
                id
                diary {
                    id
                    medicationCompliance (range: { from: %.0f, to: %.0f }) {
                        label
                        status
                        date
                    }
                }
            }
        }""" % (patient_id, from_time, to_time)

def get_documents_for_study_ids_paged_query_string(study_ids):
    study_ids_string = get_json_list(study_ids)

    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {study_ids_string}) {{{{
                id
                name
                documents {{{{
                    id
                    name
                    uploaded
                    fileSize
                    downloadFileUrl
                }}}}
            }}}}
        }}}}"""


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
                    labelGroups(limit: %.0f, offset: %.0f) {
                        id
                        name
                        numberOfLabels
                    }
                }
            }
        }
    """ % (patient_id, limit, offset)


def get_labels_for_diary_study_query_string(patient_id, label_group_id,  # pylint:disable=too-many-arguments
                                            from_time, to_time, limit, offset):
    return """
        query {
            patient (id: "%s") {
                id
                diaryStudy {
                    labelGroup (labelGroupId: "%s") {
                        id
                        labels (limit: %.0f, offset: %.0f, from: %.0f, to: %.0f) {
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
        }""" % (patient_id, label_group_id, limit, offset, from_time, to_time)


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


def get_study_ids_in_study_cohort_query_string(study_cohort_id, limit, offset):
    return """
        query {
            studyCohort(id: %s) {
                id
                name
                studies(limit: %0.f, offset: %0.f) {
                    id
                }
            }
        }
    """ % (utils.quote_str(study_cohort_id), limit, offset)


def create_study_cohort_mutation_string(name, description=None, key=None, study_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if study_ids is not None:
        args.append(
            ('studyIds', get_json_list(study_ids)))

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
                studyCohortId: %s,
                studyIds: %s
            ) {
                studyCohort {
                    id
                }
            }
        }
    """ % (
        utils.quote_str(study_cohort_id),
        get_json_list(study_ids)
    )


def remove_studies_from_study_cohort_mutation_string(study_cohort_id, study_ids):
    return """
        mutation {
            removeStudiesFromStudyCohort(
                studyCohortId: %s,
                studyIds: %s
            ) {
                studyCohort {
                    id
                }
            }
        }
    """ % (
        utils.quote_str(study_cohort_id),
        get_json_list(study_ids)
    )


def get_mood_survey_results_query_string(survey_template_ids, limit, offset):
    return """
    query {
        surveys(surveyTemplateIds: [%s], limit: %0.f, offset: %0.f) {
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
    }
    """ % (
        ','.join([f'"{survey_template_id}"' for survey_template_id in survey_template_ids]),
        limit,
        offset
    )

def get_user_ids_in_user_cohort_query_string(user_cohort_id, limit, offset):
    return """
        query {
            userCohort(id: %s) {
                id
                name
                users(limit: %0.f, offset: %0.f) {
                    id
                }
            }
        }
    """ % (utils.quote_str(user_cohort_id), limit, offset)


def get_create_user_cohort_mutation_string(name, description=None, key=None, user_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if user_ids is not None:
        args.append(
            ('userIds', get_json_list(user_ids)))

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
                userCohortId: %s,
                userIds: %s
            ) {
                userCohort {
                    id
                }
            }
        }
    """ % (
        utils.quote_str(user_cohort_id),
        get_json_list(user_ids)
    )


def get_remove_users_from_user_cohort_mutation_string(user_cohort_id, user_ids):
    return """
        mutation {
            removeUsersFromUserCohort(
                userCohortId: %s,
                userIds: %s
            ) {
                userCohort {
                    id
                }
            }
        }
    """ % (
        utils.quote_str(user_cohort_id),
        get_json_list(user_ids)
    )
