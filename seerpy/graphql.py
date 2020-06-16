"""
GraphQL queries used by various seerpy.SeerConnect methods.
"""
import json

from . import utils


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


def get_labels_paged_query_string(study_id, label_group_id, from_time, to_time):
    return f"""
        query {{{{
            study (id: "{study_id}") {{{{
                id
                name
                labelGroup (labelGroupId: "{label_group_id}") {{{{
                    id
                    name
                    labelType
                    description
                    numberOfLabels
                    labels (limit: {{limit}}, offset: {{offset}}, fromTime: {from_time}, toTime: {to_time}) {{{{
                        id
                        note
                        startTime
                        duration
                        timezone
                        confidence
                        createdBy {{{{
                            fullName
                        }}}}
                        updatedAt
                        createdAt
                        tags {{{{
                            id
                            tagType {{{{
                                id
                                category {{{{
                                    id
                                    name
                                    description
                                }}}}
                                value
                            }}}}
                        }}}}
                    }}}}
                }}}}
            }}}}
        }}}}"""


# pylint:disable=too-many-arguments
def get_labels_string_query_string(study_id, label_group_id, from_time, to_time):
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
    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {json.dumps(study_ids)}) {{{{
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
    return """
        query {
            studyChannelGroupSegments(segmentIds: %s) {
                id
                baseDataChunkUrl
            }
        }""" % json.dumps(segment_ids)


def get_data_chunk_urls_query_string(data_chunks, s3_urls=True):
    s3_urls = 'true' if s3_urls else 'false'
    return """
        query {
            studyChannelGroupDataChunkUrls(
                    chunkKeys: [%s],
                    s3Urls: %s
                    )
        }""" % (json.dumps(data_chunks), s3_urls)


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
    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {json.dumps(study_ids)}) {{{{
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
    return """
        mutation {
            addLabelsToLabelGroup(
                groupId: "%s",
                labels: [%s]
            ) {
                id
            }
        }""" % (group_id, json.dumps(labels))


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
                    lastActive
                }
            }
        }"""


def get_diary_insights_paged_query_string(patient_id, limit, offset):
    return f"""
        query {{{{
            patient (id: "{patient_id}") {{{{
                id
                insights (limit: {{limit}}, offset: {{offset}}) {{{{
                    id
                    report {{{{
                        id
                    }}}}
                    reportDate
                    reportPeriod
                    emailNotificationSent
                    emailLinkOpened
                    createdAt
                    updatedAt
                    }}}}
                }}}}
            }}}}"""


def get_diary_created_at_query_string(patient_id):
    return """
        query {
            patient (id: "%s") {
                diary {
                    createdAt
                }
            }
        }""" % patient_id


def get_diary_labels_query_string(patient_id, label_type, limit, offset, from_time, to_time,
                                  from_duration, to_duration):
    return """
        query {
            patient (id: "%s") {
                id
                diary {
                    id
                    createdAt
                    labelGroups (filters: [{name: "labelType", value:"%s"}]) {
                        id
                        labelType
                        labelSourceType
                        name
                        numberOfLabels
                        labels(limit: %.0f, offset: %.0f, ranges: [{ from: %.0f to: %.0f }, { from: %.0f to: %.0f }]) {
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
        }""" % (patient_id, label_type, limit, offset, from_time, to_time, from_duration,
                to_duration)


def get_diary_medication_alerts_query_string(patient_id, from_time, to_time):

    return """query {
                patient (id: "%s") {
                    diary {
                        id
                        alerts {
                            id
                            name
                            labels (ranges: [{ from: %.0f to: %.0f }]) {
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
            }""" % (patient_id, from_time, to_time)


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
    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: {json.dumps(study_ids)}) {{{{
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


def get_labels_for_diary_study_paged_query_string(patient_id, label_group_id, from_time, to_time):
    return f"""
        query {{{{
            patient (id: "{patient_id}") {{{{
                id
                diaryStudy {{{{
                    labelGroup (labelGroupId: "{label_group_id}") {{{{
                        id
                        labels (limit: {{limit}}, offset: {{offset}}, from: {from_time}, to: {to_time}) {{{{
                            id
                            startTime
                            timezone
                            duration
                            tags {{{{
                                tagType {{{{
                                    value
                                }}}}
                            }}}}
                        }}}}
                    }}}}
                }}}}
            }}}}
        }}}}"""


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


def get_study_ids_in_study_cohort_paged_query_string(study_cohort_id):
    return f"""
        query {{{{
            studyCohort(id: "{study_cohort_id}") {{{{
                id
                name
                studies(limit: {{limit}}, offset: {{offset}}) {{{{
                    id
                }}}}
            }}}}
        }}}}
    """


def create_study_cohort_mutation_string(name, description=None, key=None, study_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if study_ids is not None:
        args.append(('studyIds', json.dumps(study_ids)))

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
    """ % (study_cohort_id, json.dumps(study_ids))


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
    """ % (study_cohort_id, json.dumps(study_ids))


def get_mood_survey_results_paged_query_string(survey_template_ids):
    return f"""
        query {{{{
            surveys(surveyTemplateIds: {json.dumps(survey_template_ids)}, limit: {{limit}}, offset: {{offset}}) {{{{
                completer {{{{
                    id
                }}}}
                id
                fields {{{{
                    key
                    value
                }}}}
                lastSubmittedAt
            }}}}
        }}}}
    """


def get_user_ids_in_user_cohort_paged_query_string(user_cohort_id):
    return f"""
        query {{{{
            userCohort(id: "{user_cohort_id}") {{{{
                id
                name
                users(limit: {{limit}}, offset: {{offset}}) {{{{
                    id
                }}}}
            }}}}
        }}}}
    """


def get_create_user_cohort_mutation_string(name, description=None, key=None, user_ids=None):
    args = [('name', utils.quote_str(name))]

    if description is not None:
        args.append(('description', utils.quote_str(description)))

    if key is not None:
        args.append(('key', utils.quote_str(key)))

    if user_ids is not None:
        args.append(('userIds', json.dumps(user_ids)))

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
    """ % (user_cohort_id, json.dumps(user_ids))


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
    """ % (user_cohort_id, json.dumps(user_ids))
