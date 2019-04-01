

def get_json_list(list_of_strings, include_brackets=True):
    json_list = ', '.join('"%s"' % string for string in list_of_strings)
    if include_brackets:
        json_list = '[' + json_list + ']'
    return json_list

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
                    labels (limit: %.0f, offset: %.0f, fromTime: %.0f, toTime: %.0f) {
                        id
                        note
                        startTime
                        duration
                        timezone
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


def get_studies_by_search_term_paged_query_string(search_term, party_id):
    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, searchTerm: "{search_term}", 
            partyId: "{party_id}") {{{{
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
        labels_string = labels_string[:-1] # remove last comma
        labels_string += '},'
    labels_string = labels_string[:-1] # remove last comma
    return labels_string


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
        label_type_string = ', labelType: "' + label_type + '"'

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


def get_patients_query_string(party_id=""):
    return """
        query {
            patients (partyId: "%s") {
                id
                user {
                    id
                    fullName
                    shortName
                    email
                }
            }
        }""" % party_id


def get_diary_labels_query_string(patient_id):
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
                        labels {
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
        }""" % patient_id
