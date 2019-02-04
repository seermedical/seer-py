
def get_study_with_data_query_string(studyId):
    return '''
        query {
            study (id: "%s") {
                id
                patient {
                    id
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
        }
    ''' % (studyId)

def get_labels_query_string(studyId, labelGroupId, fromTime,  # pylint:disable=too-many-arguments
                            toTime, limit, offset):
    return '''
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
        }
    ''' % (studyId, labelGroupId, limit, offset, fromTime, toTime)


def get_label_groups_for_study_ids_paged_query_string(study_ids):

    study_ids_string = ','.join(f'"{study_id}"' for study_id in study_ids)

    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: [{study_ids_string}]) {{{{
                id
                name
                labelGroups {{{{
                    id
                    name
                }}}}
            }}}}
        }}}}"""


def get_channel_groups_query_string(studyId):
    return '''
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
        }
    ''' % (studyId)


#    studyChannelGroupSegments
def get_segment_urls_query_string(segmentIds):
    return '''
        query {
            studyChannelGroupSegments(segmentIds: %s) {
                id
                baseDataChunkUrl
            }
        }
    ''' % ('["' + '\",\"'.join(segmentIds) + '"]')


def get_studies_by_search_term_paged_query_string(searchTerm):
    return f'''
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, searchTerm: "{searchTerm}") {{{{
                id
                name
                patient {{{{
                    id
                }}}}
            }}}}
        }}}}'''


def get_studies_by_study_id_paged_query_string(study_ids):

    study_ids_string = ','.join(f'"{study_id}"' for study_id in study_ids)

    return f"""
        query {{{{
            studies (limit: {{limit}}, offset: {{offset}}, studyIds: [{study_ids_string}]) {{{{
                id
                name
                patient {{{{
                    id
                }}}}
            }}}}
        }}}}"""


def get_string_from_dict(dictionary):
    labels_string = ""
    for d in dictionary:
        labels_string += " {"
        for k in d.keys():
            labels_string += " " + k + ": "
            if isinstance(d[k], str):
                labels_string += "\"" + d[k] + "\","
            elif isinstance(d[k], dict):
                labels_string += get_string_from_dict(d[k])
            elif isinstance(d[k], list):
                labels_string += "[\"" + "\", \"".join(d[k]) + "\"],"
            else:
                labels_string += str(d[k]) + ","
        labels_string = labels_string[:-1] # remove last comma
        labels_string += "},"
    labels_string = labels_string[:-1] # remove last comma
    return labels_string


def get_add_labels_mutation_string(groupId, labels):

    labels_string = get_string_from_dict(labels)
    return f'''
        mutation {{
            addLabelsToLabelGroup(
                groupId: "{groupId}",
                labels: [{labels_string}]
            ) {{
                id
            }}
        }}'''

def get_tag_id_query_string():

    return '''
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
        }
        '''


def get_add_label_group_mutation_string(studyId, name, description, labelType):
    if labelType is None:
        labelTypeString = ''
    else:
        labelTypeString = ', labelType: ' + labelType
    return '''
        mutation {
            addLabelGroupToStudy(studyId: "%s", name: "%s", description: "%s"%s) {
                id
            }
        }
    ''' % (studyId, name, description, labelTypeString)

def get_remove_label_group_mutation_string(groupId):
    return '''
        mutation {
            removeLabelGroupFromStudy(groupId: "%s")
        }
    ''' % (groupId)

def get_viewed_times_query_string(studyId):
    return '''
        query {
            viewGroups(studyId: "%s") {
                user {
                  fullName
                }
                views {
                  id
                  startTime
                  duration
                  createdAt
                  updatedAt
                }
              }
            }
    ''' % (studyId)

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
        }
    """

def get_patients_query_string(party_id=""):
    return f"""
    query {{
      patients (partyId: "{party_id}") {{
        id
        user {{
          id
          fullName
          shortName
          email
        }}
      }}
    }}
    """

def get_diary_labels_query_string(patient_id):
    return f"""
    query {{
      patient (id: "{patient_id}") {{
        id
        diary {{
          id
          labelGroups {{
            id
            labelType
            labelSourceType
            name
            labels {{
              id
              startTime
              timezone
              duration
              note
              tags {{
                id
                tagType {{
                  id
                  category  {{
                    id
                    name
                    description
                  }}
                  value
                }}
              }}
            }}
          }}
        }}
      }}
    }}
    """