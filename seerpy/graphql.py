
def studyWithDataQueryString(studyId):
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

def getLabesQueryString(studyId, labelGroupId, fromTime, toTime, limit, offset):
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


def labelGroupQueryString(studyId):
        return '''
        query {
            study (id: "%s") {
                id
                name
                labelGroups {
                    id
                    name
                    labelType
                    description
                }
            }
        }
    ''' % (studyId)

def labelGroupsQueryString(limit, offset, studyIds):
    start =  '''
        query {
            studies (limit: %.0f, offset: %.0f, studyIds: ['''% (limit, offset)

    end = ''']) {
                id
                name
                labelGroups {
                    id
                    name

                }
            }
        }
    '''
    lst = ''
    for s in studyIds:
        lst = lst + '"' + s + '",'
    return start + lst[:-1] + end

def channelGroupsQueryString(studyId):
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

def segmentUrlsQueryString(segmentIds):
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


def studyQueryStudy(studyId):
    return '''
        query {
            study(id: "%s") {
                id
                patient {
                    id
                }
                name
            }
        }
    ''' % (studyId)

def addLabelMutationString(groupId, startTime, duration, timezone):
    return '''
        mutation {
            addLabelsToLabelGroup(
                groupId: "%s",
                labels: [{ startTime: %f, duration: %f, timezone: %f }]
            ) {
                id
            }
        }
    ''' % (groupId, startTime, duration, timezone)

def addLabelsMutationString(groupId, labels):
    start = '''
        mutation {
            addLabelsToLabelGroup(
                groupId: "%s",
                labels: [''' % (groupId)
    end = ''']
                ) {
                id
            }
        }
        '''

    lst = ''
    for l in labels:
        lst = lst + '{ startTime: %f, duration: %f, timezone: %f },' % (l[0], l[1], l[2])

    return start + lst[:-1] + end

def addLabelGroupMutationString(studyId, name, description):
    return '''
        mutation {
            addLabelGroupToStudy(studyId: "%s", name: "%s", description: "%s") {
                id
            }
        }
    ''' % (studyId, name, description)

def removeLabelGroupMutationString(groupId):
    return '''
        mutation {
            removeLabelGroupFromStudy(groupId: "%s")
        }
    ''' % (groupId)

def getViewedTimesString(studyId):
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