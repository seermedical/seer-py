

import seerpy
import requests


client = seerpy.SeerConnect()

party_id = ""
studies = client.get_studies_dataframe(party_id=party_id)

documents_metadata = client.get_documents_for_studies_dataframe(studies['id'].unique().tolist())

for doc in range(len(documents_metadata)):
    url = documents_metadata.loc[doc, 'downloadFileUrl']
    study_id = documents_metadata.loc[doc, 'id']
    
    with open(study_id + '.pdf', 'wb') as f:
        f.write(requests.get(url).content)
    
    break ## download just the first one in this example