import httplib2
import os
import json

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from collections import defaultdict

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

COLLECTION_ID = '0B4NMjhoRDH9vfnFwRE9PN1JMdkQ2d2tHbzNiaDVsTXVWaGRmTFh1LWpTNGdZd2dNNDJvUG8'
PROCESSING_ID = '0B4NMjhoRDH9vfkQ2SU9NdEtsam9uWmR3TlliQ3ItQS1sWkxOY2w1a2JMZ2FvaTFlbWY2QW8'
DISTRIBUTION_ID = '0B4NMjhoRDH9vflZzbF9RX3lDZWNJQVpzZWd3TjZmZDNkUVg5Sk42cFJwNjFkTF84b1RHV00'

SHEETS = {
    'collection' : COLLECTION_ID,
    'processing' : PROCESSING_ID,
    'distribution' :DISTRIBUTION_ID,
}

MIMETYPE_FOLDER = 'application/vnd.google-apps.folder'
MIMETYPE_SHEETS = 'application/vnd.google-apps.spreadsheet'

structure = {}

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'foodworks-drive.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def list_folders(service, parent_id):
    page_token = None
    while True:
        response = service.files().list(q="'{}' in parents and mimeType='{}' and trashed=false".format(parent_id, MIMETYPE_FOLDER),
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()

        for file in response.get('files', []):
            # Process change
            result = {
                "name" : file.get('name'),
                "id" : file.get('id')
            }
            # print 'Fx: {name} ({id})'.format(**result)
            yield result
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break;

def list_files(service, parent_id, files):
    page_token = None
    while True:
        response = service.files().list(q="'{}' in parents and trashed=false".format(parent_id, MIMETYPE_FOLDER),
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=page_token).execute()

        for file in response.get('files', []):
            # Process change
            result = {
                "name" : file.get('name'),
                "id" : file.get('id')
            }
            print 'SS: {name}'.format(**result)
            files.append(result)
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break;

def deep_list_folders(service, parent_id, structure, path, levels):
    results = list_folders(service, parent_id)
    for result in results:
        if len(path) > levels + 1:
            path = []
        try:
            path[levels] = result['name']
        except IndexError:
            path.append(result['name'])
        if levels == 1:
            files = []
            structure[path[0]][path[1]] = files
            
            list_files(service,result['id'], files)

        deep_list_folders(service, result['id'], structure, path, levels+1)        

def generate_structure():

    try:
        # Delete `structure.json` if you want to refresh the export
        with open('structure.json') as fp:
            structure = json.load(fp)
        
    except :

        credentials = get_credentials()
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('drive', 'v3', http=http)
        structure = {}

        for stage, sheet_id in SHEETS.iteritems():
            structure[stage] = defaultdict(dict)
            deep_list_folders(service, sheet_id, structure[stage], [], 0)
        
        with open('structure.json', 'w') as fp:
            json.dump(structure, fp)

    return structure

if __name__ == '__main__':
    print generate_structure()