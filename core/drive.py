import httplib2
import os

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive.metadata.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'

COLLECTION_ID = '0B4NMjhoRDH9vfnFwRE9PN1JMdkQ2d2tHbzNiaDVsTXVWaGRmTFh1LWpTNGdZd2dNNDJvUG8'
PROCESSING_ID = '0B4NMjhoRDH9vflZzbF9RX3lDZWNJQVpzZWd3TjZmZDNkUVg5Sk42cFJwNjFkTF84b1RHV00'
DISTRIBUTION_ID = '0B4NMjhoRDH9vfkQ2SU9NdEtsam9uWmR3TlliQ3ItQS1sWkxOY2w1a2JMZ2FvaTFlbWY2QW8'

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
                                   'drive-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    page_token = None
    while True:
        # response = service.files().list(q="name='FoodWorks' and mimeType='application/vnd.google-apps.folder'",
        #                                      fields='nextPageToken, files(id, name)',
        #                                      pageToken=page_token).execute()
        response = service.files().list(q="'0B4NMjhoRDH9vfnFwRE9PN1JMdkQ2d2tHbzNiaDVsTXVWaGRmTFh1LWpTNGdZd2dNNDJvUG8' in parents and mimeType='application/vnd.google-apps.folder'",
                                 fields='nextPageToken, files(id, name)',
                                 pageToken=page_token).execute()


        for file in response.get('files', []):
            # Process change
            print 'Found file: %s (%s)' % (file.get('name'), file.get('id'))
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break;

if __name__ == '__main__':
    main()


