__author__ = 'io'

import json
from oauth2client.client import SignedJwtAssertionCredentials

# Authenticate with Google, see http://gspread.readthedocs.org/en/latest/oauth2.html
def getGoogleCredentials():
    json_key = json.load(open('e.json'))
    scope = ['https://spreadsheets.google.com/feeds']
    return SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
