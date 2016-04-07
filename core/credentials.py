__author__ = 'io'

from oauth2client.service_account import ServiceAccountCredentials

# Authenticate with Google, see http://gspread.readthedocs.org/en/latest/oauth2.html
def getGoogleCredentials():
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('core/e.json', scope)
    return credentials