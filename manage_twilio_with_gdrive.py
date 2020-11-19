from __future__ import print_function
import pickle
import pandas as pd
import os.path
import shutil
import requests
from pprint import pprint

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
from twilio.rest import Client

import config

### GDrive interactions require file & folder keys to be known in advance.
### For more information see our GDrive interactions repository for base code and outline of function
upload_dict = {
    'to_process' : 'your_gdrive_folder_id_string_here',
    'clipping_csv' : 'your_gdrive_file_id_string_here',
    'call_log' : 'your_gdrive_file_id_string_here',
}


class gdrive_twilio_log:
    def __init__(self,
                 account_sid,
                 auth_token,
                 SCOPES,
                 upload_dict,
                 range_name='Sheet1',
                 ):
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.client = Client(self.account_sid, self.auth_token)
        self.SCOPES = SCOPES
        self.upload_dict = upload_dict
        self.existing_call_log = self.upload_dict['call_log']
        self.clipping_nam = self.upload_dict['clipping_csv']
        self.range_name = range_name
        self.existing_call_log_df = self.read_varied_gsheet(self.existing_call_log)
        self.clippingcsv_df = self.read_varied_gsheet(self.clipping_nam)


    def gsheet_api(self):
        """ Retrieves required information for GSheets API based on pickled credentials. Returns the required credentials
        for the API """
        try:
            creds = None
            if os.path.exists('token.pickle'):
                with open('token.pickle', 'rb') as token:
                    creds = pickle.load(token)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', self.SCOPES)
                    creds = flow.run_local_server(port=0)

                # Save the credentials for the next run
                with open('token.pickle', 'wb') as token:
                    pickle.dump(creds, token)
            return creds
        except:
            print("Unable to retrieve gsheet credentials using gsheet_api")

    def read_varied_gsheet(self, spread_id):
        """ Extracts contact data from the GSheets and returns a pandas df """
        try:
            service = build('sheets', 'v4', credentials=self.gsheet_api())
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spread_id,
                                        range=self.range_name).execute()
            values = result.get('values')
            df = pd.DataFrame(values)
            df.columns = df.iloc[0]
            df = df.drop(df.index[0])
            return df
        except:
            print("Error with read_gsheet with inputs", spread_id, "and ", self.range_name)

    def recording_log(self):
        """Retrieves the recording log from Twilio"""
        try:
            df_recordings = pd.DataFrame(
                [calls.sid, calls.call_sid] for calls in self.client.recordings.list()
            )
            df_recordings.columns = ['recordingid', 'callid']

            df_calls = pd.DataFrame(
                [calls.to, calls.sid, calls.date_created, calls.direction, calls.status, calls.duration, calls.price]
                for
                calls
                in self.client.calls.list()
            )
            df_calls.columns = ['to', 'callid', 'datetime', 'direction', 'status', 'duration', 'cost']

            df_combined = pd.merge(df_recordings, df_calls, on='callid', how='left')

            self.combined_log = df_combined
        except:
            print("Issue retrieving recordings log from twilio")

    def new_files(self):
        """Returns a list of new files"""
        # If the callid is not in the existing log, append the log.
        # isin returns a boolean Series, so to select rows whose value is not in some_values, negate the boolean Series using ~:
        self.existing_call_log_df = self.existing_call_log_df.append(self.combined_log.loc[~self.combined_log['callid'].isin(self.existing_call_log_df['callid'])],
                                                                     ignore_index=True, sort=False)
        print(self.clippingcsv_df)
        # can we combine this with the above line?
        self.existing_call_log_df["processed"] = self.existing_call_log_df["processed"].fillna('')

        print(self.clippingcsv_df)
        # check whether each unprocessed file is in clippingcsv
        ready_to_go = (self.existing_call_log_df['recordingid'].isin(self.clippingcsv_df['File_ID'])) & (self.existing_call_log_df['processed'] == '')

        print(ready_to_go)
        needs_clipping = self.existing_call_log_df[ready_to_go]

        return needs_clipping

    def twilio_audio_downloader(self, screener):
        """Downloads audio from twilio recordings database. Takes a list of recordings to retrieve"""
        for i in screener.index:
            try:
                url = 'https://api.twilio.com/2010-04-01/Accounts/' + self.account_sid + '/Recordings/' + \
                      screener.loc[i, 'recordingid']

                # download the url contents in binary format
                r = requests.get(url)

                basepath = os.getcwd()
                in_wd = os.listdir()

                if 'temp' not in in_wd:
                    os.makedirs('temp')
                # open method to open a file on your system and write the contents
                with open(basepath + r'/temp/' + screener.loc[i, 'recordingid'] + '.wav', 'wb') as f:
                    f.write(r.content)
            except:
                basepath = os.getcwd()
                print("Error encountered downloading ", screener.loc[i, 'recordingid'], "from twilio to ",
                      (basepath + r'/temp/' + screener.loc[i, 'recordingid'] + '.wav'))

    def upload_audio(self):
        """Used to upload whole recordings"""
        service = build('drive', 'v3', credentials=self.gsheet_api())
        folder_id = upload_dict['to_process']
        basepath = os.getcwd()
        basepath = basepath + "\\" + 'temp' + "\\"
        with os.scandir(basepath) as entries:
            for entry in entries:
                if entry.is_file():
                    print(entry.name)
                file_metadata = {
                    'name': entry.name,
                    'parents': [folder_id]
                }
                media = MediaFileUpload(basepath + '\\' + entry.name,
                                        resumable=True)
                file = service.files().create(body=file_metadata,
                                              media_body=media,
                                              fields='id').execute()

    def initial_download_from_twilio_upload_to_gdrive(self):
        """Function which transfers recordings from twilio to GDrive. Downloaded to local working directory before upload
            to GDrive."""
        new_additions = self.combined_log.loc[~self.combined_log['callid'].isin(self.existing_call_log_df['callid'])]
        new_additions['processed'] = ''
        print(new_additions)
        self.existing_call_log_df = self.existing_call_log_df.append(new_additions,
                                                                     ignore_index=True)
        for i in self.existing_call_log_df.index:
            self.existing_call_log_df.loc[i, 'datetime'] = str(self.existing_call_log_df.loc[i, 'datetime'])
        self.write_calllog_gsheet('Sheet1!') # need to amend super write function to be able to specify df
        self.twilio_audio_downloader(new_additions)
        self.upload_audio()
        shutil.rmtree('temp')

    def format_finder(self, row_start=2):
        """Takes an existing dataframe and returns formatting options for plotting to GSheet"""
        try:
            shape = self.existing_call_log_df.shape
            row_end = str(row_start + (shape[0]) + 1)
            self.format_range = f"A{row_start}:Z{row_end}"
            return self
        except:
            print("Error resolving format for dataframe: ", self.existing_call_log_df)

    def write_calllog_gsheet(self, sheet_name, row_start=2):
        """ Function to write updated df to GSheet at end of main script cycle."""
        service = build('sheets', 'v4', credentials=self.gsheet_api())
        self.format_finder(row_start)
        range = sheet_name + self.format_range
        data = [{'range': range,
                 'values': self.existing_call_log_df.values.tolist()}]
        batch_update_values_request_body = {
            'value_input_option': 'RAW',
            'data': data}
        request = service.spreadsheets().values().batchUpdate(spreadsheetId=self.incumbent_nam,
                                                              body=batch_update_values_request_body)
        response = request.execute()
        pprint(response)