import os
import re
import requests

import pandas as pd

from basic_interactions import TwilioClient


class ExtendedTwilioClient(TwilioClient):

    def phone_number_formatting(self):
        """Takes a dict of contacts and ensures correct number formatting, incl removing spaces & adding country code"""
        keys = self.contacts_dict.keys()
        for key in keys:
            # Remove spaces from the phone numbers
            self.contacts_dict[key] = re.sub(r'\s', '', self.contacts_dict[key])
            # Add country code
            self.contacts_dict[key] = re.sub(r'^0', '+44', self.contacts_dict[key])

    def read_pricing_csv(self, csv_fname):
        """ Extracts pricing data from supplied csv and logs a pandas df to the client for ref """
        self.call_prices = pd.read_csv(csv_fname)

    ### This needs sorting
    def pricing(self, max_price):
        """Identifies call charges from pricing_prefix directory from twilio,
        screening those with charges over an input threshold (max_price)"""
        pricedict = dict(zip(self.call_prices["prefixes"], self.call_prices["price"]))
        for i in range(len(self.contacts_dict)):
            if self.contacts_dict["call_charge"][i + 1] == '':
                helper_list = []
                for key in pricedict.keys():
                    if re.search(rf"^\+{key}\d+", self.df["phone_number"][i + 1]):
                        helper_list.append(pricedict[key])
                if len(helper_list) > 0:
                    self.df["call_charge"][i + 1] = float(max(helper_list))
                    if self.df["call_charge"][i + 1] > max_price:
                        self.df["state"][i + 1] = 'NOT eligible'

    def recording_log(self):
        """Retrieves the recording & call log from Twilio, merges on call id to keep all relevant info together"""
        try:
            recordings_df = pd.DataFrame(
                [calls.sid, calls.call_sid] for calls in self.client.recordings.list()
            )
            recordings_df.columns = ['recordingid', 'callid']

            calls_df = pd.DataFrame(
                [calls.to, calls.sid, calls.date_created, calls.direction, calls.status, calls.duration, calls.price]
                for
                calls
                in self.client.calls.list()
            )
            calls_df.columns = ['to', 'callid', 'datetime', 'direction', 'status', 'duration', 'cost']

            df_combined = pd.merge(recordings_df, calls_df, on='callid', how='left')

            self.combined_log = df_combined
        except:
            print("Issue retrieving logs from twilio")


    def twilio_audio_downloader(self, log):
        """Downloads audio from twilio recordings database. Takes a list of recordings to retrieve. If you want to
        download all recordings, substitute log for self.combined_log"""
        for i in log.index:
            try:
                url = 'https://api.twilio.com/2010-04-01/Accounts/' + self.account_sid + '/Recordings/' + \
                      log.loc[i, 'recordingid']

                # download the url contents in binary format
                r = requests.get(url)

                basepath = os.getcwd()
                in_wd = os.listdir()

                if 'temp' not in in_wd:
                    os.makedirs('temp')
                # open method to open a file on your system and write the contents
                with open(basepath + r'/temp/' + log.loc[i, 'recordingid'] + '.wav', 'wb') as f:
                    f.write(r.content)
            except:
                basepath = os.getcwd()
                print("Error encountered downloading ", log.loc[i, 'recordingid'], "from twilio to ",
                      (basepath + r'/temp/' + log.loc[i, 'recordingid'] + '.wav'))