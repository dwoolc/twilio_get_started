from twilio.rest import Client


class TwilioClient():
    def __init__(self, account_sid, auth_token, twilio_num):
        """create twilio client & setup basic info to use to make calls & texts"""
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.client = Client(account_sid, auth_token)
        self.twilio_num = twilio_num
        self.contacts_dict = dict()

    def populate_contacts(self, names, numbers):
        """takes a list of name and numbers and puts them in a dict for retrieval later"""
        for name, number in zip(names, numbers):
            self.contacts_dict[name] = number

    def make_a_call(self, name, url, record=False):
        """specify a name to call and a url hosting the call xml, as well as options instructions to record"""
        if name not in self.contacts_dict:
            print(f"contact details not defined for {name}. Please check and try again")
            return
        self.client.calls.create(
            record=record,
            url=url,
            to=self.contacts_dict[name],
            from_=self.twilio_num)

    def send_sms_or_whatsapp(self, name, notification):
        if name not in self.contacts_dict:
            print(f"contact details not defined for {name}. Please check and try again")
            return
        self.client.messages.create(body=notification,
                               from_=self.twilio_num,
                               to=self.contacts_dict[name])

