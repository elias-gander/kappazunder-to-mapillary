import imaplib
import email
from email.header import decode_header
import re


class ReadyToDownloadChecker:
    GUID_REGEX = re.compile(
        r"https://www\.wien\.gv\.at/ogdgeodata/download/"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.tar"
    )

    def __init__(self, email_address, app_password):
        self.email_address = email_address
        self.app_password = app_password
        self.imap = None
        self.emails = []
        self._connect()
        self._fetch_emails_by_subject()

    def _connect(self):
        if self.imap:
            try:
                self.imap.logout()
            except:
                pass
        self.imap = imaplib.IMAP4_SSL("imap.gmail.com")
        self.imap.login(self.email_address, self.app_password)
        self.imap.select("inbox")

    def _fetch_emails_by_subject(self):
        search_criterion = '(SUBJECT "Download-Link zu Ihren Geodaten")'
        status, messages = self.imap.search(None, search_criterion)
        if status != "OK":
            return
        for num in messages[0].split():
            self._fetch_email(num)

    def _fetch_email(self, num):
        status, msg_data = self.imap.fetch(num, "(RFC822)")
        if status != "OK":
            return
        msg = email.message_from_bytes(msg_data[0][1])
        subject = decode_header(msg.get("Subject") or "")[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode(errors="ignore")
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body += part.get_payload(decode=True).decode(errors="ignore")
                    except:
                        pass
        else:
            try:
                body = msg.get_payload(decode=True).decode(errors="ignore")
            except:
                pass
        self.emails.append((subject, body))

    def refresh(self):
        try:
            self._connect()
            search_criterion = f'(UNSEEN SUBJECT "Download-Link zu Ihren Geodaten")'
            status, messages = self.imap.search(None, search_criterion)
            if status == "OK":
                for num in messages[0].split():
                    self._fetch_email(num)
        except Exception as e:
            print("Error refreshing mail:", e)

    def get_ids(self):
        guids = set()
        for _, body in self.emails:
            matches = self.GUID_REGEX.findall(body)
            guids.update(matches)
        return list(guids)
