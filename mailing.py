import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
from Azure_methods import Azure_Functions


class mail:
    def __init__(self, receiveraddr):
        self.email_user = "ateshtaru@gmail.com"
        self.email_password = "netflix12345"
        self.receiveraddr = receiveraddr
        self.Azurefunc = Azure_Functions("DefaultEndpointsProtocol=https;AccountName=trainingbatchfiles;AccountKey=JPHQiUP+0kPN4UlfW+jXZm9EaPg0nsSUd9MZMLnhpjmJZnO7OXiemYqM+vosRjXA8MLOTqV2fsDEAmz6tIjGFw==;EndpointSuffix=core.windows.net")

    def Gmail(self, subject, body, filename):
        try:

            msg = MIMEMultipart()
            msg['From'] = self.email_user
            msg['To'] = self.receiveraddr
            msg['Subject'] = subject

            msg.attach(MIMEText(body, 'plain'))

            filename = filename
            df = self.Azurefunc.readingcsvfile("badraw",filename)
            csv = df.to_csv(encoding="ISO-8859-1")
            attachment = csv

            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + filename)

            msg.attach(part)
            text = msg.as_string()
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_user, self.email_password)

            server.sendmail(self.email_user, self.receiveraddr, text)
            server.quit()

        except Exception as e:
            raise e