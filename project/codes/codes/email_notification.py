# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.message import EmailMessage
from email.headerregistry import Address
import logging
#from datetime import datetime
#from pytz import timezone

def send_non_service_email( color=None, reason=None, additional_details=None, config=None, email_subject = "Connection Error" ):
    email_message = EmailMessage()
    email_message['From'] = 'stellarbi.alerts@hp.com'#Address("KnowMe HealthCheck", "", "EAP-ITG@hpe.com")
    logging.info(f"Mail From: {email_message['From']}")
    #environment = str(config.get("Env", "environment"))
    #email_subject = email_subject + " - " + environment.split("-")[1]
    #additional_details["Environment"] = environment
    email_message['Subject'] = email_subject
    email_message['To'] = 'avinash.shetti@hp.com' #str(config.get('Send_Email_Notification', 'To_Dev'))
    email_message['X-Priority'] = '2'
    logging.info(f"Mail Subject: {email_message['Subject']}")
    email_message.set_type('text/html')
    #tz1 = timezone(str(config.get('Send_Email_Notification', 'timezone')))
    #alert_time = datetime.now(tz=tz1).strftime("%I:%M %p GMT %A, %B %d, %Y)
    #email_template_file = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"email_templates","non_service_email.htmltemplate"))
    #service_email_template = email_template_file.read()
    #email_template_file.close()
    #diagnostic_logs_html = get_table_html_from_diagnostic_logs(additional_details)
    #email_body_html = service_email_template.format(alert_time, reason, diagnostic_logs_html )
    email_message.set_content("This is a testing mail")
    #email_message.add_alternative(email_body_html, subtype="html")
    #logging.info( email_body_html )
    # Make a local copy of what we are going to send.
    # with open('outgoing.msg', 'wb') as f:
    #       f.write(bytes(email_message))
    with smtplib.SMTP(host='smtp3.hp.com',port=25) as s:
        s.send_message(email_message)
        print("Send Notification Email")
        #str(config.get('Send_Email_Notification', 'SMTP'))

send_non_service_email()
