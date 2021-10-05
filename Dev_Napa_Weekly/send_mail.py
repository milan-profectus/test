# In order for the connection to work "allow less secure apps" in the gmail account must be turned on
# It may be neccessary to activate "Display Unlock Captcha" in the gmail account
import ssl
import smtplib

def send_mail(receivers, message_subject, message_body):
    port = 465 # for ssl
    gmail_user = 'aap.scraping.alters@gmail.com'
    gmail_password = 'zNrp$mYc@5dX'
    message_txt = 'Subject: {}\n\n{}'.format(message_subject, message_body)

    with smtplib.SMTP_SSL('smtp.gmail.com', port) as server:
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.sendmail(gmail_user, receivers, message_txt)
        server.close()

if __name__ == '__main__':
    # Set the email addresses in a list, then run the send_mail.py script to manually send a test email
    email_addresses = ['adam.herman@advance-auto.com','dipak.hawale@profectussolutions.co']
    msg_subject = 'test message - subject'
    msg_body = 'test message - body'

    print(email_addresses)
    print(msg_subject)
    print(msg_body)

    send_mail(email_addresses, msg_subject, msg_body)