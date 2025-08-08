import emails
import logging


def send_mail_datagouv(
    email_user: str,
    email_password: str,
    email_recipients: list,
    subject: str,
    message: str,
    attachment_path: str = None,
):
    # supports_lineage = True

    # template_fields = (
    #     "email_user",
    #     "email_password",
    #     "email_recipients",
    #     "subject",
    #     "message",
    #     "attachment_path",
    # )
    if not email_user or not email_password:
        raise ValueError("Not enough information to send message")

    sender = email_user
    message = message
    subject = subject
    message = emails.html(html="<p>%s</p>" % message, subject=subject, mail_from=sender)

    smtp = {
        "host": "mail.data.gouv.fr",
        "port": 587,
        "tls": True,
        "user": email_user,
        "password": email_password,
        "timeout": 60,
    }
    if attachment_path:
        message.attach(
            data=open(attachment_path),
            filename=attachment_path.split("/")[-1],
        )

    retry = True
    tries = 0
    while retry:
        r = message.send(to=email_recipients, smtp=smtp)
        logging.info(r)
        tries = tries + 1
        if (r.status_code == 250) | (tries == 5):
            retry = False
    assert r.status_code == 250
