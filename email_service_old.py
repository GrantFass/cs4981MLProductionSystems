from flask import Flask, request
import structlog

app = Flask(__name__)


# GET /mailbox/email/<email_id:int>
# Returns a JSON object with the key "email" and an associated value of a String containing the entire email text
@app.route('/mailbox/email/')
def get_email():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# GET /mailbox/email/<email_id:int>/folder
# Get the folder containing the given email.  Examples of folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/')
def get_folder():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# GET /mailbox/email/<email_id:int>/labels
# Returns a JSON object with the fields "email_id" and "labels".  The value for labels is a list of strings.  Valid labels include "spam", "read", and "important".  No label may be repeated.
@app.route('/mailbox/email/')
def get_json():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)


# GET /mailbox/folder/<folder:str>
# Lists the emails in a given folder.  Returns a list of email_ids.
@app.route('/mailbox/email/')
def get_emails():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# GET /mailbox/labels/<label:str>
# List emails with the given label.  Returns a list of email_ids.
@app.route('/mailbox/email/')
def get_emails_with_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# PUT /mailbox/email/<email_id:int>/folder/<folder:str>
# Moves email to the given folder.  Folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/')
def put_email():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# PUT /mailbox/email/<email_id:int>/label/<label:str>
# Mark the given email with the given label. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/')
def put_email_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# DELETE /mailbox/email/<email_id:int>/label/<label:str>
# Remove the given label from the given email. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/')
def delete_email_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)


if __name__ == '__main__':
    with open("log_file.json", "wt", encoding="utf-8") as log_fl:
        structlog.configure(
        processors=[structlog.processors.TimeStamp(fmt="iso"),
        structlog.processors.JSONRenderer()],
        logger_factory=structlog.WriteLoggerFactory(file=log_fl))
