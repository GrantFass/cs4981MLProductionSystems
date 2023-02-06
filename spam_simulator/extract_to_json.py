#!/usr/bin/env python

import argparse

from email.parser import Parser
import glob
import json
import os
from bs4 import BeautifulSoup

def _parse_message(message):
    body = ""

    if message.is_multipart():
        for part in message.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get('Content-Disposition'))

            # skip any attachments
            if ctype == 'text/html' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)
                break
            elif ctype == 'text/txt' and 'attachment' not in cdispo:
                body = part.get_payload(decode=True)
                break
    # not multipart - i.e. plain text, no attachments, keeping fingers crossed
    else:
        body = message.get_payload(decode=True)

    return message["To"], message["From"], message["Subject"], BeautifulSoup(body, 'html.parser').get_text()

def stream_email(input_dir):
    email_parser = Parser()

    index_flname = os.path.join(input_dir, "full/index")
    with open(index_flname) as index_fl:
        for idx, ln in enumerate(index_fl):
            category, email_fl_suffix = ln.strip().split()

            # strip .. prefix from path
            email_flname = "trec07p"  + email_fl_suffix[2:]
            with open(email_flname, "rt") as email_fl:
                try:
                    message = email_parser.parse(email_fl)
                    to, from_, subject, body = _parse_message(message)

                    yield (category, to, from_, subject, body)
                except:
                    pass

def convert_emails(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    streamer = stream_email(input_dir)
    next_output = 1

    for i, (category, to, from_, subject, body) in enumerate(streamer):
        if i == next_output:
            print("Processing email", (i+1))
            next_output *= 2

        output_path = os.path.join(output_dir, "message_%05d.json" % i)
        with open(output_path, "w") as fl:
            record = { "label" : category,
                       "to_address" : to,
                       "from_address" : from_,
                       "subject" : subject,
                       "body" : body }
            json.dump(record, fl)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--trec07p-dir",
                        type=str,
                        required=True)

    parser.add_argument("--output-dir",
                        type=str,
                        required=True)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    convert_emails(args.trec07p_dir,
                   args.output_dir)


