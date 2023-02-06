#!/usr/bin/env python

import argparse
import datetime as dt
import glob
import json
import math
import os
import random
import sys
from tqdm import tqdm

import requests

from sklearn.metrics import balanced_accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score

def sample_poisson_distribution(mean):
    """
    Samples a non-negative integer from the Poisson distribution,
    often used to model counts.

    Uses inversion by sequential search.  See https://en.wikipedia.org/wiki/Poisson_distribution#Evaluating_the_Poisson_distribution
    """

    # count
    x = 0
    # inverse probability
    p = math.exp(-mean)
    # summed probability
    s = p
    # uniform random
    u = random.random()
    while u > s:
        x = x + 1
        p = p * mean / x
        s = s + p
    return x
    

def run_user_simulation(args):
    email_url = os.path.join(args.email_url, "email")
    mailbox_url = args.mailbox_url
    path = os.path.join(args.email_dir, "message_*.json")

    # we are going to loop through the emails
    files = glob.glob(path)
    file_idx = 0

    for _ in range(args.number_emails):
        with open(files[file_idx]) as fl:
            email_obj = json.load(fl)

            email_record = { "to" : email_obj["to_address"],
                             "from" : email_obj["from_address"],
                             "subject" : email_obj["subject"],
                             "body" : email_obj["body"] }
            
            response = requests.post(email_url, json=email_record)
                
            if not response.ok:
                print("Error: {} {}".format(response.status_code, response.reason), file=sys.stderr)
                sys.exit(1)

            json_response = response.json()

            if "email_id" not in json_response:
                print("Error: Expected response to contain an email_id field. Received: {}".format(str(response)), file=sys.stderr)
                sys.exit(1)
            
            email_id = json_response["email_id"]
            options = [
                ("{}/mailbox/email/{}".format(mailbox_url, email_id), "GET"),
                ("{}/mailbox/email/{}/folder".format(mailbox_url, email_id), "GET"),
                ("{}/mailbox/email/{}/labels".format(mailbox_url, email_id), "GET"),
                ("{}/mailbox/folder/Inbox".format(mailbox_url), "GET"),
                ("{}/mailbox/label/read".format(mailbox_url), "GET"),
                ("{}/mailbox/email/{}/folder/Archive".format(mailbox_url, email_id), "PUT"),
                ("{}/mailbox/email/{}/label/read".format(mailbox_url, email_id), "PUT"),
                ("{}/mailbox/email/{}/label/important".format(mailbox_url, email_id), "DELETE")
            ]

            n_events = sample_poisson_distribution(args.average_events_per_email)
            for i in range(n_events):
                chosen_url, chosen_request_type = random.choice(options)
                if chosen_request_type == "GET":
                    response = requests.get(chosen_url)
                elif chosen_request_type == "PUT":
                    response = requests.put(chosen_url)
                elif chosen_request_type == "DELETE":
                    response = requests.delete(chosen_url)
                else:
                    print("Unknown REST method {}".format(chosen_request_type), file=sys.stderr)
                    sys.exit(1)
                    
                if not response.ok:
                    print("Error: {} {}".format(response.status_code, response.reason), file=sys.stderr)
                    sys.exit(1)

            if email_obj["label"] == "spam":
                spam_url = "{}/mailbox/email/{}/label/spam".format(mailbox_url, email_id)
                response = requests.put(spam_url)

                if not response.ok:
                    print("Error: {} {}".format(response.status_code, response.reason), file=sys.stderr)
                    sys.exit(1)


        # move to next email, wraparound if needed
        file_idx += 1
        if file_idx >= len(files):
            file_idx = 0

    
def run_evaluation(args):
    url = os.path.join(args.classifier_url, "classify")
    path = os.path.join(args.email_dir, "message_*.json")

    # we are going to loop through the emails
    files = glob.glob(path)
    file_idx = 0
    
    true_labels = []
    pred_labels = []

    for _ in tqdm(range(args.number_emails)):
        with open(files[file_idx]) as fl:
            email_obj = json.load(fl)

            email_record = {
                "email" : {
                    "to" : email_obj["to_address"],
                    "from" : email_obj["from_address"],
                    "subject" : email_obj["subject"],
                    "body" : email_obj["body"]
                }
                }
            
            response = requests.get(url, json=email_record)
                
            if not response.ok:
                print("Error: {} {}".format(response.status_code, response.reason), file=sys.stderr)
                sys.exit(1)

            json_response = response.json()
                
            if email_obj["label"] == "ham":
                true_labels.append(1.0)
            elif email_obj["label"] == "spam":
                true_labels.append(0.0)
            else:
                print("Error! True label did not contain an expected value")

                
            if json_response["predicted_class"] == "ham":
                pred_labels.append(1.0)
            elif json_response["predicted_class"] == "spam":
                pred_labels.append(0.0)
            else:
                print("Error! Predicted label did contain an expected value")

            # move to next email, wraparound if needed
            file_idx += 1
            if file_idx >= len(files):
                file_idx = 0
                
    acc = balanced_accuracy_score(true_labels, pred_labels)
    prec = precision_score(true_labels, pred_labels)
    rec = recall_score(true_labels, pred_labels)

    print("Balanced Accuracy: {:.1%}".format(acc))
    print("Precision: {:.1%}".format(prec))
    print("Recall: {:.1%}".format(rec))
    print()

def parse_args():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="mode", required=True)
    
    eval_parser = subparsers.add_parser("evaluate-model")

    eval_parser.add_argument("--email-dir",
                             type=str,
                             required=True)

    eval_parser.add_argument("--classifier-url",
                             type=str,
                             required=True)

    eval_parser.add_argument("--number-emails",
                             type=int,
                             required=True)

    user_parser = subparsers.add_parser("simulate-user")

    user_parser.add_argument("--email-dir",
                             type=str,
                             required=True)

    user_parser.add_argument("--email-url",
                             type=str,
                             required=True)

    user_parser.add_argument("--mailbox-url",
                             type=str,
                             required=True)
    
    user_parser.add_argument("--number-emails",
                             type=int,
                             required=True)

    user_parser.add_argument("--average-events-per-email",
                             type=int,
                             required=True)

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    if args.mode == "evaluate-model":
        run_evaluation(args)
    elif args.mode == "simulate-user":
        run_user_simulation(args)
