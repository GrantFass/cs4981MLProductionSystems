#!/usr/bin/env python

import argparse
import glob
import json
import os

import random

def parse_book(flname):
    paragraphs = []
    paragraph = ""
    
    with open(flname) as fl:
        for ln in fl:
            ln = ln.strip()
            if len(ln) == 0:
                if len(paragraph) > 300:
                    paragraphs.append(paragraph)
                paragraph = ""
            else:
                paragraph += " " + ln

        if len(ln) == 0:
            if len(paragraph) > 300:
                paragraphs.append(paragraph)
                
    return paragraphs

def read_books(books_dir):
    books_path = os.path.join(books_dir, "*.txt")
    
    paragraphs = []
    for flname in glob.iglob(books_path):
        paragraphs.extend(parse_book(flname))

    return paragraphs

def substitute_email_text(input_dir, output_dir, spam_replacement_paragraphs, ham_replacement_paragraphs):
    email_path = os.path.join(input_dir, "message_*.json")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for email_fl in glob.iglob(email_path):
        basename = os.path.basename(email_fl)
        with open(email_fl, "rt") as infl:
            email_obj = json.load(infl)
            
            if email_obj["label"] == "spam":
                email_obj["body"] = random.choice(spam_replacement_paragraphs)
            else:
                email_obj["body"] = random.choice(ham_replacement_paragraphs)
                
        target_fl = os.path.join(output_dir, basename)
        with open(target_fl, "wt") as outfl:
            json.dump(email_obj, outfl)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input-email-dir",
                        type=str,
                        required=True)

    parser.add_argument("--spam-books-dir",
                        type=str,
                        required=True)
                        
    parser.add_argument("--ham-books-dir",
                        type=str,
                        required=True)

    parser.add_argument("--output-email-dir",
                        type=str,
                        required=True)

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    spam_books = read_books(args.spam_books_dir)
    ham_books = read_books(args.ham_books_dir)
    
    substitute_email_text(args.input_email_dir,
                            args.output_email_dir,
                            spam_books,
                            ham_books)

    

    

