import csv
import json
import sys
import re
from collections import defaultdict
import argparse
import boto3

cloudwatch_logs_client = boto3.client('logs')

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def print_emails(email_reasons, from_csv=False):
    print("Bounced emails %s (total=%d):" % (("from CSV" if from_csv else "from Cloudwatch"), len(email_reasons)))
    for email, reasons in email_reasons.items():
        print(f"+ {email}:")
        for reason in reasons:
            print(f"  • {reason}")
        print()  # Add an empty line between email entries

def process_event(event, email_reasons):
    try:
        message = json.loads(event['message'], strict=False)
        bounce = message.get('bounce', {})
        bounced_recipients = bounce.get('bouncedRecipients', [])
        
        for recipient in bounced_recipients:
            email = recipient.get('emailAddress')
            reason = recipient.get('diagnosticCode', 'No diagnostic code provided')
            reason = clean_text(reason)
            if email:
                email_reasons[email].add(reason)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON in event: {e}", file=sys.stderr)
    except KeyError as e:
        print(f"Missing key in event: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error processing event: {e}", file=sys.stderr)

def process_log_events(log_events, email_reasons):
    for event in log_events:
        process_event(event, email_reasons)

def extract_bounced_emails_from_cloudwatch():
    log_streams = cloudwatch_logs_client.describe_log_streams(
        logGroupName='/aws/ses/email-verification-events',
        descending=True,
    )["logStreams"]

    email_reasons = defaultdict(set)
    
    for log_stream in log_streams:
        events = cloudwatch_logs_client.get_log_events(
            logGroupIdentifier=log_stream["arn"],
            logStreamName=log_stream["logStreamName"],
        )
        process_log_events(events["events"], email_reasons)

    print_emails(email_reasons)

def process_csv(file_path):
    email_reasons = defaultdict(set)

    try:
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                process_event(row, email_reasons)

        print_emails(email_reasons, from_csv=True)

    except FileNotFoundError:
        print(f"File not found: {file_path}", file=sys.stderr)
    except csv.Error as e:
        print(f"CSV error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)

def configure_args():
    parser = argparse.ArgumentParser(description='Extract email addresses and bounce reasons from a CSV file')
    parser.add_argument('-c', '--csv', help='Path to the CSV file')
    return parser

if __name__ == "__main__":
    parser = configure_args()
    command_args = parser.parse_args()
    
    if command_args.csv != None:
        process_csv(command_args.csv)
    else:
        # Note: because each log event has its own log stream, pulling from cloudwatch isn't reliable because it will only get the 50 most recent log events.
        # the queried log streams may have events that aren't bounce events, so until log streams are aggregated to be by day instead of per log event, this method is not reliable.
        #extract_bounced_emails_from_cloudwatch()
        print("this method is disabled for now until the log streams are aggregated to be by day instead of per log event so that we don't rack up an insane bill on queries.")