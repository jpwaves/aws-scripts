import csv
import json
import sys
import re
from collections import defaultdict
import argparse
import boto3
from enum import Enum

cloudwatch_logs_client = boto3.client('logs')

class EventType(Enum):
    BOUNCE = 'Bounce'
    DELIVERY = 'Delivery'
    SEND = 'Send'

class CSVEmailsProcessor:
    def __default_value(self):
        return {
            "type": None,
            "reasons": set(),
            "occurred_at": None
        }

    def __init__(self, csv_file_path):
        self.processed_emails = defaultdict(self.__default_value)
        self.csv_file_path = csv_file_path

    def print_emails(self, event_type=None, verbose=False, sort_by_date=False, sort_by_email=False):
        emails_to_print = self.processed_emails
        if event_type:
            emails_to_print = {email: data for email, data in self.processed_emails.items() if data["type"] == event_type}

        if sort_by_date:
            emails_to_print = {k: v for k, v in sorted(emails_to_print.items(), key=lambda item: item[1]["occurred_at"])}

        if sort_by_email:
            emails_to_print = {k: v for k, v in sorted(emails_to_print.items())}

        print(self.__build_print_header(event_type, len(emails_to_print)))
        for email, data in emails_to_print.items():
            if verbose:
                print(f"+ {email}:")
                print(f"  • Type: {data['type']}")
                print(f"  • Occurred at: {data['occurred_at']}")
                if data["type"] == EventType.BOUNCE:
                    for reason in data["reasons"]:
                        print(f"  • {reason}")
                print()
            else:
                print(email)

    def process(self):
        try:
            with open(self.csv_file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    self.process_event(row)
            
            return self
        except FileNotFoundError:
            print(f"File not found: {self.csv_file_path}", file=sys.stderr)
        except csv.Error as e:
            print(f"CSV error: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Unexpected error: {e}", file=sys.stderr)

    def process_event(self, event):
        try:
            message = json.loads(event['message'], strict=False)
            eventType = message.get('eventType')

            if eventType == 'Bounce':
                self.__process_bounce(message)
            elif eventType == 'Delivery':
                self.__process_delivery(message)
            elif eventType == 'Send':
                self.__process_send(message)
            else:
                print(f"Skipping event with unknown eventType: {eventType}", file=sys.stderr)
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON in event: {e}", file=sys.stderr)
        except KeyError as e:
            print(f"Missing key in event: {e}", file=sys.stderr)
        except Exception as e:
            print(f"Unexpected error processing event: {e}", file=sys.stderr)

    def __build_print_header(self, event_type, total_emails):
        divider = ("-" * 50) + "\n"
        if event_type == EventType.BOUNCE:
            return divider + "Bounced emails (total=%d):" % total_emails
        elif event_type == EventType.DELIVERY:
            return divider + "Delivered emails (total=%d):" % total_emails
        elif event_type == EventType.SEND:
            return divider + "Sent emails (total=%d):" % total_emails
        else:
            return divider + "Processed Emails (total=%d):" % total_emails

    def __clean_text(text):
        return re.sub(r'\s+', ' ', text).strip()

    def __process_bounce(self, event):
        bounce = event.get('bounce', {})
        bounced_recipients = bounce.get('bouncedRecipients', [])
        
        for recipient in bounced_recipients:
            email = recipient.get('emailAddress')
            reason = recipient.get('diagnosticCode', 'No diagnostic code provided')
            occurred_at = bounce.get('timestamp')
            reason = self.__clean_text(reason)
            if email:
                self.processed_emails[email]["type"] = EventType.BOUNCE
                self.processed_emails[email]["reasons"].add(reason)
                self.processed_emails[email]["occurred_at"] = occurred_at

    def __process_delivery(self, event):
        delivery = event.get('delivery', {})
        delivery_recipients = delivery.get('recipients', [])

        for recipient in delivery_recipients:
            delivered_at = delivery.get('timestamp')
            self.processed_emails[recipient]["type"] = EventType.DELIVERY
            self.processed_emails[recipient]["occurred_at"] = delivered_at

    def __process_send(self, event):
        send = event.get('mail', {})
        destination = send.get('destination', [])

        for recipient in destination:
            sent_at = send.get('timestamp')
            self.processed_emails[recipient]["type"] = EventType.SEND
            self.processed_emails[recipient]["occurred_at"] = sent_at

# def extract_bounced_emails_from_cloudwatch():
#     log_streams = cloudwatch_logs_client.describe_log_streams(
#         logGroupName='/aws/ses/email-verification-events',
#         descending=True,
#     )["logStreams"]

#     email_reasons = defaultdict(set)
    
#     for log_stream in log_streams:
#         events = cloudwatch_logs_client.get_log_events(
#             logGroupIdentifier=log_stream["arn"],
#             logStreamName=log_stream["logStreamName"],
#         )
#         process_log_events(events["events"], email_reasons)

#     print_emails(email_reasons)

def configure_args():
    parser = argparse.ArgumentParser(description='Extract email addresses and bounce reasons from a CSV file')
    parser.add_argument('-c', '--csv', help='Path to the CSV file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Prints more detailed information about each email')
    parser.add_argument('-d', '--sort-by-date', action='store_true', help='Sorts the emails by the date they occurred')
    parser.add_argument('-e', '--sort-by-email', action='store_true', help='Sorts the emails by the email name in alphabetical order')
    return parser

if __name__ == "__main__":
    parser = configure_args()
    command_args = parser.parse_args()
    
    if command_args.sort_by_date and command_args.sort_by_email:
        print("You can't sort by date and email at the same time. Please choose one or the other.")
        sys.exit(1)

    if command_args.csv != None:
        CSVEmailsProcessor(command_args.csv).process().print_emails(verbose=command_args.verbose, sort_by_date=command_args.sort_by_date, sort_by_email=command_args.sort_by_email)
    else:
        # Note: because each log event has its own log stream, pulling from cloudwatch isn't reliable because it will only get the 50 most recent log events.
        # the queried log streams may have events that aren't bounce events, so until log streams are aggregated to be by day instead of per log event, this method is not reliable.
        #extract_bounced_emails_from_cloudwatch()
        print("this method is disabled for now until the log streams are aggregated to be by day instead of per log event so that we don't rack up an insane bill on queries.")