import csv
import json
import sys
import re
from collections import defaultdict

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def process_csv(file_path):
    email_reasons = defaultdict(set)

    try:
        with open(file_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    message = json.loads(row['message'], strict=False)
                    bounce = message.get('bounce', {})
                    bounced_recipients = bounce.get('bouncedRecipients', [])
                    
                    for recipient in bounced_recipients:
                        email = recipient.get('emailAddress')
                        reason = recipient.get('diagnosticCode', 'No diagnostic code provided')
                        reason = clean_text(reason)
                        if email:
                            email_reasons[email].add(reason)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON in row: {e}", file=sys.stderr)
                except KeyError as e:
                    print(f"Missing key in row: {e}", file=sys.stderr)
                except Exception as e:
                    print(f"Unexpected error processing row: {e}", file=sys.stderr)

        # Print grouped results
        print("Bounced emails (total: %d):" % len(email_reasons))
        for email, reasons in email_reasons.items():
            print(f"+ {email}:")
            for reason in reasons:
                print(f"  • {reason}")
            print()  # Add an empty line between email entries

    except FileNotFoundError:
        print(f"File not found: {file_path}", file=sys.stderr)
    except csv.Error as e:
        print(f"CSV error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <csv_file_path>", file=sys.stderr)
        sys.exit(1)
    
    csv_file_path = sys.argv[1]
    process_csv(csv_file_path)