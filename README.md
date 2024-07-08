# aws-scripts

# Setup

If the `boto3` library isn't already installed on your machine, navgiate to this repo's directory and run `python3 -m venv .venv` to create a virtual environment. Then run `source .venv/bin/activate` to activate it. The terminal should update and have a `(.venv) -> ` prefix to indicate that you are currently in the virtual environment. Then install the `boto3` library with `pip3 install boto3`. Once you are done with using the script, you can exit out of the virtual environment by running `deactivate` in the terminal. The `boto3` installation will persist after you deactivate, so you can reactivate the virtual environment with the same command to run the script again without needing to install the `boto3` library again

Command to run script using `boto3`:

```python3 extract_emails_aws_cloudwatch.py```

Command to run script against a CSV file with Cloudwatch log events:

```python3 extract_emails_aws_cloudwatch.py -c <path to csv file>``` or ```python3 extract_emails_aws_cloudwatch.py --csv <path to csv file>```