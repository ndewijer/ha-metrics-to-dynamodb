# python-ha-metrics-to-dynamodb

This python script will run through an home-assistant database and grabs the defined metrics, then puts them in a AWS DynamoDB table.

## Functionality
1. Connects to AWS and SQLite or MariaDB database to also test functionality
2. Gets metadata_id based on defined sensors from the database
3. Gets timestamp from SSM Parameter store.
4. Polls all metrics past the defined timestamp from the database
5. Puts the records into a defined DynamoDB table and reports its progress.
6. Saves final timestamp in SSM Parameter store. 

## Requirements
   * SSM Parameter to read from and store the timestamp of where to pick-up from
   * DynamoDB to write records too with metadata_id as the hash and timestamp as the range key.
   * IAM credentials to do the above.

