import boto3
from dotenv import load_dotenv
import os
from collections import deque

def generate_sequence(input_num, limit):
    # Initialize a queue with the starting number
    queue = deque([input_num])

    # Store the sequence in a list
    result = []

    while queue:
        current = queue.popleft()

        # If the current number exceeds the limit, stop processing further
        if current > limit:
            continue

        # Add the current number to the result list
        result.append(current)

        # Generate the next numbers by appending digits 0-9
        for i in range(10):
            next_num = current * 10 + i
            if next_num <= limit:
                queue.append(next_num)

    return result


load_dotenv()

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# S3 bucket and folder (prefix) details
bucket_name = 'bucket-name'

# breadth-first search
# input_num = 9
# limit = 9999
# sequence = generate_sequence(input_num, limit)

# for directory in sequence:
folder_prefix = f'path/'  # Include trailing slash to target folder

# Local directory to store the downloaded content
local_dir = '/home/kmgkumar/Downloads/'

# Ensure the local directory exists
if not os.path.exists(local_dir):
    os.makedirs(local_dir)

# Pagination handling for listing objects in the specified folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
counter = 0
while True:
    if 'Contents' in response:
        for obj in response['Contents']:
            counter = counter + 1
            file_key = obj['Key']

            # Create the directory structure locally
            local_file_path = os.path.join(local_dir, os.path.relpath(file_key, folder_prefix))
            local_dir_path = os.path.dirname(local_file_path)
            if not os.path.exists(local_dir_path):
                os.makedirs(local_dir_path)

            # Skip if the object is a directory (ends with '/')
            if not file_key.endswith('/'):
                # Download the file
                s3.download_file(bucket_name, file_key, local_file_path)
                print(f'Total - {counter} Downloaded {file_key} to {local_file_path}')

    # Check if there are more objects to list (pagination)
    if response.get('IsTruncated'):  # More objects are available
        continuation_token = response.get('NextContinuationToken')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix, ContinuationToken=continuation_token)
    else:
        break
