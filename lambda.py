import json
import requests
import boto3
import os
from datetime import datetime

S3_BUCKET = os.getenv("S3_BUCKET")
S3_FILE_KEY = os.getenv("S3_FILE_KEY")
LEETCODE_URL = os.getenv("LEETCODE_URL")
DEFAULT_USERNAME = os.getenv("DEFAULT_USERNAME")

s3_client = boto3.client("s3")

GRAPHQL_QUERY = """
query getUserProfile($username: String!) {
  matchedUser(username: $username) {
    username
    profile {
      ranking
    }
    submitStats {
      acSubmissionNum {
        difficulty
        count
      }
    }
  }
}
"""


def lambda_handler(event, context):
    """Lambda function handler."""
    username = event.get("username", DEFAULT_USERNAME)
    log_data = fetch_leetcode_data(username)
    if log_data:
        update_s3_logs(log_data)
        return {"statusCode": 200, "body": json.dumps(log_data)}
    else:
        return {
            "statusCode": 500,
            "body": json.dumps("Failed to fetch or process data."),
        }


def fetch_leetcode_data(username):
    """Fetches user data from LeetCode API and formats it."""
    variables = {"username": username}
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        LEETCODE_URL,
        json={"query": GRAPHQL_QUERY, "variables": variables},
        headers=headers,
    )
    if response.status_code != 200:
        print(f"Failed to fetch data from LeetCode: {response.status_code}")
        return None
    data = response.json().get("data", {}).get("matchedUser", {})
    ranking = data.get("profile", {}).get("ranking", "N/A")
    submissions = data.get("submitStats", {}).get("acSubmissionNum", [])
    submission_stats = {sub["difficulty"]: sub["count"] for sub in submissions}
    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "ranking": ranking,
        "solved_problems": submission_stats,
    }


def update_s3_logs(log_data):
    """Fetches logs from S3, appends new data, keeps only the most recent 1500 data points, and uploads back to S3."""
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY)
        logs = json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        logs = []
    except json.JSONDecodeError:
        logs = []

    # Append new data
    logs.append(log_data)

    # Keep only the most recent 1500 data points
    if len(logs) > 1000:
        logs = logs[-1000:]

    # Upload the updated logs back to S3
    s3_client.put_object(Bucket=S3_BUCKET, Key=S3_FILE_KEY, Body=json.dumps(logs))

