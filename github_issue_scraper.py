import os
import argparse
from dotenv import load_dotenv
from github_api import init_api, action_collect
from utils import post_fault_record, post_fault_record_updates, update_github_comments, read_from_file

load_dotenv()

# The number of requests inbetween logging API quota
API_LOGGING_FREQUENCY = os.getenv("API_LOGGING_FREQUENCY", 100)

# Size of a page to be used in paginated requests (maximum of 100 is used)
API_PAGE_SIZE = os.getenv("API_PAGE_SIZE", 100)

# The number of remaining requests at which the script will temporarily halt
API_QUOTA_THRESHOLD = os.getenv("API_QUOTA_THRESHOLD", 10)

# Timeout for API requests, used to prevent a hanging issue in ghapi
API_REQUEST_TIMEOUT = os.getenv("API_REQUEST_TIMEOUT", 100)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
FAULT_RECORD_API_URL = os.getenv("FAULT_RECORD_API_URL")
FAULT_RECORD_POST_URL = os.path.join(FAULT_RECORD_API_URL, "api/v1/faults")
FAULT_RECORD_UPDATE_POST_URL = os.path.join(FAULT_RECORD_API_URL, "api/v1/updates")
UPDATE_REPLIES_FOR_DAYS = os.getenv("UPDATE_REPLIES_FOR_DAYS")


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Fault Record Slack message scraper.",
        description="The program scrapes Slack messages for Fault records from different Slack channels.",
    )
    parser.add_argument("-o", "--owner", help="GitHub repository owner")
    parser.add_argument("-r", "--repo", help="GitHub repository name")
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    owner = args.owner
    repo = args.repo
    ghapi = init_api(GITHUB_TOKEN, API_LOGGING_FREQUENCY, API_QUOTA_THRESHOLD)
    # update_github_comments(FAULT_RECORD_API_URL, int(UPDATE_REPLIES_FOR_DAYS), ghapi, owner, repo, FAULT_RECORD_UPDATE_POST_URL)
    resume_page = read_from_file(f"{owner}-{repo}.txt")
    issues = action_collect(ghapi, owner, repo, API_PAGE_SIZE, FAULT_RECORD_API_URL, resume_page)
    for issue in issues:
        fault_id = post_fault_record(issue, FAULT_RECORD_POST_URL)
        if issue["comments"]:
            post_fault_record_updates(issue["comments"], fault_id, FAULT_RECORD_UPDATE_POST_URL)


if __name__ == "__main__":
    main()
