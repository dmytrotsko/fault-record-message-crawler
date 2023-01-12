import os
import sys

import slack
from dotenv import load_dotenv
import argparse

from utils import get_conversation_history, process_conversation_history, post_fault_record, post_fault_record_updates

load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
MESSAGE_LIMIT_PER_REQUEST = os.getenv("MESSAGE_LIMIT_PER_REQUEST")
FAULT_RECORD_POST_URL = os.getenv("FAULT_RECORD_POST_URL")
FAULT_RECORD_UPDATE_POST_URL = os.getenv("FAULT_RECORD_UPDATE_POST_URL")

parser = argparse.ArgumentParser(
    prog="Fault Record Slack message scraper.",
    description="The program scrapes Slack messages for Fault records from different Slack channels.",
)
parser.add_argument("-c", "--channel_id", help="Slack channel ID")
parser.add_argument("-o", "--oldest_ts", help="Only messages after this Unix timestamp will be included in results.")
args = parser.parse_args()

if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)

if not "-c" or "--channel_id" in sys.argv:
    print("\nSlack channel_id should be provided. Please, check input and try again.\n")
    parser.print_help()
    sys.exit(1)

channel_id = args.channel_id
oldest_timestamp = args.oldest_ts or 0


def main():
    client = slack.WebClient(token=SLACK_TOKEN)
    conversation_history = get_conversation_history(
        client=client,
        channel_id=channel_id,
        msg_limit=MESSAGE_LIMIT_PER_REQUEST,
        oldest=oldest_timestamp
    )
    processed_messages = process_conversation_history(conversation_history, client, channel_id)
    for message in processed_messages:
        fault_id = post_fault_record(message, FAULT_RECORD_POST_URL)
        if message.get("updates"):
            post_fault_record_updates(message["updates"], fault_id, FAULT_RECORD_UPDATE_POST_URL)


if __name__ == "__main__":
    main()
