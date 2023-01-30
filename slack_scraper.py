import os
import sys
import json

import slack
from dotenv import load_dotenv
import logging
import argparse

from utils import (
    get_conversation_history,
    process_conversation_history,
    post_fault_record,
    post_fault_record_updates,
    write_oldest_ts,
    get_oldest_ts,
)

load_dotenv()

logger = logging.getLogger("slack-fault-scrapper")
logger.setLevel(logging.DEBUG)

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
MESSAGE_LIMIT_PER_REQUEST = os.getenv("MESSAGE_LIMIT_PER_REQUEST")
FAULT_RECORD_POST_URL = os.getenv("FAULT_RECORD_POST_URL")
FAULT_RECORD_UPDATE_POST_URL = os.getenv("FAULT_RECORD_UPDATE_POST_URL")


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Fault Record Slack message scraper.",
        description="The program scrapes Slack messages for Fault records from different Slack channels.",
    )
    parser.add_argument("-c", "--channel_id", help="Slack channel ID")
    parser.add_argument("-o", "--oldest", default=0, help="Slack message oldest timestamp")
    parser.add_argument(
        "-cr",
        "--cronicle_run",
        choices=[0, 1],
        default=1,
        help="Cronicle run. Enabled by default. If you want to run without cronicle, please set to 0.",
    )
    args = parser.parse_args()
    if not "-c" or "--channel_id" in sys.argv and not args.cronicle_run:
        logger.error("\nSlack channel_id should be provided. Please, check input and try again.\n")
        parser.print_help()
        sys.exit(1)
    return args


def parse_cronicle_params():
    cronicle_params = json.load(sys.stdin)["params"]
    channel_id = cronicle_params.get("CHANNEL_ID")
    return channel_id


def main():
    args = parse_args()
    if not args.cronicle_run:
        channel_id = args.channel_id
        oldest_timestamp = args.oldest
    else:
        channel_id = parse_cronicle_params()
    oldest_timestamp = get_oldest_ts(f"{channel_id}.txt") or oldest_timestamp
    logger.info(
        f"Starting scraping Slack messages.\nChannel ID: {channel_id}.\tOldest message timestamp: {oldest_timestamp}."
    )
    client = slack.WebClient(token=SLACK_TOKEN)
    conversation_history = get_conversation_history(
        client=client, channel_id=channel_id, msg_limit=MESSAGE_LIMIT_PER_REQUEST, oldest=oldest_timestamp
    )
    logger.info("Processing conversation history.")
    processed_messages, oldest_message_ts = process_conversation_history(conversation_history, client, channel_id)
    logger.info("Posting messages to fault-record API.")
    for message in processed_messages:
        fault_id = post_fault_record(message, FAULT_RECORD_POST_URL)
        if message.get("updates"):
            post_fault_record_updates(message["updates"], fault_id, FAULT_RECORD_UPDATE_POST_URL)
    write_oldest_ts(f"{channel_id}.txt", oldest_message_ts)


if __name__ == "__main__":
    main()
