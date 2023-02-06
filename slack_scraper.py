import argparse
from logzero import logger
import os
import sys

import slack
from dotenv import load_dotenv

from utils import (
    get_conversation_history,
    read_from_file,
    post_fault_record,
    post_fault_record_updates,
    process_conversation_history,
    write_to_file,
    update_slack_replies
)

load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
MESSAGE_LIMIT_PER_REQUEST = os.getenv("MESSAGE_LIMIT_PER_REQUEST")
FAULT_RECORD_API_URL = os.getenv("FAULT_RECORD_API_URL")
FAULT_RECORD_POST_URL = os.path.join(FAULT_RECORD_API_URL, "api/v1/faults")
FAULT_RECORD_UPDATE_POST_URL = os.path.join(FAULT_RECORD_API_URL, "api/v1/updates")
UPDATE_REPLIES_FOR_DAYS = os.getenv("UPDATE_REPLIES_FOR_DAYS")


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Fault Record Slack message scraper.",
        description="The program scrapes Slack messages for Fault records from different Slack channels.",
    )
    parser.add_argument("-c", "--channel_id", help="Slack channel ID")
    parser.add_argument("-o", "--oldest", default=0, help="Slack message oldest timestamp")
    args = parser.parse_args()
    if not "-c" or "--channel_id" in sys.argv and not args.cronicle_run:
        logger.error("\nSlack channel_id should be provided. Please, check input and try again.\n")
        parser.print_help()
        sys.exit(1)
    return args


def main():
    args = parse_args()
    channel_id = args.channel_id
    oldest_timestamp = args.oldest
    oldest_timestamp = read_from_file(f"{channel_id}.txt") or oldest_timestamp
    logger.info(
        f"Starting scraping Slack messages.\nChannel ID: {channel_id}.\tOldest message timestamp: {oldest_timestamp}."
    )
    client = slack.WebClient(token=SLACK_TOKEN)
    update_slack_replies(
        client,
        channel_id,
        FAULT_RECORD_API_URL,
        UPDATE_REPLIES_FOR_DAYS,
        FAULT_RECORD_UPDATE_POST_URL
    )
    conversation_history = get_conversation_history(
        client=client, channel_id=channel_id, msg_limit=MESSAGE_LIMIT_PER_REQUEST, oldest=oldest_timestamp
    )
    logger.info("Processing conversation history.")
    processed_messages, oldest_message_ts = process_conversation_history(conversation_history, client, channel_id, FAULT_RECORD_API_URL)
    logger.info("Posting messages to fault-record API.")
    for message in processed_messages:
        fault_id = post_fault_record(message, FAULT_RECORD_POST_URL)
        if message.get("updates"):
            post_fault_record_updates(message["updates"], fault_id, FAULT_RECORD_UPDATE_POST_URL)
    write_to_file(f"{channel_id}.txt", oldest_message_ts)


if __name__ == "__main__":
    main()
