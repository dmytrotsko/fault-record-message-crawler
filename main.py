import os

import slack
from dotenv import load_dotenv

from utils import (get_conversation_history, process_conversation_history, post_fault_record, post_fault_record_updates)

load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
MESSAGE_LIMIT_PER_REQUEST = os.getenv("MESSAGE_LIMIT_PER_REQUEST")
FAULT_RECORD_POST_URL = os.getenv("FAULT_RECORD_POST_URL")
FAULT_RECORD_UPDATE_POST_URL = os.getenv("FAULT_RECORD_UPDATE_POST_URL")


def main():
    client = slack.WebClient(token=SLACK_TOKEN)
    conversation_history = get_conversation_history(
        client=client,
        channel_id=CHANNEL_ID,
        msg_limit=MESSAGE_LIMIT_PER_REQUEST
    )
    processed_messages = process_conversation_history(conversation_history, client, CHANNEL_ID)
    for message in processed_messages:
        fault_id = post_fault_record(message, FAULT_RECORD_POST_URL)
        if message.get("updates"):
            post_fault_record_updates(message.get("updates"), fault_id, FAULT_RECORD_UPDATE_POST_URL)


if __name__ == "__main__":
    main()
