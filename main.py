import os

import slack
from dotenv import load_dotenv
from tqdm import tqdm

from utils import (get_conversation_history, get_message_replies,
                   parse_user_message, write_result, parse_bot_message, logger)

load_dotenv()

SLACK_TOKEN = os.getenv("SLACK_TOKEN")
CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
MESSAGE_LIMIT_PER_REQUEST = os.getenv("MESSAGE_LIMIT_PER_REQUEST")


def main():
    client = slack.WebClient(token=SLACK_TOKEN)
    conversation_history = get_conversation_history(
        client=client,
        channel_id=CHANNEL_ID,
        msg_limit=MESSAGE_LIMIT_PER_REQUEST
    )
    result = []
    for message in tqdm(conversation_history):
        if message.get("subtype", "").startswith("channel_"):
            continue
        if message.get("subtype", "") != "bot_message":
            parsed_message = parse_user_message(message, CHANNEL_ID, client)
            if message.get("reply_count") is not None:
                replies = get_message_replies(
                    client=client,
                    channel_id=CHANNEL_ID,
                    parent_message_ts=message.get("ts")
                )
                parsed_message["updates"] = replies[1:]
        else:
            try:
                if "successful" in message["attachments"][0]["title"].lower():
                    continue # To skip messages about successful runs
                parsed_message = parse_bot_message(message, CHANNEL_ID, client)
            except KeyError:
                logger.error(f"Could not parse bot message. Message ts: {message['ts']}")
                continue
        result.append(parsed_message)
    write_result("outages-parsed-users", result)



if __name__ == "__main__":
    main()
