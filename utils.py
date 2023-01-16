from datetime import datetime as dtime
import re
import os
from dotenv import load_dotenv
import logging
import requests
from typing import Dict, List

import pytz
import marko
from slack import WebClient
from slack.errors import SlackApiError


logger = logging.getLogger("slack-fault-scrapper")

load_dotenv()
EMOJI_FLAG = os.getenv("EMOJI_FLAG")

CLEANR = re.compile('<.*?>')


def parse_timestamp(ts: float) -> str:
    """Convert Slack message timestamp to date

    Args:
        ts (float): Slack message timestamp

    Returns:
        str: date
    """
    dt = dtime.strftime(dtime.fromtimestamp(ts, pytz.UTC), "%Y-%m-%d")
    return dt


def reactions_list(message: dict):
    return [reaction["name"] for reaction in message["reactions"]] if message.get("reactions") else []


def get_user_info(client: WebClient, user_id: str) -> str:
    """Get user info (name, email)

    Args:
        client (WebClient): Slack WebClient object
        user_id (str): user's id

    Returns:
        str: user email
    """

    user_info = client.users_info(user=user_id)
    try:
        real_name = user_info["user"]["profile"]["real_name"]
        email = user_info["user"]["profile"]["email"]
        return f"{real_name} ({email})"
    except KeyError:
        return "<anonymous>"


def get_message_replies(client: WebClient, channel_id: str, parent_message_ts: str) -> list:
    """Fetch message replies

    Args:
        client (WebClient): Slack WebClient object;
        channel_id (str): id of the channel to get message replies from
        parent_message_ts (str): Slack parent message timestamp

    Returns:
        list: replies
    """

    parsed_replies = []
    replies = client.conversations_replies(channel=channel_id, ts=parent_message_ts)
    for reply in replies["messages"][1:]:  # To skip first thread message which is the original message
        parsed_replies.append({
            "author": get_user_info(client, reply["user"]),
            "created": parse_timestamp(float(reply["ts"])),
            "description": replace_user_id(reply["text"], client)
        })
    return parsed_replies


def remove_markdown(text: str):
    cleantext = re.sub(CLEANR, '', text)
    return cleantext


def extract_source_signal_pair(raw_text: str):
    source_regexp = "<em>.*<\/em>"
    signal_regexp = "<code>.*<\/code>"
    converted_text = marko.convert(raw_text).replace("<p>", "").replace("</p>", "")
    source = remove_markdown(list(filter(None, re.findall(source_regexp, converted_text)))[-1])
    source_signal_list = list(source + ":" + remove_markdown(el).split(":")[0] for el in filter(None, re.findall(signal_regexp, converted_text)))
    return source_signal_list if source_signal_list and len(source_signal_list[0].split(":")) == 2 else []


def get_conversation_history(client: WebClient, channel_id: str, msg_limit: int, oldest: int = 0) -> List[Dict]:
    """Fetch the conversaion history of particular channel;

    Args:
        client (WebClient): Slack WebClient object
        channel_id (str): id of the channel to get messages from
        msg_limit (int): limit of the messages to get per 1 API call
        oldest (int, optional): timestamp of the oldest message to start from

    Returns:
        (List[Dict]): list of the scraped messages
    """
    try:
        result = client.conversations_history(
            channel=channel_id,
            oldest=oldest,
            limit=msg_limit)
        all_messages = []
        all_messages += result["messages"]
        while result['has_more']:
            result = client.conversations_history(
                channel=channel_id,
                cursor=result['response_metadata']['next_cursor'],
                limit=msg_limit
            )
            all_messages += result["messages"]
        return all_messages[::-1]  # Return messages in the right order
    except SlackApiError:
        logger.exception("Error while fetching the conversation history")


def replace_user_id(inp_str: str, client: WebClient) -> str:
    """Takes input string and replaces Slack user ID with email

    Args:
        inp_str (str): Input string (slack message)
        client (WebClient): Slack WebClient

    Returns:
        str: Message string with replaced user ids to user emails
    """
    regex = r"\<\@([a-zA-Z0-9]*)\>"
    matches = [x.group() for x in re.finditer(regex, inp_str)]
    if matches:
        users = {}
        for user_id in matches:
            users[user_id] = get_user_info(client, user_id[2:-1])
        for k, v in users.items():
            inp_str = inp_str.replace(k, v)
    return inp_str


def parse_user_message(message: dict, channel_id: str, client: WebClient) -> dict:
    """Parse Slack message from user
    Args:
        message (dict): raw Slack message
        channel_id (str): id of the channel to get messages from
        client (WebClient): Slack WebClient object

    Returns:
        dict: parsed Slack message
    """

    parsed_message = {}
    parsed_message["url"] = f"https://delphi-org.slack.com/archives/{channel_id}/p{message['ts'].replace('.', '')}"
    parsed_message["title"] = replace_user_id(message["text"].split('.')[0], client)
    parsed_message["reported_by"] = get_user_info(client, message["user"])
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(message["text"], client)
    return parsed_message


def parse_bot_message(message: dict, channel_id: str, client: WebClient) -> dict:
    """Parse Slack message from bot

    Args:
        message (dict): raw Slack message
        channel_id (str): id of the channel to get messages from
        client (WebClient): Slack WebClient object

    Returns:
        dict: parsed Slack message
    """
    parsed_message = {}
    parsed_message["url"] = f"https://delphi-org.slack.com/archives/{channel_id}/p{message['ts'].replace('.', '')}"
    parsed_message["title"] = replace_user_id(message["attachments"][0]["title"].split(": ")[1], client)
    parsed_message["reported_by"] = message["username"]
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(" ".join(message["attachments"][0]["text"].split("\n")[2:]).strip(), client)
    parsed_message["signals"] = extract_source_signal_pair(message["attachments"][0]["text"])
    return parsed_message


def process_conversation_history(conversation_history: List, client: WebClient, channel_id: str):
    """Process Slack conversation history

    Args:
        conversation_history (List): Slack conversation history (list of messages)
        client (WebClient): Slack WebClient object
        channel_id (str): Slack channel id

    Yields:
        parsed_message (dict): parsed Slack message with replies
    """
    for message in conversation_history:
        if message.get("subtype", "").startswith("channel_"):
            continue
        if message.get("subtype", "") != "bot_message":
            if EMOJI_FLAG in reactions_list(message):
                parsed_message = parse_user_message(message, channel_id, client)
                if message.get("reply_count") is not None:
                    replies = get_message_replies(
                        client=client,
                        channel_id=channel_id,
                        parent_message_ts=message["ts"]
                    )
                    parsed_message["updates"] = replies
                    yield parsed_message
        else:
            try:
                if "successful" in message["attachments"][0]["title"].lower():
                    continue  # To skip messages about successful runs
                parsed_message = parse_bot_message(message, channel_id, client)
                if message.get("reply_count") is not None and message.get("reply_count") > 1:
                    replies = get_message_replies(
                        client=client,
                        channel_id=channel_id,
                        parent_message_ts=message["ts"]
                    )
                    parsed_message["updates"] = replies
                    yield parsed_message
            except KeyError:
                logger.error(f"Could not parse bot message. Message ts: {message['ts']}")
                continue


def post_fault_record(message: dict, record_post_url: str):
    """Creates Fault Record from Slack message

    Args:
        message (dict): parsed Slack message
        record_post_url (str): fault-record API url to post Record

    Returns:
        response: json response which contains Record info
    """
    payload = {
        "DVC_id": 1,
        "name": message["title"],
        "desc": message["description"],
        "user_id": 1,  # message["reported_by"],
        "first_occurance": message["reported_date"],
        "last_occurance": message["reported_date"],
        "record_date": message["reported_date"],
        "last_updated": dtime.strftime(dtime.now(), "%Y-%m-%d"),
        "published": False,
        "signals": message.get("signals")
    }
    response = requests.post(url=record_post_url, json=payload)
    return response.json().get("fault_id")


def post_fault_record_updates(updates: List[Dict], fault_id: int, update_post_url: str):
    """Creates Fault Record Update from Slack message replies

    Args:
        updates (List[Dict]): list of Slack message replies
        fault_id (int): Fault Record id
        update_post_url (str): fault-record API url to post Update
    """
    for update in updates:
        payload = {
            "user_id": 1,  # update["author"]
            "desc": update["description"],
            "fault_id": fault_id,
            "fault_status": "Test Status",
            "record_date": update["created"]
        }
        requests.post(url=update_post_url, json=payload)
