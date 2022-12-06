from datetime import datetime as dtime
import re
import json
import logging
from typing import Dict, List

import pytz
from slack import WebClient
from slack.errors import SlackApiError

logger = logging.getLogger("slack-fault-scrapper")

def parse_timestamp(ts: float) -> str:
    """Convert Slack message timestamp to date 

    Args:
        ts (float): Slack message timestamp

    Returns:
        str: date
    """
    dt = dtime.strftime(dtime.fromtimestamp(ts, pytz.UTC), "%Y-%m-%d")
    return dt


def write_result(file_name: str, inp: List[Dict]) -> None:
    """Write result to the json file

    Args:
        file_name (str): name of the output file
        inp (List[Dict]): Slack messages
    """
    with open(f"{file_name}.json", "w") as outfile:
        outfile.write(json.dumps(inp))


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
        return user_info["user"]["profile"]["email"]
    except KeyError as e:
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
    for reply in replies["messages"]:
        parsed_replies.append({
            "author": get_user_info(client, reply["user"]),
            "created": parse_timestamp(float(reply["ts"])),
            "description": replace_user_id(reply["text"], client)
        })
    return parsed_replies


def get_conversation_history(client: WebClient, channel_id: str, msg_limit: int) -> List[Dict]:
    """Fetch the conversaion history of particular channel;

    Args:
        client (WebClient): Slack WebClient object
        channel_id (str): id of the channel to get messages from
        msg_limit (int): limit of the messages to get per 1 API call

    Returns:
        (List[Dict]): list of the scraped messages
    """
    try:
        result = client.conversations_history(
            channel=channel_id,
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
        inp_str_split = inp_str.split(" ")
        for i in range(len(inp_str_split)):
            if users.get(inp_str_split[i]) is not None:
                inp_str_split[i] = users[inp_str_split[i]]
        return " ".join(inp_str_split)
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
    parsed_message["title"] = message["text"].split('.')[0]
    parsed_message["reported_by"] = get_user_info(client, message["user"])
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(message["text"],client)
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
    parsed_message["title"] = message["attachments"][0]["title"].split(": ")[1]
    parsed_message["reported_by"] = message["username"]
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(" ".join(message["attachments"][0]["text"].split("\n")[2:]).strip(), client)
    return parsed_message




