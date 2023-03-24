import json
import os
import re
from datetime import datetime as dtime
from datetime import timedelta
from typing import Dict, List
from urllib.parse import urlparse

import marko
import pytz
import requests
from retry import retry
from dotenv import load_dotenv
from ghapi.core import GhApi
from logzero import logger
from slack import WebClient
from slack.errors import SlackApiError

load_dotenv()
EMOJI_FLAG = os.getenv("EMOJI_FLAG")

CLEANR = re.compile("<.*?>")


def parse_timestamp(ts: float) -> str:
    """Convert Slack message timestamp to date

    Args:
        ts (float): Slack message timestamp

    Returns:
        str: date
    """
    dt = dtime.strftime(dtime.fromtimestamp(ts, pytz.UTC), "%Y-%m-%d")
    return dt


def remove_emojis(data):
    """Removes emojis from the string.

    Args:
        data (_type_): string or text that containts emojis

    Returns:
        _type_: string or text with removed emojis
    """
    emoj = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+",
        re.UNICODE,
    )
    return re.sub(emoj, "", data)


def reactions_list(message: dict) -> List:
    """Get reactions on Slack message (emojis)

    Args:
        message (dict): Slack message

    Returns:
        List: list of emojis names that were used in the message
    """
    return [reaction["name"] for reaction in message["reactions"]] if message.get("reactions") else []


@retry(SlackApiError, tries=3, delay=5)
def get_user_info(client: WebClient, user_id: str, return_email: bool = False) -> str:
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
        return f"{real_name} ({email})" if not return_email else email
    except KeyError:
        return "<anonymous>"


def get_user_id(user_email: str, fault_record_api_url: str) -> int:
    """Get user id by email. Sends request to the fault-record API with email filter.


    Args:
        user_email (str): user email
        fault_record_api_url (str): fault record API url

    Returns:
        int: In case there is no user with given email, returns 1, which is ID of the anonymous user.
    """
    base_url = f"{fault_record_api_url}/api/v1/users?disable_pagination=True"
    query_filter = f'"field": "email", "op": "=", "value": "{user_email}"'
    result_url = f"{base_url}&filters=[{{{query_filter}}}]"
    try:
        user = requests.get(result_url).json()[0]
        return int(user.get("user_id"))
    except IndexError:
        return 1  # TODO: should be removed. As we don't have real users it returns id == 1


def get_message_replies(client: WebClient, channel_id: str, parent_message_ts: str, fault_record_api_url: str) -> List:
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
        parsed_replies.append(
            {
                "author": get_user_id(get_user_info(client, reply["user"], True), fault_record_api_url),
                "url": f"https://delphi-org.slack.com/archives/{channel_id}/p{reply['ts'].replace('.', '')}?thread_ts={reply['thread_ts']}&cid={channel_id}",
                "created": parse_timestamp(float(reply["ts"])),
                "description": replace_user_id(remove_emojis(reply["text"]), client),
            }
        )
    return parsed_replies


def remove_markdown(text: str):
    """Removes html markdown from the text

    Args:
        text (str): raw text which contains html tags

    Returns:
        str: text without html tags
    """
    cleantext = re.sub(CLEANR, "", text)
    return cleantext


def extract_source_signal_pair(raw_text: str):
    """Extracts (source, signal) pairs from Slack message.

    Args:
        raw_text (str): Slack message

    Returns:
        str, list: source name, list of signals for that source
    """
    source_regexp = "<em>.*<\/em>"
    signal_regexp = "<code>.*<\/code>"
    converted_text = marko.convert(raw_text).replace("<p>", "").replace("</p>", "")
    source = remove_markdown(list(filter(None, re.findall(source_regexp, converted_text)))[-1])
    signals = list(remove_markdown(el).split(":")[0] for el in filter(None, re.findall(signal_regexp, converted_text)))
    if source and signals:
        return source, signals


def get_signals_url(source: str, fault_record_api_url: str, signals: List[str]):
    """Get url to query (source, signal) pairs from fault-record API

    Args:
        source (str): signal source
        fault_record_api_url (str): fault-record API url
        signals (List[str]): list of signals

    Returns:
        str: compiled url with filters to get existing (source, signal) pairs from failt-record API
    """
    base_url = f"{fault_record_api_url}/api/v1/signals?disable_pagination=True"
    source_filter = f'"field": "source", "op": "=", "value": "{source}"'
    signals_str = '"' + '", "'.join(signals) + '"'
    signals_filter = f'"field": "signal", "op": "in", "value": [{signals_str}]'
    result_url = f"{base_url}&filters=[{{{source_filter}}},{{{signals_filter}}}]"
    return result_url


def get_signal_ids(message: str) -> List:
    """Get signal ids from Slack message

    Args:
        message (str): Slack message that may contain signals

    Returns:
        List: list of signal ids from fault-record API
    """
    try:
        source, signals = extract_source_signal_pair(message)
        query_signals_url = get_signals_url(source, signals)
        signals = requests.get(query_signals_url)
        signal_ids = [sig.get("signal_id") for sig in signals.json()]
        return signal_ids
    except Exception:
        return []


def get_conversation_history(client: WebClient, channel_id: str, msg_limit: int, oldest: float = 0) -> List[Dict]:
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
        result = client.conversations_history(channel=channel_id, oldest=oldest, limit=msg_limit)
        all_messages = []
        all_messages += result["messages"]
        while result["has_more"]:
            result = client.conversations_history(
                channel=channel_id, cursor=result["response_metadata"]["next_cursor"], limit=msg_limit
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


def parse_user_message(message: dict, channel_id: str, client: WebClient, fault_record_api_url: str) -> dict:
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
    parsed_message["title"] = replace_user_id(remove_emojis(message["text"]).split(".")[0], client)
    parsed_message["reported_by"] = get_user_id(get_user_info(client, message["user"], True), fault_record_api_url)
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(remove_emojis(message["text"]), client)
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
    parsed_message["title"] = replace_user_id(remove_emojis(message["attachments"][0]["title"]).split(": ")[1], client)
    parsed_message[
        "reported_by"
    ] = 1  # message["username"] # TODO: should be replaced with better logic. Maybe create some user for those records which don't have user as author
    parsed_message["reported_date"] = parse_timestamp(float(message["ts"]))
    parsed_message["description"] = replace_user_id(
        " ".join(remove_emojis(message["attachments"][0]["text"]).split("\n")[2:]).strip(), client
    )
    parsed_message["signals"] = get_signal_ids(message["attachments"][0]["text"])
    return parsed_message


def process_conversation_history(
    conversation_history: List, client: WebClient, channel_id: str, fault_record_api_url: str
):
    """Process Slack conversation history

    Args:
        conversation_history (List): Slack conversation history (list of messages)
        client (WebClient): Slack WebClient object
        channel_id (str): Slack channel id

    Yields:
        parsed_message (dict): parsed Slack message with replies
    """
    parsed_messages = []
    for i in range(len(conversation_history)):
        logger.info(f"Processing message {i+1}/{len(conversation_history)+1}")
        if conversation_history[i].get("subtype", "").startswith("channel_"):
            logger.info("Channel notification message. Skipping.")
            continue
        if conversation_history[i].get("subtype", "") != "bot_message":
            if EMOJI_FLAG in reactions_list(conversation_history[i]) or channel_id == "C0130CSQRN3":
                logger.info("Parsing user message.")
                parsed_message = parse_user_message(conversation_history[i], channel_id, client, fault_record_api_url)
                if conversation_history[i].get("reply_count") is not None:
                    logger.info("Getting message replies.")
                    replies = get_message_replies(
                        client=client,
                        channel_id=channel_id,
                        parent_message_ts=conversation_history[i]["ts"],
                        fault_record_api_url=fault_record_api_url,
                    )
                    parsed_message["updates"] = replies
                parsed_messages.append(parsed_message)
        else:
            try:
                if "successful" in conversation_history[i]["attachments"][0]["title"].lower():
                    logger.info("Message about successfull run. Skipping.")
                    continue  # To skip messages about successful runs
                logger.info("Parsing bot message.")
                parsed_message = parse_bot_message(conversation_history[i], channel_id, client)
                if (
                    conversation_history[i].get("reply_count") is not None
                    and conversation_history[i].get("reply_count") > 1
                ):
                    logger.info("Getting message replies.")
                    replies = get_message_replies(
                        client=client,
                        channel_id=channel_id,
                        parent_message_ts=conversation_history[i]["ts"],
                        fault_record_api_url=fault_record_api_url,
                    )
                    parsed_message["updates"] = replies
                parsed_messages.append(parsed_message)
            except KeyError as e:
                logger.error(f"Could not parse bot message. Message ts: {conversation_history[i]['ts']}\nReason: {e}")
                continue
    oldest_message_ts = conversation_history[-1]["ts"]
    return parsed_messages, oldest_message_ts


def post_fault_record(message: dict, record_post_url: str):
    """Creates Fault Record from Slack message

    Args:
        message (dict): parsed Slack message
        record_post_url (str): fault-record API url to post Record

    Returns:
        response: json response which contains Record info
    """
    logger.info("Posting new Fault Record.")
    payload = {
        "name": message["title"],
        "desc": message["description"],
        "user_id": message["reported_by"],
        "first_occurance": message["reported_date"],
        "last_occurance": message["reported_date"],
        "record_date": message["reported_date"],
        "signals": message.get("signals"),
        "source_link": message["url"],
    }
    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    response = requests.post(url=record_post_url, data=json.dumps(payload), headers=headers)
    if response.status_code == 200:
        logger.info(f"Request ended with status {response.status_code}. Fault Record #{response.json().get('fault_id')} has been successfully created.")
    else:
        logger.error(f"Something went wrong. Fault Record from {message['url']} was not created.")
    return response.json().get("fault_id")


def post_fault_record_updates(updates: List[Dict], fault_id: int, update_post_url: str):
    """Creates Fault Record Update from Slack message replies

    Args:
        updates (List[Dict]): list of Slack message replies
        fault_id (int): Fault Record id
        update_post_url (str): fault-record API url to post Update
    """
    logger.info(f"Posting Fault Record updates for #{fault_id}")
    for update in updates:
        payload = {
            "user_id": update["author"],
            "desc": update["description"],
            "fault_id": fault_id,
            "fault_status": "Test Status",
            "record_date": update["created"],
            "source_link": update["url"],
        }
        response = requests.post(url=update_post_url, json=payload)
        if response.status_code == 200:
            logger.info("Fault Record update has been successfully posted.")
        else:
            logger.error(f"Something went wrong. Could not post Fault Record update for Fault #{fault_id} (slack message url -> {update['url']}).")


def write_to_file(file_name: str, data: str):
    """Writes data to the text file. Is created to store oldest_timestamp for Slack messages scraper
    and resume_page for GitHub issues scraper

    Args:
        file_name (str): file name with extension (ex. .txt, .json, etc.)
        data (str): data to be written
    """
    with open(file_name, "w") as f:
        f.write(data)


def read_from_file(file_name: str):
    """Reads data from the given file.

    Args:
        file_name (str): file name with extension (ex. .txt, .json, etc.)

    Returns:
        _type_: first line from the text file
    """
    try:
        with open(file_name, "r") as f:
            return f.readline()
    except FileNotFoundError:
        return 0


def get_fault_records(fault_record_api_url: str, from_date_days: int, source: str):
    """Get fault Records from fault-record API for given timedelta

    Args:
        fault_record_api_url (str): fault-record API url
        from_date_days (int): days timedelta
        source (str): source name "github" or "slack"

    Returns:
        List[Dict]: list of fault-records
    """
    result = []
    base_url = f"{fault_record_api_url}/api/v1/faults?disable_pagination=True"
    from_record_date = dtime.now() - timedelta(days=int(from_date_days))
    from_record_date_str = dtime.strftime(from_record_date, "%Y-%m-%d")
    query_filter = f'"field": "record_date", "op": ">", "value": "{from_record_date_str}"'
    result_url = f"{base_url}&filters=[{{{query_filter}}}]"
    response = requests.get(result_url).json()
    for record in response:
        record_source = urlparse(record.get("source_link"))
        if source in record_source.netloc.split("."):
            result.append(record)
    return result


def get_fault_record_updates(fault_record_api_url: str, fault_id: int):
    """Get Fault Record Updates

    Args:
        fault_record_api_url (str): fault-record API url
        fault_id (int): fault id

    Returns:
        List[Dict] (json): fault record updates
    """
    base_url = f"{fault_record_api_url}/api/v1/updates?disable_pagination=True"
    query_filter = f'"field": "fault_id", "op": "=", "value": "{fault_id}"'
    result_url = f"{base_url}&filters=[{{{query_filter}}}]"
    response = requests.get(result_url).json()
    return response


def get_message_ts_from_link(message_link: str):
    """Extract Slack message timestamp from source_link

    Args:
        message_link (str): Fault Record `source_link` column

    Returns:
        float: Slack message timestamp
    """
    message_ts = message_link.split("/")[-1].replace("p", "")
    parsed_ts = f"{message_ts[:-6]}.{message_ts[-6:]}"
    return float(parsed_ts)


def update_slack_replies(
    client: WebClient,
    channel_id: str,
    fault_record_api_url: str,
    update_replies_for_last_days: int,
    fault_record_update_post_url: str,
):
    """Get new Fault Record Updates from Slack message replies

    Args:
        client (WebClient): Slack WebClient
        channel_id (str): Slack channel ID
        fault_record_api_url (str): fault-record API url
        update_replies_for_last_days (int): days timedelta
        fault_record_update_post_url (str): fault-record API post Update url
    """
    fault_records = get_fault_records(fault_record_api_url, update_replies_for_last_days, "slack")
    if fault_records:
        for record in fault_records:
            fault_record_updates = get_fault_record_updates(fault_record_api_url, record["fault_id"])
            source_links = [el.get("source_link") for el in fault_record_updates]
            message_ts = get_message_ts_from_link(record["source_link"])
            slack_message_replies = get_message_replies(client, channel_id, message_ts)
            new_replies = []
            for reply in slack_message_replies:
                if reply.get("url") not in source_links:
                    new_replies.append(reply)
            post_fault_record_updates(new_replies, record["fault_id"], fault_record_update_post_url)
    else:
        raise Exception("No faults found. Skipping update.")


def get_github_user(ghapi: GhApi, username: str, return_email: bool = False):
    """Get github user by email

    Args:
        ghapi (GhApi): github api instance
        username (str): github username
        return_email (bool, optional): return user email or "user_name user_email" string. Defaults to False.

    Returns:
        _type_: email or "user_name user_email"
    """
    user_info = ghapi.users.get_by_username(username)
    user_name = user_info["name"] if user_info["name"] else user_info["login"]
    user_email = f"({user_info['email']}" if user_info["email"] else ""
    user = f"{user_name} ({user_email})".strip()
    return user if not return_email else user_email


def parse_github_issue(ghapi: GhApi, issue: dict, fault_record_api_url: str):
    """Parse GitHub issue

    Args:
        ghapi (GhApi): github api instance
        issue (dict): raw github issue
        fault_record_api_url (str): fault-record API url

    Returns:
        _type_: parsed issue with mapped columns
    """
    parsed_issue = {}
    parsed_issue["url"] = issue["html_url"]
    parsed_issue["title"] = remove_emojis(issue["title"])
    parsed_issue["reported_by"] = get_user_id(get_github_user(ghapi, issue["user"]["login"]), fault_record_api_url)
    parsed_issue["reported_date"] = issue["created_at"]
    parsed_issue["description"] = remove_emojis(issue["body"])
    parsed_issue["signals"] = []
    return parsed_issue


def parse_github_issue_comments(ghapi: GhApi, issue_comments: List[Dict], fault_record_api_url: str):
    """Parse GitHub issue comments

    Args:
        ghapi (GhApi): github api instance
        issue_comments (List[Dict]): list of github issue comments
        fault_record_api_url (str): fault-record API url

    Returns:
        List[Dict]: parsed github issue comments
    """
    parsed_comments = []
    for comment in issue_comments:
        parsed_comment = {}
        parsed_comment["url"] = comment["html_url"]
        parsed_comment["author"] = get_user_id(
            get_github_user(ghapi, comment["user"]["login"], True), fault_record_api_url
        )
        parsed_comment["created"] = comment["created_at"]
        parsed_comment["description"] = remove_emojis(comment["body"])
        parsed_comments.append(parsed_comment)
    return parsed_comments


def update_github_comments(
    fault_record_api_url: str,
    update_replies_for_last_days: int,
    ghapi: GhApi,
    owner: str,
    repo: str,
    fault_record_update_post_url: str,
):
    """Get new Fault Record Updates from GitHub issue comments

    Args:
        fault_record_api_url (str): fault-record API url
        update_replies_for_last_days (int): days timedeltat
        ghapi (GhApi): github API instance
        owner (str): GitHub repository owner
        repo (str): GitHub repository name
        fault_record_update_post_url (str): fault-record API post Update url
    """
    fault_records = get_fault_records(fault_record_api_url, update_replies_for_last_days, "github")
    if fault_records:
        for record in fault_records:
            fault_record_updates = get_fault_record_updates(fault_record_api_url, record["fault_id"])
            source_links = [el.get("source_link") for el in fault_record_updates]
            issue_number = urlparse(record["source_link"]).path.split("/")[-1]
            comments = ghapi.issues.list_comments(owner=owner, repo=repo, issue_number=issue_number)
            new_comments = []
            for comment in comments:
                if comment.get("html_url") not in source_links:
                    new_comments.append()
            post_fault_record_updates(new_comments, record["fault_id"], fault_record_update_post_url)
    else:
        raise Exception("No faults found. Skipping update.")
