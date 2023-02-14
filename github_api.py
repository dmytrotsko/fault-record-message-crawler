import time
from datetime import datetime as dtime

from ghapi.core import GhApi
from ghapi.page import paged
from logzero import logger

from utils import parse_github_issue, parse_github_issue_comments, write_to_file


def unix_2_utc(unix_timestamp):
    """
    Turns a Unix timestamp into a UTC timestamp string.

    @param unix_timestamp: Unix timestamp.
    @return: Corresponding timestamp string
    """
    return dtime.utcfromtimestamp(unix_timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")


def init_api(token: str, api_logging_frequency: int, api_quota_threshold: int):
    """
    Creates a GitHub API instance with set token and rate limit callbacks.

    @param token: GitHub API token (see https://github.com/settings/tokens).
    @return: a GhApi instance.
    """

    # Initialize API with GitHub token
    api = GhApi(token=token)

    # Define limit_cb (called after each API request with rate limit info)
    def limit_callback(rem, quota):
        # Periodically print remaining quota
        if rem % api_logging_frequency == 0:
            logger.info(f"Remaining API requests per hour: {rem} of {quota}")

        # Once the quota is close enough to ending, wait until it cools down
        if rem == api_quota_threshold:
            # Reset the callback for the next rate_limit.get() call
            api.limit_cb = None

            # Get the timestamp at which the quota will be reset
            limits = api.rate_limit.get()
            reset = limits.resources.core.reset

            # Sleep until that timestamp
            logger.warn("Close to reaching API rate limit. " + f"Halting until {unix_2_utc(reset)}...")
            while int(time.time()) <= reset:
                time.sleep(1)
            logger.warn("Rate limit is reset, resuming...")

            # Restore the callback to this function again
            api.limit_cb = limit_callback

    api.limit_cb = limit_callback
    return api


def list_issues(api: GhApi, owner: str, repository: str, api_page_size: int, ascending=False):
    """
    Creates a GitHub API iterator that returns all pull requests
    in a given repository, sorted by last updated time.

    @param api: GhApi instance.
    @param owner: The account owning the GitHub repository.
    @param repository: The name of the GitHub repository.
    @param ascending: Whether the results should be sorted in ascending order.
    @return: GhApi paging iterator object.
    """
    direction = "asc" if ascending else "desc"
    return paged(
        api.issues.list_for_repo,
        owner=owner,
        repo=repository,
        state="open",
        sort="updated",
        direction=direction,
        per_page=api_page_size,
    )


def action_collect(ghapi: GhApi, owner: str, repo: str, api_page_size: int, fault_record_api_url: str, resume_page: int = None):
    """Collect GitHub issues

    Args:
        ghapi (GhApi): github API instance
        owner (str): GitHub repository owner
        repo (str): GitHub repository name
        api_page_size (int): number of records per page (default=30, max=100)
        fault_record_api_url (str): fault-record API url
        resume_page (int, optional): start from page number. Defaults to None.

    Returns:
        List[Dict]: list of parsed GitHub issues
    """
    issues_list = list_issues(ghapi, owner, repo, api_page_size, ascending=True)
    parsed_issues = []
    try:
        logger.info(f"Collecting issues from {owner}/{repo}...")
        for count, page in enumerate(issues_list):
            # If resume_page is set, skip until that page
            if resume_page is not None and count + 1 < resume_page:
                continue

            # Parse all issues & comments and post them to the fault-record API
            logger.debug(f"GET list issues {owner}/{repo} | page #{count+1}")
            for issue in page:
                if not issue.get("pull_request"):
                    parsed_issue = parse_github_issue(ghapi, issue, fault_record_api_url)
                    issue_comments = ghapi.issues.list_comments(owner=owner, repo=repo, issue_number=issue["number"])
                    parsed_comments = parse_github_issue_comments(ghapi, issue_comments, fault_record_api_url)
                    parsed_issue["comments"] = parsed_comments
                    parsed_issues.append(parsed_issue)
        write_to_file(f"{repo}-{owner}.txt", str(count))
        return parsed_issues
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
