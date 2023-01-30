from cronicle_wrapper import create_event, run_event, get_job_status, delete_event
import time
import logging


logger = logging.getLogger("slack-scraper-runner")
logger.setLevel(logging.DEBUG)


def get_oldest_ts(file_name: str):
    logger.info("Trying to get oldest slack message timestamp from the file: {file_name}")
    try:
        with open(file_name, "r") as f:
            oldest_timetsmp = f.readline()
            logger.info(f"Returning oldest timetsmp: {oldest_timetsmp}")
            return oldest_timetsmp
    except FileNotFoundError:
        return 0


def write_oldest_ts(file_name: str, oldest_ts: str):
    with open(file_name, "w") as f:
        f.write(oldest_ts)


def main():
    channels = {"system-monitoring": "C01LZ3A2UMU", "outages": "C0130CSQRN3"}
    for _, v in channels.items():
        logger.info(f"Creating event for channel: {v}")
        oldest_ts = get_oldest_ts(f"{v}.txt")
        event_id = create_event(
            "Slack message scraper",
            "general",
            "pldbg9biz03",
            "allgrp",
            1,
            params={"CHANNEL_ID": v, "OLDEST_TIMESTAMP": oldest_ts},
        )["id"]
        logger.info(f"Running event (event id: {event_id}) for channel: {v}")
        job_id = run_event(event_id)["ids"][0]
        job_end_time = None
        while not job_end_time:
            time.sleep(30)
            job_status = get_job_status(job_id)
            job_end_time = job_status["job"].get("time_end")
        write_oldest_ts(f"{v}.txt", job_end_time)
        delete_event(event_id)


if __name__ == "__main__":
    main()
