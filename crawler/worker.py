from threading import Event, Thread, Timer
import signal
import sys
import time

from inspect import getsource
from urllib.parse import urldefrag
from utils.download import download
from utils import get_logger
import scraper


# Define the signal handler to handle interruptions
def handle_interrupt(signum, frame):
    scraper.print_statistics()  # Print crawler statistics before exiting
    print("Process paused.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # Ensure no usage of forbidden modules in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        visited_urls = set()
        word_statistics = {}
        max_word_count_url = ""
        max_word_count = 0

        # Create an event object to handle timeout
        timeout_signal = Event()

        # Read the log file to load previously downloaded URLs
        with open('Logs/Worker.log', 'r') as log_file:
            for log_entry in log_file:
                if 'Downloaded' in log_entry and 'status' in log_entry:
                    downloaded_url = log_entry.split()[1].rstrip(',')
                    # Remove fragments from URL for uniqueness
                    downloaded_url = urldefrag(downloaded_url)[0]
                    visited_urls.add(downloaded_url)
        
        try:
            while True:
                # Reset the timeout event and start a timer for each URL
                timeout_signal.clear()
                timeout_timer = Timer(10, timeout_signal.set)
                timeout_timer.start()

                try:
                    tbd_url = self.frontier.get_tbd_url()
                    if not tbd_url:
                        self.logger.info("Frontier is empty. Stopping Crawler.")
                        break

                    response = download(tbd_url, self.config, self.logger)
                    self.logger.info(
                        f"Downloaded {tbd_url}, status <{response.status}>, "
                        f"using cache {self.config.cache_server}."
                    )

                    if response.status == 200:
                        extracted_urls = scraper.scraper(tbd_url, response, visited_urls, word_statistics, max_word_count_url, max_word_count)
                        for extracted_url in extracted_urls:
                            self.frontier.add_url(extracted_url)
                        self.frontier.mark_url_complete(tbd_url)
                except Exception as error:
                    self.logger.error(f"An exception occurred: {error}")
                    continue
                finally:
                    timeout_timer.cancel()    

                # Skip URL if timeout occurs
                if timeout_signal.is_set():
                    self.logger.info(f"Timeout reached for URL {tbd_url}. Skipping.")
                    continue

                time.sleep(self.config.time_delay)
            
            # Print final statistics when crawling is complete
            scraper.print_statistics()
        except Exception as error:
            scraper.print_statistics()  # Print statistics on unexpected error
            self.logger.error(f"Unexpected error: {error}")
