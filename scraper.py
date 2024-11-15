from collections import Counter
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from threading import Lock
from simhash import Simhash, SimhashIndex

# Global variables for tracking visited URLs and word statistics
tracked_urls = set()
word_frequency = {}
max_words_page_url = ""
max_words_count = 0
redirect_tracking = Counter()
page_simhashes = {}
simhash_index = SimhashIndex([], k=3)

# Lock for thread-safe writing to output file
file_lock = Lock()

update_interval = 0
php_trap_counter = Counter()
url_trap_counter = Counter()

# Default list of common English stopwords
stopwords = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", 
    "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", 
    "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", 
    "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", 
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", 
    "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", 
    "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", 
    "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", 
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", 
    "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", 
    "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", 
    "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", 
    "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", 
    "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", 
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", 
    "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
])


def save_to_output():
    """Thread-safe function to save crawler statistics to output.txt"""
    with file_lock:
        try:
            with open("output.txt", 'w') as file:
                # Save the total count of unique pages visited
                file.write(f"Total unique pages: {len(tracked_urls)}\n")
                
                # Save the page with the highest word count
                file.write(f"Page with the highest word count: {max_words_page_url} (Words: {max_words_count})\n")
                
                # Save the top 50 most frequent words
                top_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:50]
                file.write("Top 50 most frequent words:\n")
                for word, count in top_words:
                    file.write(f"{word}: {count}\n")
                
                # Save subdomain statistics for .ics.uci.edu
                subdomains = Counter()
                for tracked_url in tracked_urls:
                    parsed = urlparse(tracked_url)
                    if parsed.netloc.endswith('.ics.uci.edu'):
                        subdomains[parsed.netloc] += 1
                
                file.write("\nSubdomain statistics for .ics.uci.edu:\n")
                for subdomain, subdomain_count in sorted(subdomains.items()):
                    file.write(f"{subdomain}, {subdomain_count}\n")
        except Exception as err:
            print(f"Error occurred while writing to output.txt: {err}")

def handle_response_error(resp):
    """Handles response errors based on status codes to determine further processing."""
    if not resp or not hasattr(resp, 'error'):
        return False
    
    error_code = resp.error

    # Critical errors - must stop processing
    if error_code in [603, 604, 605, 608]:
        critical_errors = {
            603: "URL scheme must be http or https.",
            604: "Domain must be within specified domains.",
            605: "Invalid file extension detected.",
            608: "Access denied by robots.txt."
        }
        print(f"Critical Error {error_code}: {critical_errors[error_code]}")
        return False

    # Errors to handle with specific action
    elif error_code in [606, 607]:
        if error_code == 607:
            content_length = resp.headers.get('content-length', 'unknown')
            print(f"Error 607: Content exceeds size limit - {content_length} bytes")
        elif error_code == 606:
            print(f"Error 606: Cannot parse URL.")
        return False

    # Ignorable errors that can be skipped
    elif error_code in [600, 601, 602]:
        print(f"Ignorable error {error_code}: Continuing with next URL.")
        return True

    return True

def load_from_output():
    """Function to restore statistics from output.txt"""
    global word_frequency, max_words_page_url, max_words_count
    
    try:
        with open('output.txt', 'r') as file:
            lines = file.readlines()
            index = 0
            while index < len(lines):
                line = lines[index]
                # Restore word frequency
                if line.startswith("Top 50 most frequent words:"):
                    word_frequency = {}
                    index += 1
                    while index < len(lines) and lines[index].strip() != "":
                        word, count = lines[index].split(": ")
                        word_frequency[word] = int(count)
                        index += 1
                # Restore the page with the most words
                elif line.startswith("Page with the highest word count:"):
                    parts = line.strip().split(' ')
                    max_words_page_url = parts[5]
                    max_words_count = int(parts[-1])
                index += 1
    except FileNotFoundError:
        # Initialize with empty values if the file does not exist
        word_frequency = {}
        max_words_count = 0
        max_words_page_url = ""
    except Exception as err:
        print(f"Error occurred while reading from output.txt: {err}")
        word_frequency = {}
        max_words_count = 0
        max_words_page_url = ""


def scraper(url, resp, unique_pages, word_count_data, longest_url, longest_word_count):
    """Scraper function to extract valid links and update statistics"""
    global max_words_count, max_words_page_url, tracked_urls, word_frequency
    
    # Initialize data from saved output or passed parameters
    if not tracked_urls:
        tracked_urls = unique_pages
        load_from_output()
    if not word_frequency:
        word_frequency = word_count_data
    if not max_words_page_url:
        max_words_page_url = longest_url
    if max_words_count == 0:
        max_words_count = longest_word_count
    
    # Handle response errors
    if not handle_response_error(resp):
        return []

    # Check content size (Error 607)
    if resp.raw_response and 'content-length' in resp.raw_response.headers:
        content_length = int(resp.raw_response.headers['content-length'])
        if content_length > 10_000_000:  # 10MB limit example
            print(f"Error 607: Content too large ({content_length} bytes)")
            return []

    extracted_links = extract_next_links(url, resp)
    valid_links = []
    i = 0
    while i < len(extracted_links):
        link = extracted_links[i]
        if validate_url(link):
            valid_links.append(link)
        i += 1
    tracked_urls.update(valid_links)
    
    # Periodically save statistics to output.txt
    global update_interval
    update_interval = (update_interval + 1) % 50
    if update_interval == 0:
        save_to_output()
    
    return valid_links

def extract_next_links(url, resp):
    """Extract links from the page, process text, and update statistics"""
    global max_words_count, max_words_page_url, tracked_urls, word_frequency, update_interval

    extracted_links = []

    if resp.status == 200 and resp.raw_response and resp.raw_response.content:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        text_content = soup.get_text().lower()
        words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text_content) if word not in stopwords and not word.isdigit()]
        
        # Calculate simhash to detect similar pages
        current_simhash = Simhash(text_content)
        if simhash_index.get_near_dups(current_simhash):
            print(f"Skipping similar page: {url}")
            return []
        else:
            simhash_index.add(url, current_simhash)

        # Update page with the most words
        word_count = len(words)
        if word_count > max_words_count:
            max_words_count = word_count
            max_words_page_url = url

        # Update word frequency
        for word in words:
            word_frequency[word] = word_frequency.get(word, 0) + 1

        # Extract all hyperlinks from the page
        anchors = soup.find_all('a', href=True)
        i = 0
        while i < len(anchors):
            abs_url, _ = urldefrag(urljoin(url, anchors[i]['href']))
            extracted_links.append(abs_url)
            i += 1

    return extracted_links


def validate_url(url):
    """Check if a URL is valid for crawling"""
    global php_trap_counter, redirect_tracking

    try:
        parsed = urlparse(url)
        allowed_subdomains = {"ics", "cs", "informatics", "stat"}
        allowed_domains = {"uci.edu"}
        netloc_parts = parsed.netloc.split('.')

        # Check if the domain and subdomain are valid
        if len(netloc_parts) < 2:
            return False
        if redirect_tracking[url] > 5:
            return False

        redirect_tracking[url] += 1

        domain = ".".join(netloc_parts[-2:])
        if domain not in allowed_domains:
            return False
        if len(netloc_parts) > 2 and netloc_parts[-3] not in allowed_subdomains:
            return False

        # Additional filtering to avoid traps and unwanted patterns
        if repeating_path(parsed.path):
            return False
        if parsed.path.count("/") > 5:
            return False
        if re.search(r'\d{4}-\d{2}', url):
            return False
        if url in tracked_urls:
            return False
        if parsed.query.count("%") >= 3 or parsed.query.count("=") >= 3 or parsed.query.count("&") >= 3:
            return False
        if parsed.scheme not in {"http", "https"}:
            return False
        if re.match(r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|pdf|docx|pptx|exe|zip|rar|gz)$", parsed.path.lower()):
            return False

        # Check for PHP trap URLs
        php_url = url.strip().split(".php")[0] + ".php"
        if php_trap_counter[php_url] > 10:
            return False
        php_trap_counter[php_url] += 1

        if url_trap_counter[parsed.netloc + parsed.path] > 10:
            return False
        url_trap_counter[parsed.netloc + parsed.path] += 1

        return True
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return False

def repeating_path(path):
    """Detect repeating segments within a given path"""
    path_segments = path.strip("/").split('/')
    
    # Check if a segment is followed by itself
    for i in range(len(path_segments) - 1):
        if path_segments[i] == path_segments[i + 1]:
            return True
    
    # Count segment occurrences and identify potential traps
    segment_frequency = {}
    for segment in path_segments:
        segment_frequency[segment] = segment_frequency.get(segment, 0) + 1
        if segment_frequency[segment] >= 3:
            return True

    return False

def print_statistics():
    """Display and save crawler statistics"""
    stats_header = "\nCrawler Statistics Overview:"
    unique_pages_info = f"Total number of unique pages visited: {len(tracked_urls)}"
    longest_page_info = f"URL of the page with the most words: {max_words_page_url}"
    word_count_info = f"Word count of the longest page: {max_words_count}"
    
    print(stats_header)
    print(unique_pages_info)
    print(longest_page_info)
    print(word_count_info)
    print("Top 10 frequent words found:")

    top_10_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:10]
    for word, count in top_10_words:
        print(f"{word}: {count}")
        
    # Save the output to a file
    save_to_output()
