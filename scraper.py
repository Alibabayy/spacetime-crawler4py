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
    """Thread-safe function to save statistics to output.txt"""
    with file_lock:
        try:
            with open("output.txt", 'w') as f:
                # Write total unique pages count
                f.write(f"Unique pages: {len(tracked_urls)}\n")
                
                # Write page with most words
                f.write(f"Page with most words: {max_words_page_url} (word count: {max_words_count})\n")
                
                # Write top 50 words
                top_words = dict(sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:50])
                f.write(f"Top 50 words: {top_words}\n")
                
                # Write subdomain statistics for .ics.uci.edu
                subdomains = Counter()
                for url in tracked_urls:
                    parsed_url = urlparse(url)
                    if parsed_url.netloc.endswith('.ics.uci.edu'):
                        subdomains[parsed_url.netloc] += 1
                
                for subdomain, count in sorted(subdomains.items()):
                    f.write(f"{subdomain}, {count}\n")
        except Exception as e:
            print(f"Error writing to output.txt: {e}")

def load_from_output():
    """Function to restore statistics from output.txt"""
    global word_frequency, max_words_page_url, max_words_count
    
    try:
        with open('output.txt', 'r') as f:
            lines = f.readlines()
            for line in reversed(lines):
                # Restore word frequency
                if line.startswith("Top 50 words:"):
                    dict_str = line.replace("Top 50 words: ", "").strip()
                    word_frequency = eval(dict_str)
                
                # Restore longest page information
                elif line.startswith("Page with most words:"):
                    parts = line.strip().split(' ')
                    max_words_page_url = parts[4]
                    max_words_count = int(parts[-1])
                    break
    except FileNotFoundError:
        # Initialize with empty values if file doesn't exist
        word_frequency = {}
        max_words_count = 0
        max_words_page_url = ""
    except Exception as e:
        print(f"Error reading from output.txt: {e}")
        word_frequency = {}
        max_words_count = 0
        max_words_page_url = ""

def scraper(url, resp, unique_pages, word_count_data, longest_url, longest_word_count):
    """Scraper function to extract valid links and update statistics"""
    global max_words_count, max_words_page_url, tracked_urls, word_frequency
    
    # Initialize data from saved output or passed parameters
    if len(tracked_urls) == 0:
        tracked_urls = unique_pages
        load_from_output()
    if len(word_frequency) == 0:
        word_frequency = word_count_data
    if max_words_page_url == "":
        max_words_page_url = longest_url
    if max_words_count == 0:
        max_words_count = longest_word_count

    extracted_links = extract_next_links(url, resp)
    valid_links = [link for link in extracted_links if is_valid(link)]
    tracked_urls.update(valid_links)
    
    # Periodically save statistics to output.txt
    global update_interval
    if update_interval >= 50:
        save_to_output()
        update_interval = 0
    else:
        update_interval += 1
    
    return valid_links

def extract_next_links(url, resp):
    """Extract links from the page, process text, and update statistics"""
    global max_words_count, max_words_page_url, tracked_urls, word_frequency, update_interval
    
    extracted_links = []

    if resp.status == 200:
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')
        text = soup.get_text().lower()
        words = [word for word in re.findall(r"\b[a-zA-Z]{2,}\b", text) if word not in stopwords and not word.isdigit()]
        
        # Calculate simhash and avoid similar pages
        current_hash = Simhash(text)
        if simhash_index.get_near_dups(current_hash):
            print(f"Skipping similar page: {url}")
            return []
        simhash_index.add(url, current_hash)

        # Update page with the most words
        if len(words) > max_words_count:
            max_words_count = len(words)
            max_words_page_url = url

        # Update word frequency
        for word in words:
            word_frequency[word] = word_frequency.get(word, 0) + 1

        # Extract all links from the page
        for anchor in soup.find_all('a', href=True):
            abs_url, _ = urldefrag(urljoin(url, anchor['href']))
            extracted_links.append(abs_url)

    return extracted_links

def is_valid(url):
    """Check if a URL is valid for crawling"""
    global php_trap_counter, redirect_tracking

    try:
        parsed = urlparse(url)
        allowed_subdomains = ["ics", "cs", "informatics", "stat"]
        allowed_domains = ["uci.edu"]
        netloc_parts = parsed.netloc.split('.')

        # Ensure valid domain and subdomain
        if len(netloc_parts) < 2:
            return False
        if redirect_tracking[url] > 5:
            return False
        else:
            redirect_tracking[url] += 1

        domain = ".".join(netloc_parts[-2:])
        if domain in allowed_domains:
            if len(netloc_parts) > 2 and netloc_parts[-3] not in allowed_subdomains:
                return False
        else:
            return False

        # Additional filtering for traps and unwanted patterns
        if repeating_path(parsed.path):
            return False
        if len(parsed.path.split("/")) > 5:
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
        php_url = url.strip().split(".php")[0] + ".php"
        if php_trap_counter[php_url] > 10:
            return False
        else:
            php_trap_counter[php_url] += 1
        if url_trap_counter[parsed.netloc + parsed.path] > 10:
            return False
        else:
            url_trap_counter[parsed.netloc + parsed.path] += 1

        return True
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return False

def repeating_path(path):
    """Detect repeating segments in the path"""
    segments = path.strip("/").split('/')
    for i in range(len(segments) - 1):
        if segments[i] == segments[i + 1]:
            return True
    segment_freq = {}
    for segment in segments:
        if segment not in segment_freq:
            segment_freq[segment] = 1
        else:
            segment_freq[segment] += 1
            if segment_freq[segment] >= 3:
                return True
    return False

