import logging
import re
from urllib.parse import urlparse
from corpus import Corpus
import os
import lxml.html
from jellyfish import jaro_winkler, damerau_levenshtein_distance

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier):
        self.frontier = frontier
        self.corpus = Corpus()
        self.most_links = (None,0) #keeps track of page with most valid out links
        self.traps = set()
        
        ##next 3 attributes are used to compare consecutive links in regards to trap detection
        self.old_link = None
        self.old_path = None
        self.old_query = None

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        f = open("analytics.txt","w+")
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.fetch_url(url)
            
            count = 0
            for next_link in self.extract_next_links(url_data):
                if self.corpus.get_file_name(next_link) is not None:
                    if self.is_valid(next_link):
                        count += 1
                        self.old_link = next_link
                        self.frontier.add_url(next_link)
            if count > self.most_links[1]:
                self.most_links = (url_data["url"], count)
                    
        ##WRITING TO ANALYTICS FILE
        f.write("1)Domain Count\n"+"-"*50+"\n")
        for key, val in self.frontier.domains.items():
            f.write("{}:\t{}\n".format(key,val))
        f.write("\n2)Most links:\t{}\n\n".format(self.most_links[0]))
        f.write("3)Downloaded\n"+"-"*50+"\n")
        for d in self.frontier.urls_set:
            f.write("{}\n".format(d))
        f.write("\nTraps\n"+"-"*50+"\n")
        for t in self.traps:
            f.write("{}\n".format(t))
        f.close()
            
    def fetch_url(self, url):
        """
        This method, using the given url, should find the corresponding file in the corpus and return a dictionary
        containing the url, content of the file in binary format and the content size in bytes
        :param url: the url to be fetched
        :return: a dictionary containing the url, content and the size of the content. If the url does not
        exist in the corpus, a dictionary with content set to None and size set to 0 can be returned.
        """
        url_data = {
            "url": url,
            "content": None,
            "size": 0
        }
        corpus_file = self.corpus.get_file_name(url)
        if corpus_file != None:
            cf = open(corpus_file, "rb")
            url_data["content"] = cf.read()
            url_data["size"] = os.path.getsize(corpus_file)
            cf.close()
        return url_data

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        
        outputLinks = []
        #parses doc from given string(binary content)
        doc = lxml.html.document_fromstring(url_data["content"])
        #changes all links in doc to absolute form
        doc.make_links_absolute(url_data["url"], resolve_base_href=True)
        #checks for links
        for e, attr, link, p in doc.iterlinks():
            if attr == "href":
                outputLinks.append(link)
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        ##damerau-levenshtein distance counts transpositions between two strings
        if (damerau_levenshtein_distance(str(self.old_link),str(url))) == 1:
            self.traps.add(url)
            return False
        if (parsed.path == self.old_path):
            if parsed.query:
                ##jaro_winkler returns a string-edit distance that gives a float in [0,1] where
                #  0 represents two completely dissimilar strings and 1 represents identical strings
                if (jaro_winkler(parsed.query,self.old_query) >= .85):
                    self.traps.add(url)
                    return False
        self.old_path = parsed.path
        self.old_query = parsed.query
        if len(url) > 200:
            self.traps.add(url)
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

