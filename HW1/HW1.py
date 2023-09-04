from bs4 import BeautifulSoup
import requests
from random import randint
# from time import sleep
import time
from html.parser import  HTMLParser

USER_AGENT = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}


class SearchEngine:

    @staticmethod
    def search(query, sleep=True):
        if sleep: # Prevents loading too many pages too soon
            time.sleep(randint(10, 100))
        temp_url = '+'.join(query.split()) #for adding + between words for the query
        url = 'http://www.bing.com/search?q=' + temp_url
        soup = BeautifulSoup(requests.get(url, headers=USER_AGENT).text,"html.parser")
        new_results = SearchEngine.scrape_search_result(soup)
        return new_results

    @staticmethod
    def scrape_search_result(soup):
        raw_results = soup.find_all(" [“li”, attrs = {“class” : “b_algo”}] ")
        results = []

        for result in raw_results:
            link = result.get('href')
            results.append(link)
        return results

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(SearchEngine.search("How do you replace coolant thermostat"))