import time
import requests
import mysql.connector
from db_connection import get_database_connection
from bs4 import BeautifulSoup
from datetime import datetime
from itertools import zip_longest

# Scrape the first page to get the total number of items
first_page_url = 'https://busandabom.net/play/list.nm?page=1&sort=1'
response = requests.get(first_page_url)
content = response.content
soup = BeautifulSoup(content, 'html.parser')

listform_div = soup.find('div', class_='listform')
total_items_text = listform_div.text.split()[1]
total_items = int(total_items_text[:-1])

items_per_page = 10
total_pages = (total_items + items_per_page - 1) // items_per_page

print("Total items:", total_items)
print("Total pages:", total_pages)

# Scrape all the titles and dates from the pages
all_info = []

for page in range(1, total_pages + 1):
    url = f'https://busandabom.net/play/list.nm?page={page}&sort=1'
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    titles = soup.select('h3.tit.ellipsis')
    dates = soup.select('p.sub.mb30 > span.date')
    image_urls = soup.select('div.img.imgposter0 > img[src]')
    venues = soup.select('p.sub.mb30 > span.where')

    print(f"Page {page}:")
    print(f"  Titles: {len(titles)}, Dates: {len(dates)}, Image URLs: {len(image_urls)}, Venues: {len(venues)}")

    for title, date, image_url, venue in zip_longest(titles, dates, image_urls, venues):
        print(title.text)