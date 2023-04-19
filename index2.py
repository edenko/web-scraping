import time
import requests
import mysql.connector
from db_connection import get_database_connection
from bs4 import BeautifulSoup
from itertools import zip_longest
from datetime import datetime

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

    image_urls = soup.select('div.img > img[src]')
    titles = soup.select('h3.tit.ellipsis')
    dates = soup.select('p.sub.mb30 > span.date')
    venues = soup.select('p.sub.mb30 > span.where')
    hrefs = soup.select('li > a[href^="javascript:fn_view"]')

    for title, date, image_url, venue, href in zip_longest(titles, dates, image_urls, venues, hrefs):
        if image_url and not image_url['src'].startswith('http'):
            image_url = 'https://busandabom.net' + image_url['src']
        elif image_url:
            image_url = image_url['src']
        else:
            image_url = None

        venue_text = venue.text.strip()

        res_no = href['href'].split("'")[1] if href else None
        href_url = f'https://busandabom.net/play/view.nm?lang=ko&url=play&menuCd=&res_no={res_no}&veiwParam=Y' if res_no else None

        all_info.append((title.text, date.text, image_url, venue_text, href_url))

# Connect to the MySQL server
connection = get_database_connection()
cursor = connection.cursor()

# Iterate through the titles and image URLs and save them to the database
for event in all_info:
    title, date, image_url, venue, href_url = event

    # date
    if "~" in date:
        start, end = date.split('~')
        started_at = datetime.strptime(start, '%Y.%m.%d').date()
        ended_at = datetime.strptime(end, '%Y.%m.%d').date()
    else:
        started_at = datetime.strptime(date, '%Y.%m.%d').date()
        ended_at = None
    created_at = time.strftime('%Y-%m-%d %H:%M:%S')

    # Check for duplicates
    duplicate_check_query = "SELECT * FROM events WHERE region=%s AND title=%s AND venue=%s AND started_at=%s"
    cursor.execute(duplicate_check_query, ('부산', title, venue, started_at))
    duplicates = cursor.fetchall()
    
    if not duplicates:
        insert_query = "INSERT INTO events (region, title, image_url, venue, started_at, ended_at, duration, created_at, href_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, ('부산', title, image_url, venue, started_at, ended_at, None, created_at, href_url))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Scraped data has been saved to the database.")
