import time
import cfscrape
import mysql.connector
from db_connection import get_database_connection
from bs4 import BeautifulSoup
from datetime import datetime

# Initialize the scraper
scraper = cfscrape.create_scraper()

# Scrape the first page to get the total number of pages and posts
first_page_url = 'https://www.bscc.or.kr/01_perfor/?mcode=0401010200&mode=1&page=1'
response = scraper.get(first_page_url)
content = response.content
soup = BeautifulSoup(content, 'html.parser')

div_element = soup.find('div', class_='board_total')
total_posts_text = div_element.find_all('span')[0].text
total_pages_text = div_element.text.split('/')[-1].split()[0]

total_posts = int(total_posts_text)
total_pages = int(total_pages_text)

print("Total posts:", total_posts)
print("Total pages:", total_pages)

# Scrape all the titles and image URLs from the pages
all_info = []

for page in range(1, total_pages + 1):
    page_url = f'https://www.bscc.or.kr/01_perfor/?mcode=0401010200&mode=1&page={page}'
    response = scraper.get(page_url)
    content = response.content
    soup = BeautifulSoup(content, 'html.parser')

    titles = soup.select('h5 > a')
    images = soup.select('div.link_img > a > img')
    hrefs = soup.select('div.link_img > a')
    item_statuses = soup.select('dl.item-status')

    for title, image, href, item_status in zip(titles, images, hrefs, item_statuses):
        venue = item_status.find_all('dd')[0].text
        date = item_status.find_all('dd')[1].text
        duration = item_status.find_all('dd')[2].text
        image_url = image['src']
        if not image_url.startswith('http'):
            image_url = None
        all_info.append((title.text, image_url, venue, date, duration, 'https://www.bscc.or.kr/01_perfor/' + href['href']))

# Connect to the MySQL server
connection = get_database_connection()
cursor = connection.cursor()

# Fetch all events from the database
cursor.execute("SELECT region, title, venue, started_at FROM events")
existing_events = cursor.fetchall()

# Iterate through the titles and image URLs and save them to the database
for event in all_info:
    title, images, venue, date, duration, href = event

    # date
    if "~" in date:
        start, end = date.split(' ~ ')
        started_at = datetime.strptime(start, '%Y-%m-%d').date()
        ended_at = datetime.strptime(end, '%Y-%m-%d').date()
    else:
        started_at = datetime.strptime(date, '%Y-%m-%d').date()
        ended_at = None
    created_at = time.strftime('%Y-%m-%d %H:%M:%S')

    # Check for duplicates
    is_duplicate = False
    for existing_event in existing_events:
        if '부산' == existing_event[0].strip() and title.strip() == existing_event[1].strip() and venue.strip() == existing_event[2].strip() and started_at == existing_event[3]:
            is_duplicate = True
            break

    if not is_duplicate:
        insert_query = "INSERT INTO events (region, title, image_url, venue, started_at, ended_at, duration, created_at, href_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(insert_query, ('부산', title, images, venue, started_at, ended_at, duration, created_at, href))

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("Scraped data has been saved to the database.")