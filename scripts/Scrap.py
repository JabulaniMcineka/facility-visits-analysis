import requests
from bs4 import BeautifulSoup

# URL of the website we want to scrape
url = 'https://tut.ac.za/latest-news'

# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    print("Request successful")

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <h2> elements on the page
    headlines = soup.find_all('h2')

    # Print each headline
    for headline in headlines:
        print(headline.text.strip())
else:
    print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
