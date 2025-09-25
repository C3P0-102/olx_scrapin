import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import random
from urllib.parse import urljoin
import logging
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('olx_scraper.log'),
        logging.StreamHandler()
    ]
)

class OLXScraper:
    def __init__(self):
        self.base_url = "https://www.olx.in"
        self.search_url = "https://www.olx.in/spare-parts_c1585/q-car-covers"
        self.session = requests.Session()
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        self.session.headers.update(self.headers)
        
    def get_page(self, url, retries=5):
        for attempt in range(retries):
            try:
                time.sleep(random.uniform(2, 5))
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                logging.info(f"Successfully fetched {url}")
                return response
            except requests.exceptions.Timeout as e:
                logging.warning(f"Timeout on attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch {url} after {retries} timeout attempts")
                    return None
                time.sleep(random.uniform(5, 10))
            except requests.exceptions.ConnectionError as e:
                logging.warning(f"Connection error on attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == retries - 1:
                    logging.error(f"Failed to connect to {url} after {retries} attempts")
                    return None
                time.sleep(random.uniform(3, 8))
            except requests.RequestException as e:
                logging.warning(f"Request failed on attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == retries - 1:
                    logging.error(f"Failed to fetch {url} after {retries} attempts")
                    return None
                time.sleep(random.uniform(2, 6))
        return None

    def extract_listing_data(self, listing_element):
        try:
            data = {}
            
            title_elem = listing_element.find('span', {'data-aut-id': 'itemTitle'})
            data['title'] = title_elem.get_text(strip=True) if title_elem else 'N/A'
            
            price_elem = listing_element.find('span', {'data-aut-id': 'itemPrice'})
            data['price'] = price_elem.get_text(strip=True) if price_elem else 'N/A'
            
            location_elem = listing_element.find('span', {'data-aut-id': 'item-location'})
            data['location'] = location_elem.get_text(strip=True) if location_elem else 'N/A'
            
            date_elem = listing_element.find('span', {'data-aut-id': 'itemDate'})
            data['date'] = date_elem.get_text(strip=True) if date_elem else 'N/A'
            
            link_elem = listing_element.find('a', href=True)
            if link_elem:
                data['link'] = urljoin(self.base_url, link_elem['href'])
            else:
                data['link'] = 'N/A'
            
            img_elem = listing_element.find('img')
            data['image_url'] = img_elem.get('src', 'N/A') if img_elem else 'N/A'
            
            desc_elem = listing_element.find('span', {'data-aut-id': 'itemDescription'})
            data['description'] = desc_elem.get_text(strip=True) if desc_elem else 'N/A'
            
            return data
            
        except Exception as e:
            logging.error(f"Error extracting listing data: {str(e)}")
            return None

    def scrape_listings(self, max_pages=5):
        all_listings = []
        
        logging.info(f"Starting to scrape OLX car cover listings...")
        
        for page in range(1, max_pages + 1):
            if page == 1:
                url = self.search_url
            else:
                url = f"{self.search_url}?page={page}"
            
            logging.info(f"Scraping page {page}: {url}")
            
            response = self.get_page(url)
            if not response:
                continue
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            listing_containers = (
                soup.find_all('div', {'data-aut-id': 'itemBox'}) or
                soup.find_all('div', class_=lambda x: x and 'item' in x.lower()) or
                soup.find_all('li', {'data-aut-id': 'itemBox'}) or
                soup.find_all('article') or
                soup.find_all('div', class_=lambda x: x and any(
                    term in str(x).lower() if x else False for term in ['listing', 'ad', 'card', '_1MUx', 'UdGd']
                ))
            )
            
            if not listing_containers:
                logging.warning(f"No listings found on page {page}")
                listing_containers = soup.find_all('div', class_=lambda x: x and any(
                    term in x.lower() for term in ['listing', 'ad', 'item', 'card']
                ))
            
            page_listings = []
            for container in listing_containers:
                listing_data = self.extract_listing_data(container)
                if listing_data and listing_data['title'] != 'N/A':
                    if self.is_actual_car_cover(listing_data):
                        page_listings.append(listing_data)
            
            all_listings.extend(page_listings)
            logging.info(f"Found {len(page_listings)} listings on page {page}")
            
            if not page_listings:
                logging.info(f"No more listings found. Stopping at page {page}")
                break
        
        logging.info(f"Total listings scraped: {len(all_listings)}")
        return all_listings

    def is_actual_car_cover(self, listing_data):
        title = listing_data['title'].lower()
        price_text = listing_data['price'].lower()
        
        exclude_keywords = ['bhk', 'bathroom', 'sqft', 'flat', 'parking', 'rent', 'sale', 'villa', 'office']
        include_keywords = ['cover', 'body cover', 'car cover', 'waterproof', 'dust proof']
        
        if any(keyword in title for keyword in exclude_keywords):
            return False
            
        if any(keyword in title for keyword in include_keywords):
            try:
                price_num = int(''.join(filter(str.isdigit, listing_data['price'])))
                if 100 <= price_num <= 10000:
                    return True
            except:
                pass
    def simple_scrape_fallback(self):
        """Fallback method using basic text parsing when CSS selectors fail"""
        try:
            response = self.get_page(self.search_url)
            if not response:
                return []
            
            content = response.text
            listings = []
            
            import re
            price_pattern = r'₹\s*[\d,]+'
            prices = re.findall(price_pattern, content)
            
            lines = content.split('\n')
            current_listing = {}
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                    
                if '₹' in line and any(keyword in line.lower() for keyword in ['cover', 'body']):
                    if 'bhk' not in line.lower() and 'parking' not in line.lower():
                        price_match = re.search(price_pattern, line)
                        if price_match:
                            price = price_match.group()
                            try:
                                price_num = int(''.join(filter(str.isdigit, price)))
                                if 100 <= price_num <= 10000:
                                    title = line.replace(price, '').strip()
                                    if len(title) > 5:
                                        listings.append({
                                            'title': title,
                                            'price': price,
                                            'location': 'N/A',
                                            'date': 'N/A',
                                            'link': 'N/A',
                                            'image_url': 'N/A',
                                            'description': 'N/A'
                                        })
                            except ValueError:
                                continue
            
            logging.info(f"Fallback method found {len(listings)} listings")
            return listings[:20]
            
        except Exception as e:
            logging.error(f"Fallback method failed: {str(e)}")
            return []

    def save_to_csv(self, listings, filename=None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"olx_car_covers_{timestamp}.csv"
        
        if not listings:
            logging.warning("No listings to save")
            return filename
            
        fieldnames = ['title', 'price', 'location', 'date', 'link', 'image_url', 'description']
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(listings)
            
            logging.info(f"Data saved to {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Error saving to CSV: {str(e)}")
            return None

    def save_to_json(self, listings, filename=None):
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"olx_car_covers_{timestamp}.json"
        
        if not listings:
            logging.warning("No listings to save")
            return filename
            
        try:
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(listings, jsonfile, indent=2, ensure_ascii=False)
            
            logging.info(f"Data saved to {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Error saving to JSON: {str(e)}")
            return None

def main():
    scraper = OLXScraper()
    
    try:
        print("Starting OLX Car Cover Scraper...")
        print("Testing connection first...")
        
        test_response = scraper.get_page(scraper.base_url)
        if not test_response:
            print("❌ Cannot connect to OLX. Possible reasons:")
            print("1. Internet connection issues")
            print("2. OLX is blocking requests")
            print("3. Site is temporarily down")
            print("\nTrying alternative approach...")
            
            scraper.headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
            test_response = scraper.get_page(scraper.base_url)
            
        if test_response:
            print("✅ Connection successful!")
            print(f"Response status: {test_response.status_code}")
            print(f"Content length: {len(test_response.content)} bytes")
        
        listings = scraper.scrape_listings(max_pages=2)
        
        if not listings:
            print("🔄 Trying fallback method...")
            listings = scraper.simple_scrape_fallback()
        
        if listings:
            csv_file = scraper.save_to_csv(listings)
            json_file = scraper.save_to_json(listings)
            
            print(f"\n✅ Scraping completed!")
            print(f"Total listings found: {len(listings)}")
            print(f"CSV file: {csv_file}")
            print(f"JSON file: {json_file}")
            
            print(f"\nFirst 3 listings preview:")
            for i, listing in enumerate(listings[:3], 1):
                print(f"\n{i}. {listing['title']}")
                print(f"   Price: {listing['price']}")
                print(f"   Location: {listing['location']}")
                print(f"   Date: {listing['date']}")
                print(f"   Link: {listing['link']}")
        else:
            print("❌ No listings found. Troubleshooting:")
            print("1. Check if OLX structure changed")
            print("2. Try using a VPN")
            print("3. OLX may be using JavaScript to load content")
            print("4. Consider using Selenium for JavaScript-heavy sites")
            
            print("\n💡 Alternative solutions:")
            print("1. Use Selenium WebDriver:")
            print("   pip install selenium")
            print("   # Add WebDriver code to handle JavaScript")
            print("\n2. Try different search terms:")
            print("   https://www.olx.in/cars_c84/accessories_c1563")
            print("\n3. Use OLX API (if available)")
            
    except KeyboardInterrupt:
        print("\n⏹️  Scraping stopped by user")
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        print(f"❌ An error occurred: {str(e)}")
        print("Check olx_scraper.log for detailed error information")

if __name__ == "__main__":
    main()