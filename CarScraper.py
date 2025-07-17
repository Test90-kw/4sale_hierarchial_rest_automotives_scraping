import asyncio
from playwright.async_api import async_playwright
from DetailsScraper import DetailsScraping

class CarScraper:
    def __init__(self, url, num_pages=1, specific_brands=None, specific_pages=None):
        self.url = url  # The main URL to start scraping from
        self.num_pages = num_pages  # Default number of pages to scrape per brand
        self.specific_brands = specific_brands or []  # List of brands that require special page count
        self.specific_pages = specific_pages if specific_pages else num_pages  # Page count for specific brands
        self.data = []  # Holds the final scraped data

    async def scrape_brands_and_types(self):
        # Launch Playwright in asynchronous context
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Launch a headless Chromium browser
            page = await browser.new_page()  # Open a new tab/page
            await page.goto(self.url)  # Navigate to the main scraping URL

            # Select all anchor tags inside brand wrappers
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            # If no brand elements are found, exit early
            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            # Loop through each brand element found on the page
            for element in brand_elements:
                title = await element.get_attribute('title')  # Get brand name
                brand_link = await element.get_attribute('href')  # Get brand relative URL

                if brand_link:
                    # Construct full brand URL from relative path
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link
                    
                    # Decide how many pages to scrape for this brand
                    if title in self.specific_brands:
                        pages_to_scrape = self.specific_pages
                        print(f"Using {pages_to_scrape} pages for specific brand: {title}")
                    else:
                        pages_to_scrape = self.num_pages
                    
                    brand_data = []  # Stores car details for this brand
                    print(f"Full brand link: {full_brand_link}")

                    # Loop through each paginated page for this brand
                    for page_num in range(1, pages_to_scrape + 1):
                        paginated_link = f"{full_brand_link}/{page_num}"  # Construct the paginated URL
                        print(f"Scraping paginated link: {paginated_link}")

                        try:
                            # Use the DetailsScraping class to extract car details from each page
                            details_scraper = DetailsScraping(paginated_link)
                            car_details = await details_scraper.get_car_details()

                            if car_details:
                                brand_data.extend(car_details)  # Add details to brand list
                                print(f"Page {page_num}: Found {len(car_details)} cars")
                            else:
                                print(f"Page {page_num}: No cars found")
                                break  # If no cars found, stop scraping more pages
                        except Exception as e:
                            print(f"Error scraping {paginated_link}: {e}")
                            break  # Stop further scraping for this brand if error occurs

                    # Append the collected data for this brand
                    self.data.append({
                        'brand_title': title,  # Brand name
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # Template link with page number slot
                        'available_cars': brand_data,  # All collected car listings
                    })

                    print(f"Found brand: {title}, Data collected for {len(brand_data)} cars")

            await browser.close()  # Close the browser session
        return self.data  # Return the full dataset
