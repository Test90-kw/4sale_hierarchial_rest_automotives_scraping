import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from CarScraper import CarScraper
from SavingOnDrive import SavingOnDrive
from pathlib import Path
from typing import Dict, List, Tuple
from playwright.async_api import async_playwright

class HierarchialMainScraper:
    def __init__(self, credentials_dict):
        # Set up logging first for the class
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        
        # Calculate yesterday's date (used for filtering listings and naming folders)
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Initialize Google Drive saving utility and authenticate
        self.drive_saver = SavingOnDrive(credentials_dict)
        self.drive_saver.authenticate()
        
        # Create or get yesterday's folders in the specified parent folders
        self.folder_ids = self.get_or_create_yesterday_folders()
        
        # Create a temp directory for storing Excel files
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)

        # Retry configuration for Drive uploads
        self.upload_retries = 3
        self.upload_retry_delay = 15

        # Delay configuration for scraping (optional throttling)
        self.page_delay = 3
        self.chunk_delay = 10

    def setup_logging(self):
        """Configure logging settings for the class."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # Console output
                logging.FileHandler('scraper.log')  # File log
            ]
        )
        self.logger.setLevel(logging.INFO)

    def get_or_create_yesterday_folders(self):
        """Create or retrieve folders in all parent directories for yesterday."""
        folder_ids = []
        for parent_id in self.drive_saver.parent_folder_ids:
            folder_id = self.drive_saver.get_or_create_folder(self.yesterday, parent_id)
            if folder_id:
                folder_ids.append(folder_id)
                self.logger.info(f"Using folder '{self.yesterday}' in parent {parent_id}")
        return folder_ids

    def filter_yesterday_data(self, cars_data):
        """Filter scraped car data to include only listings from yesterday."""
        filtered_data = []
        for car in cars_data:
            try:
                # Parse and compare date
                car_date = datetime.strptime(car['date_published'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                if car_date == self.yesterday:
                    filtered_data.append(car)
            except (ValueError, TypeError):
                continue  # Skip any malformed dates
        return filtered_data

    def save_to_excel(self, category_name, brand_data):
        """Save filtered data into an Excel file, with a sheet per brand."""
        filename = f"{category_name}.xlsx"
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # If no data at all, write an empty sheet
                if not brand_data:
                    pd.DataFrame().to_excel(writer, sheet_name='No Data', index=False)
                    self.logger.info(f"Created empty Excel file for {category_name}")
                    return filename

                # Write each brand to a separate sheet
                for brand in brand_data:
                    yesterday_cars = self.filter_yesterday_data(brand.get('available_cars', []))
                    if yesterday_cars:
                        df = pd.DataFrame(yesterday_cars)
                        # Clean and shorten sheet name to max 31 characters
                        sheet_name = "".join(x for x in brand['brand_title'] if x.isalnum())[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        self.logger.info(f"No data for brand {brand['brand_title']} from yesterday")
            self.logger.info(f"Successfully created Excel file: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving Excel file for {category_name}: {str(e)}")
            return None

    def upload_to_drive(self, file_name):
        """Upload a file to all Google Drive folders created for yesterday."""
        if not self.folder_ids:
            self.logger.error("No valid folder IDs available for upload")
            return

        try:
            self.drive_saver.save_files([file_name])
            self.logger.info(f"Uploaded {file_name} to Google Drive folders")
        except Exception as e:
            self.logger.error(f"Error uploading {file_name} to Google Drive: {str(e)}")

    async def process_category(self, category_name, category_data):
        """Process a single automotive category: scrape, save, upload, clean."""
        all_brand_data = []
        url, num_pages, specific_brands, specific_pages = category_data

        try:
            self.logger.info(f"Scraping data from URL: {url} with {num_pages} pages")
            # Initialize scraper for the category
            scraper = CarScraper(url, num_pages, specific_brands, specific_pages)
            # Run scraping logic
            brand_data = await scraper.scrape_brands_and_types()
            if brand_data:
                all_brand_data.extend(brand_data)
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")

        # Save to Excel and upload to Drive
        excel_file = self.save_to_excel(category_name, all_brand_data)
        if excel_file:
            self.upload_to_drive(excel_file)
            try:
                # Clean up local Excel file
                os.remove(excel_file)
                self.logger.info(f"Cleaned up local file: {excel_file}")
            except Exception as e:
                self.logger.error(f"Error cleaning up {excel_file}: {e}")

    async def run(self, automotives_data):
        """Main loop to process all automotive categories."""
        self.logger.info("Starting the scraping process...")
        for category_name, data in automotives_data.items():
            self.logger.info(f"\nProcessing category: {category_name}")
            await self.process_category(category_name, data)
        self.logger.info("\nScraping process completed!")

def main():
    # Dictionary of automotive categories to scrape: each contains
    # (URL, num_pages, list of specific brands, specific subpage number)
    automotives_data = {
        "مكاتب السيارات": ("https://www.q84sale.com/ar/automotive/car-offices", 1, ["الركن الدولي 3 للسيارات", "يوركار", "الريان  1 للسيارات", "القمة"], 2),
        "الوكالات": ("https://www.q84sale.com/ar/automotive/dealerships", 1, ["يوسف أحمد الغانم وأولاده"], 3),
        "دراجات": ("https://www.q84sale.com/ar/automotive/bikes", 1, ["دراجات نارية رياضية", "الدراجات الرباعية"], 5),
        "مكاتب تأجير السيارات": ("https://www.q84sale.com/ar/automotive/car-rental", 1, ["لا شئ"], 1),
        "خدمات المحركات": ("https://www.q84sale.com/ar/automotive/automotive-services", 1, ["خدمات السيارات", "سطحات", "برمجة ريموت"], 2),
    }

    # Load credentials from environment variable
    credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
    if not credentials_json:
        raise EnvironmentError("Google Drive credentials not found in environment variable.")
    credentials_dict = json.loads(credentials_json)

    # Instantiate the scraper and start the event loop
    scraper = HierarchialMainScraper(credentials_dict)
    asyncio.run(scraper.run(automotives_data))

# Entry point of the script
if __name__ == "__main__":
    main()

