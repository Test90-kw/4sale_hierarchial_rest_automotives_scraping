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
        # Set up logging first
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        
        # Then initialize other attributes
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.drive_saver = SavingOnDrive(credentials_dict)
        self.drive_saver.authenticate()
        self.folder_ids = self.get_or_create_yesterday_folders()
        self.temp_dir = Path("temp_files")
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3
        self.upload_retry_delay = 15
        self.page_delay = 3
        self.chunk_delay = 10

    def setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('scraper.log')
            ]
        )
        self.logger.setLevel(logging.INFO)

    def get_or_create_yesterday_folders(self):
        """Create or get folders in both parent directories."""
        folder_ids = []
        for parent_id in self.drive_saver.parent_folder_ids:
            folder_id = self.drive_saver.get_or_create_folder(self.yesterday, parent_id)
            if folder_id:
                folder_ids.append(folder_id)
                self.logger.info(f"Using folder '{self.yesterday}' in parent {parent_id}")
        return folder_ids

    def filter_yesterday_data(self, cars_data):
        filtered_data = []
        for car in cars_data:
            try:
                car_date = datetime.strptime(car['date_published'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                if car_date == self.yesterday:
                    filtered_data.append(car)
            except (ValueError, TypeError):
                continue
        return filtered_data

    def save_to_excel(self, category_name, brand_data):
        filename = f"{category_name}.xlsx"
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                if not brand_data:
                    pd.DataFrame().to_excel(writer, sheet_name='No Data', index=False)
                    self.logger.info(f"Created empty Excel file for {category_name}")
                    return filename

                for brand in brand_data:
                    yesterday_cars = self.filter_yesterday_data(brand.get('available_cars', []))
                    if yesterday_cars:
                        df = pd.DataFrame(yesterday_cars)
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
        """Upload file to all valid folder locations."""
        if not self.folder_ids:
            self.logger.error("No valid folder IDs available for upload")
            return

        try:
            self.drive_saver.save_files([file_name])
            self.logger.info(f"Uploaded {file_name} to Google Drive folders")
        except Exception as e:
            self.logger.error(f"Error uploading {file_name} to Google Drive: {str(e)}")

    async def process_category(self, category_name, category_data):
        all_brand_data = []
        url, num_pages, specific_brands, specific_pages = category_data

        try:
            self.logger.info(f"Scraping data from URL: {url} with {num_pages} pages")
            scraper = CarScraper(url, num_pages, specific_brands, specific_pages)
            brand_data = await scraper.scrape_brands_and_types()
            if brand_data:
                all_brand_data.extend(brand_data)
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")

        excel_file = self.save_to_excel(category_name, all_brand_data)
        if excel_file:
            self.upload_to_drive(excel_file)
            try:
                os.remove(excel_file)
                self.logger.info(f"Cleaned up local file: {excel_file}")
            except Exception as e:
                self.logger.error(f"Error cleaning up {excel_file}: {e}")

    async def run(self, automotives_data):
        self.logger.info("Starting the scraping process...")
        for category_name, data in automotives_data.items():
            self.logger.info(f"\nProcessing category: {category_name}")
            await self.process_category(category_name, data)
        self.logger.info("\nScraping process completed!")

def main():
    automotives_data = {
        "مكاتب السيارات": ("https://www.q84sale.com/ar/automotive/car-offices", 1, ["الركن الدولي 3 للسيارات", "يوركار", "الريان  1 للسيارات", "القمة"], 2),
        "الوكالات": ("https://www.q84sale.com/ar/automotive/dealerships", 1, ["يوسف أحمد الغانم وأولاده"], 3),
        "دراجات": ("https://www.q84sale.com/ar/automotive/bikes", 1, ["دراجات نارية رياضية", "الدراجات الرباعية"], 5),
        "مكاتب تأجير السيارات": ("https://www.q84sale.com/ar/automotive/car-rental", 1, ["لا شئ"], 1),
        "خدمات المحركات": ("https://www.q84sale.com/ar/automotive/automotive-services", 1, ["خدمات السيارات", "سطحات", "برمجة ريموت"], 2),
    }

    credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
    if not credentials_json:
        raise EnvironmentError("Google Drive credentials not found in environment variable.")
    credentials_dict = json.loads(credentials_json)

    scraper = HierarchialMainScraper(credentials_dict)
    asyncio.run(scraper.run(automotives_data))

if __name__ == "__main__":
    main()


# import asyncio
# import pandas as pd
# import os
# import json
# import logging
# from datetime import datetime, timedelta
# from DetailsScraper import DetailsScraping
# from CarScraper import CarScraper
# from SavingOnDrive import SavingOnDrive
# from pathlib import Path
# from typing import Dict, List, Tuple
# from playwright.async_api import async_playwright

# class HierarchialMainScraper:
#     def __init__(self, credentials_dict):
#         self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#         self.drive_saver = SavingOnDrive(credentials_dict)
#         self.drive_saver.authenticate()
#         self.folder_id = self.get_or_create_yesterday_folder()

#     def get_or_create_yesterday_folder(self):
#         folder_id = self.drive_saver.get_folder_id(self.yesterday)
#         if not folder_id:
#             folder_id = self.drive_saver.create_folder(self.yesterday)
#             print(f"Created new folder '{self.yesterday}'")
#         else:
#             print(f"Using existing folder '{self.yesterday}'")
#         return folder_id

#     def filter_yesterday_data(self, cars_data):
#         filtered_data = []
#         for car in cars_data:
#             try:
#                 car_date = datetime.strptime(car['date_published'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
#                 if car_date == self.yesterday:
#                     filtered_data.append(car)
#             except (ValueError, TypeError):
#                 continue
#         return filtered_data

#     def save_to_excel(self, category_name, brand_data):
#         filename = f"{category_name}.xlsx"
#         try:
#             with pd.ExcelWriter(filename, engine='openpyxl') as writer:
#                 if not brand_data:
#                     pd.DataFrame().to_excel(writer, sheet_name='No Data', index=False)
#                     print(f"Created empty Excel file for {category_name}")
#                     return filename

#                 for brand in brand_data:
#                     yesterday_cars = self.filter_yesterday_data(brand.get('available_cars', []))
#                     if yesterday_cars:
#                         df = pd.DataFrame(yesterday_cars)
#                         sheet_name = "".join(x for x in brand['brand_title'] if x.isalnum())[:31]
#                         df.to_excel(writer, sheet_name=sheet_name, index=False)
#                     else:
#                         print(f"No data for brand {brand['brand_title']} from yesterday")
#             print(f"Successfully created Excel file: {filename}")
#             return filename
#         except Exception as e:
#             print(f"Error saving Excel file for {category_name}: {str(e)}")
#             return None

#     def upload_to_drive(self, file_name):
#         try:
#             self.drive_saver.upload_file(file_name, self.folder_id)
#             print(f"Uploaded {file_name} to Google Drive.")
#         except Exception as e:
#             print(f"Error uploading {file_name} to Google Drive: {str(e)}")

#     async def process_category(self, category_name, category_data):
#         all_brand_data = []
#         url, num_pages, specific_brands, specific_pages = category_data

#         try:
#             print(f"Scraping data from URL: {url} with {num_pages} pages")
#             scraper = CarScraper(url, num_pages, specific_brands, specific_pages)
#             brand_data = await scraper.scrape_brands_and_types()
#             if brand_data:
#                 all_brand_data.extend(brand_data)
#         except Exception as e:
#             print(f"Error processing URL {url}: {str(e)}")

#         excel_file = self.save_to_excel(category_name, all_brand_data)
#         if excel_file:
#             self.upload_to_drive(excel_file)

#     async def run(self, automotives_data):
#         print("Starting the scraping process...")
#         for category_name, data in automotives_data.items():
#             print(f"\nProcessing category: {category_name}")
#             await self.process_category(category_name, data)
#         print("\nScraping process completed!")

# def main():
#     automotives_data = {
#         "مكاتب السيارات": ("https://www.q84sale.com/ar/automotive/car-offices", 1, ["الركن الدولي 3 للسيارات", "يوركار", "الريان  1 للسيارات", "القمة"], 2),
#         "الوكالات": ("https://www.q84sale.com/ar/automotive/dealerships", 1, ["يوسف أحمد الغانم وأولاده"], 3),
#         "دراجات": ("https://www.q84sale.com/ar/automotive/bikes", 1, ["دراجات نارية رياضية", "الدراجات الرباعية"], 5),
#         "مكاتب تأجير السيارات": ("https://www.q84sale.com/ar/automotive/car-rental", 1, ["لا شئ"], 1),
#         "خدمات المحركات": ("https://www.q84sale.com/ar/automotive/automotive-services", 1, ["خدمات السيارات", "سطحات", "برمجة ريموت"], 2),
#     }

#     credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
#     if not credentials_json:
#         raise EnvironmentError("Google Drive credentials not found in environment variable.")
#     credentials_dict = json.loads(credentials_json)

#     scraper = HierarchialMainScraper(credentials_dict)
#     asyncio.run(scraper.run(automotives_data))

# if __name__ == "__main__":
#     main()
