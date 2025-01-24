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
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.drive_saver = SavingOnDrive(credentials_dict)
        self.drive_saver.authenticate()
        self.folder_id = self.get_or_create_yesterday_folder()

    def get_or_create_yesterday_folder(self):
        folder_id = self.drive_saver.get_folder_id(self.yesterday)
        if not folder_id:
            folder_id = self.drive_saver.create_folder(self.yesterday)
            print(f"Created new folder '{self.yesterday}'")
        else:
            print(f"Using existing folder '{self.yesterday}'")
        return folder_id

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
                    print(f"Created empty Excel file for {category_name}")
                    return filename

                for brand in brand_data:
                    yesterday_cars = self.filter_yesterday_data(brand.get('available_cars', []))
                    if yesterday_cars:
                        df = pd.DataFrame(yesterday_cars)
                        sheet_name = "".join(x for x in brand['brand_title'] if x.isalnum())[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        print(f"No data for brand {brand['brand_title']} from yesterday")
            print(f"Successfully created Excel file: {filename}")
            return filename
        except Exception as e:
            print(f"Error saving Excel file for {category_name}: {str(e)}")
            return None

    def upload_to_drive(self, file_name):
        try:
            self.drive_saver.upload_file(file_name, self.folder_id)
            print(f"Uploaded {file_name} to Google Drive.")
        except Exception as e:
            print(f"Error uploading {file_name} to Google Drive: {str(e)}")

    async def process_category(self, category_name, category_data):
        all_brand_data = []
        url, num_pages, specific_brands, specific_pages = category_data

        try:
            print(f"Scraping data from URL: {url} with {num_pages} pages")
            scraper = CarScraper(url, num_pages, specific_brands, specific_pages)
            brand_data = await scraper.scrape_brands_and_types()
            if brand_data:
                all_brand_data.extend(brand_data)
        except Exception as e:
            print(f"Error processing URL {url}: {str(e)}")

        excel_file = self.save_to_excel(category_name, all_brand_data)
        if excel_file:
            self.upload_to_drive(excel_file)

    async def run(self, automotives_data):
        print("Starting the scraping process...")
        for category_name, data in automotives_data.items():
            print(f"\nProcessing category: {category_name}")
            await self.process_category(category_name, data)
        print("\nScraping process completed!")

def main():
    automotives_data = {
        "مكاتب السيارات": ("https://www.q84sale.com/ar/automotive/car-offices", 1, ["الركن الدولى 3 للسيارات", "يوركار", "الريان 1 للسيارات"], 2),
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
#         """Get or create the folder named after yesterday's date."""
#         folder_id = self.drive_saver.get_folder_id(self.yesterday)
#         if not folder_id:
#             folder_id = self.drive_saver.create_folder(self.yesterday)
#             print(f"Created new folder '{self.yesterday}'")
#         else:
#             print(f"Using existing folder '{self.yesterday}'")
#         return folder_id

#     def filter_yesterday_data(self, cars_data):
#         """Filter car data to only include entries from yesterday."""
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
#         """Save data to Excel file with multiple sheets based on brand titles."""
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
#         """Upload the Excel file to the Drive folder named after yesterday."""
#         try:
#             self.drive_saver.upload_file(file_name, self.folder_id)
#             print(f"Uploaded {file_name} to Google Drive.")
#         except Exception as e:
#             print(f"Error uploading {file_name} to Google Drive: {str(e)}")

#     async def process_category(self, category_name, category_data):
#         """Process a category of URLs with their respective page numbers and save results to Excel."""
#         all_brand_data = []
#         url, num_pages = category_data

#         try:
#             print(f"Scraping data from URL: {url} with {num_pages} pages")
#             scraper = CarScraper(url, num_pages)
#             brand_data = await scraper.scrape_brands_and_types()
#             if brand_data:
#                 all_brand_data.extend(brand_data)
#         except Exception as e:
#             print(f"Error processing URL {url}: {str(e)}")

#         # Save the data to Excel and upload it to Google Drive
#         excel_file = self.save_to_excel(category_name, all_brand_data)
#         if excel_file:
#             self.upload_to_drive(excel_file)

#     async def run(self, automotives_data):
#         """Main method to run the scraper."""
#         print("Starting the scraping process...")
#         for category_name, urls in automotives_data.items():
#             print(f"\nProcessing category: {category_name}")
#             await self.process_category(category_name, urls)
#         print("\nScraping process completed!")

# def main():
#     automotives_data = {
#         # "الوكالات": ("https://www.q84sale.com/ar/automotive/dealerships", 3),
#         # "دراجات": ("https://www.q84sale.com/ar/automotive/bikes", 5),
#         "مكاتب تأجير السيارات": ("https://www.q84sale.com/ar/automotive/car-rental", 1),
#         # "مكاتب السيارات": ("https://www.q84sale.com/ar/automotive/car-offices", 2),
#         "خدمات المحركات": ("https://www.q84sale.com/ar/automotive/automotive-services", 2),
#     }

#     # Load Google Drive credentials from environment variable
#     credentials_json = os.environ.get('HIERARCHIAL_GCLOUD_KEY_JSON')
#     if not credentials_json:
#         raise EnvironmentError("Google Drive credentials not found in environment variable.")
#     credentials_dict = json.loads(credentials_json)

#     scraper = HierarchialMainScraper(credentials_dict)
#     asyncio.run(scraper.run(automotives_data))


# if __name__ == "__main__":
#     main()
