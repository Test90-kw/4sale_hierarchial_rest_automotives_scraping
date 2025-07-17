# Import standard libraries
import os  # for file system operations
import json  # for working with JSON data
import time  # for delay/sleep during retries
import socket  # for handling network-related exceptions
import ssl  # for SSL exceptions
import logging  # for logging messages

# Import Google API and auth-related modules
from google.oauth2.service_account import Credentials  # for authenticating using service account credentials
from googleapiclient.discovery import build  # for building the Google Drive service
from googleapiclient.http import MediaFileUpload  # for uploading files
from datetime import datetime, timedelta  # for date handling
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type  # for retry mechanisms
from googleapiclient.errors import HttpError  # for catching Drive-specific errors


class SavingOnDrive:
    def __init__(self, credentials_dict):
        # Initialize with credentials dictionary and define Drive API scopes
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Full access to Google Drive
        self.service = None  # Will hold the Drive service object after authentication
        self.parent_folder_ids = [  # List of parent folder IDs where files will be saved
            '1PBrE4Qfage1WgcS_rRjNpO7hW50emOaT',
            '1mLRdYvZb56LS10M0hjpzYTVrQiyWGN7m'
        ]
        self.logger = logging.getLogger(__name__)  # Logger instance
        self.setup_logging()  # Setup logging on initialization
        
    def setup_logging(self):
        """Configure logging to both console and file."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # Log to console
                logging.FileHandler("drive_upload.log")  # Log to a file
            ]
        )
        self.logger.setLevel(logging.INFO)  # Set logging level to INFO

    def authenticate(self):
        """Authenticate with Google Drive API with retry logic."""
        try:
            # Create credentials from service account dictionary
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Google Drive service
            self.service = build('drive', 'v3', credentials=creds, num_retries=3)
            self.logger.info("Successfully authenticated with Google Drive")
        except Exception as e:
            # Log any authentication errors
            self.logger.error(f"Authentication error: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=4, max=10),  # Exponential backoff between retries
        retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError))  # Retry only on network-related errors
    )
    def get_or_create_folder(self, folder_name, parent_folder_id):
        """Get or create folder in specified parent folder with retries."""
        try:
            # Construct query to search for folder
            query = (f"name='{folder_name}' and "
                    f"'{parent_folder_id}' in parents and "
                    f"mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false")
            
            # Execute the search query
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])  # Extract matching folders
            if files:
                # Folder already exists
                self.logger.info(f"Found existing folder '{folder_name}' in parent {parent_folder_id}")
                return files[0]['id']
            
            # Folder does not exist; create a new one
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            self.logger.info(f"Created new folder '{folder_name}' in parent {parent_folder_id}")
            return folder.get('id')
        except HttpError as e:
            # Handle specific Drive API errors
            if e.resp.status == 404:
                self.logger.error(f"Parent folder not found (ID: {parent_folder_id})")
                return None
            self.logger.error(f"Error getting/creating folder: {e}")
            raise
        except Exception as e:
            # Log and re-raise any other unexpected exceptions
            self.logger.error(f"Error in get_or_create_folder: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=4, max=10),  # Exponential backoff between retries
        retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError))  # Retry on network-related errors
    )
    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive with retries."""
        try:
            # Check if file exists before attempting upload
            if not os.path.exists(file_name):
                self.logger.error(f"File not found: {file_name}")
                return None

            # Check for valid folder ID
            if not folder_id:
                self.logger.error(f"Invalid folder ID for file {file_name}")
                return None

            # Metadata for the file to be uploaded
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            
            # Prepare media upload object (in chunks of 1MB)
            media = MediaFileUpload(
                file_name,
                resumable=True,
                chunksize=1024*1024
            )
            
            # Upload file to Drive
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            self.logger.info(f"Successfully uploaded {file_name} to folder {folder_id}")
            return file.get('id')
            
        except Exception as e:
            # Log and re-raise any upload error
            self.logger.error(f"Error uploading file {file_name}: {str(e)}")
            raise

    def save_files(self, files):
        """Save files to all valid Google Drive folders."""
        try:
            # Get yesterday's date formatted as a string (used as folder name)
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Loop through each parent folder
            for parent_folder_id in self.parent_folder_ids:
                # Create or get sub-folder for yesterday inside current parent folder
                folder_id = self.get_or_create_folder(yesterday, parent_folder_id)
                if not folder_id:
                    self.logger.error(f"Skipping uploads to parent folder {parent_folder_id}")
                    continue
                
                # Loop through each file to upload
                for file_name in files:
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries:
                        try:
                            # Try uploading the file
                            self.upload_file(file_name, folder_id)
                            break  # Success, break the retry loop
                        except Exception as e:
                            retry_count += 1
                            if retry_count == max_retries:
                                # Failed after max retries
                                self.logger.error(f"Failed to upload {file_name} after {max_retries} attempts")
                            else:
                                # Retry with exponential backoff
                                self.logger.info(f"Retrying upload of {file_name} (attempt {retry_count + 1})")
                                time.sleep(2 ** retry_count)
            
            self.logger.info("Files upload process completed")
            
        except Exception as e:
            # Log and re-raise any error that occurred in the overall upload process
            self.logger.error(f"Error in save_files: {str(e)}")
            raise
