import os
import json
import time
import socket
import ssl
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class SavingOnDrive:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None
        self.parent_folder_id = '1wwVdI2kT2k_j_pScF13PDhm2hd9EvjRN'
        
    def authenticate(self):
        """Authenticate with Google Drive API with retry logic."""
        try:
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds, num_retries=3)
        except Exception as e:
            print(f"Authentication error: {e}")
            raise

    @retry(stop=stop_after_attempt(3), 
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder with retries."""
        try:
            query = (f"name='{folder_name}' and "
                    f"'{self.parent_folder_id}' in parents and "
                    f"mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false")
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"Error getting folder ID: {e}")
            raise

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def create_folder(self, folder_name):
        """Create a new folder in the parent folder with retries."""
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            return folder.get('id')
        except Exception as e:
            print(f"Error creating folder: {e}")
            raise

    @retry(stop=stop_after_attempt(3),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type((socket.error, ssl.SSLError, ConnectionError)))
    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive with retries."""
        try:
            if not os.path.exists(file_name):
                print(f"File not found: {file_name}")
                return None

            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            
            media = MediaFileUpload(
                file_name,
                resumable=True,
                chunksize=1024*1024  # 1MB chunks for better reliability
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            print(f"Successfully uploaded {file_name}")
            return file.get('id')
            
        except Exception as e:
            print(f"Error uploading file {file_name}: {str(e)}")
            raise

    def save_files(self, files, folder_id=None):
        """Save files to Google Drive with enhanced error handling."""
        try:
            if not folder_id:
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                folder_id = self.get_folder_id(yesterday)
                if not folder_id:
                    folder_id = self.create_folder(yesterday)
            
            for file_name in files:
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        self.upload_file(file_name, folder_id)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count == max_retries:
                            print(f"Failed to upload {file_name} after {max_retries} attempts")
                        else:
                            print(f"Retrying upload of {file_name} (attempt {retry_count + 1})")
                            time.sleep(2 ** retry_count)  # Exponential backoff
            
            print("Files upload process completed")
            
        except Exception as e:
            print(f"Error in save_files: {str(e)}")
            raise
