#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import requests
from datetime import datetime
import os

def upload_sales_console():
    """
    Function to upload sales console file to API
    All data is hardcoded in this function
    
    Returns:
        dict: Response with success status and data/error
    """
    try:
        # ========== HARDCODED DATA - EDIT DI SINI ==========
        
        # API Configuration
        API_URL = 'https://dev-ofr.pinusmerahabadi.co.id/apiacc/sales-console/upload'
        TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NjYzNSwibGV2ZWwiOjIsImtvZGUiOiIiLCJuYW1lIjoic2VwdGlhbiIsImlhdCI6MTc2OTM5MzU4MX0.bQAwVyGsIEY_CtJklY649Fk0r7p8MFch1PJiac3a30g'
        
        # File Configuration
        FILE_PATH = 'C:/Users/user/Downloads/file sales console.xlsx'
        FILE_NAME = 'file sales console'
        FILE_TYPE = 'file-sales-console'
        DATE_REPORT = '2026-01-26'
        
        # ===================================================
        
        print(f"[UPLOAD] Starting upload process...", file=sys.stderr)
        print(f"[UPLOAD] File: {FILE_PATH}", file=sys.stderr)
        print(f"[UPLOAD] API: {API_URL}", file=sys.stderr)
        
        # Check if file exists
        if not os.path.exists(FILE_PATH):
            error_msg = f"File tidak ditemukan: {FILE_PATH}"
            print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
            return {
                'success': False,
                'error': error_msg,
                'timestamp': datetime.now().isoformat()
            }
        
        # Get file size for logging
        file_size = os.path.getsize(FILE_PATH)
        print(f"[UPLOAD] File size: {file_size} bytes ({file_size / 1024:.2f} KB)", file=sys.stderr)
        
        # Prepare headers
        headers = {
            'Authorization': f'Bearer {TOKEN}'
        }
        
        # Prepare form data
        form_data = {
            'name': FILE_NAME,
            'type': FILE_TYPE,
            'date_report': DATE_REPORT
        }
        
        # Open file and prepare multipart upload
        with open(FILE_PATH, 'rb') as file:
            files = {
                'master': (os.path.basename(FILE_PATH), file, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            }
            
            print(f"[UPLOAD] Sending request...", file=sys.stderr)
            
            # Make POST request
            response = requests.post(
                API_URL,
                headers=headers,
                data=form_data,
                files=files,
                timeout=300  # 5 minutes timeout untuk upload file
            )
        
        print(f"[UPLOAD] Response status code: {response.status_code}", file=sys.stderr)
        
        # Check if request was successful
        if response.status_code == 200 or response.status_code == 201:
            try:
                response_data = response.json()
            except:
                response_data = {'message': response.text}
            
            print(f"[UPLOAD] Upload successful!", file=sys.stderr)
            
            return {
                'success': True,
                'status_code': response.status_code,
                'data': response_data,
                'file_info': {
                    'name': FILE_NAME,
                    'path': FILE_PATH,
                    'size': file_size,
                    'type': FILE_TYPE,
                    'date_report': DATE_REPORT
                },
                'timestamp': datetime.now().isoformat(),
                'message': 'Upload file sales console berhasil'
            }
        else:
            print(f"[UPLOAD] Upload failed with status code: {response.status_code}", file=sys.stderr)
            
            # Try to parse error message from response
            try:
                error_data = response.json()
                error_message = error_data.get('message', 'Upload gagal')
            except:
                error_message = f"Upload gagal dengan status code: {response.status_code}"
            
            return {
                'success': False,
                'status_code': response.status_code,
                'error': error_message,
                'response_text': response.text[:500],  # First 500 chars of response
                'timestamp': datetime.now().isoformat()
            }
            
    except requests.exceptions.Timeout:
        error_msg = "Request timeout - Upload melebihi 5 menit"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except requests.exceptions.ConnectionError:
        error_msg = "Connection error - Tidak dapat terhubung ke API"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except FileNotFoundError:
        error_msg = f"File tidak ditemukan: {FILE_PATH}"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except PermissionError:
        error_msg = f"Permission denied: Tidak dapat membaca file {FILE_PATH}"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[UPLOAD ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'trace': str(e),
            'timestamp': datetime.now().isoformat()
        }


def main():
    """
    Main function - no input needed, all data is hardcoded
    """
    try:
        print(f"[WORKER] Sales Console Upload Worker Started", file=sys.stderr)
        print(f"[WORKER] Timestamp: {datetime.now().isoformat()}", file=sys.stderr)
        
        # Perform upload
        result = upload_sales_console()
        
        # Output result as JSON to stdout
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        # Exit with appropriate code
        if result['success']:
            print(f"[WORKER] Process completed successfully", file=sys.stderr)
            sys.exit(0)
        else:
            print(f"[WORKER] Process failed", file=sys.stderr)
            sys.exit(1)
            
    except Exception as e:
        error_result = {
            'success': False,
            'error': f'Worker error: {str(e)}',
            'trace': str(e),
            'timestamp': datetime.now().isoformat()
        }
        print(json.dumps(error_result), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()