#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import json
import requests
from datetime import datetime

def login(username, password, api_url):
    """
    Function to perform login to the API
    
    Args:
        username: Username for login
        password: Password for login
        api_url: API endpoint URL for login
    
    Returns:
        dict: Response with success status and data/error
    """
    try:
        # Prepare the payload
        payload = {
            'username': username,
            'password': password
        }
        
        # Set headers
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        print(f"[LOGIN] Attempting to login user: {username}", file=sys.stderr)
        print(f"[LOGIN] API URL: {api_url}", file=sys.stderr)
        
        # Make POST request to login endpoint
        response = requests.post(
            api_url,
            data=payload,
            headers=headers,
            timeout=30  # 30 seconds timeout
        )
        
        # Check if request was successful
        if response.status_code == 200:
            response_data = response.json()
            
            print(f"[LOGIN] Login successful for user: {username}", file=sys.stderr)
            
            return {
                'success': True,
                'status_code': response.status_code,
                'data': response_data,
                'timestamp': datetime.now().isoformat(),
                'message': 'Login berhasil'
            }
        else:
            print(f"[LOGIN] Login failed with status code: {response.status_code}", file=sys.stderr)
            
            # Try to parse error message from response
            try:
                error_data = response.json()
                error_message = error_data.get('message', 'Login gagal')
            except:
                error_message = f"Login gagal dengan status code: {response.status_code}"
            
            return {
                'success': False,
                'status_code': response.status_code,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
            
    except requests.exceptions.Timeout:
        error_msg = "Request timeout - API tidak merespons dalam 30 detik"
        print(f"[LOGIN ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except requests.exceptions.ConnectionError:
        error_msg = "Connection error - Tidak dapat terhubung ke API"
        print(f"[LOGIN ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"[LOGIN ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        print(f"[LOGIN ERROR] {error_msg}", file=sys.stderr)
        return {
            'success': False,
            'error': error_msg,
            'trace': str(e),
            'timestamp': datetime.now().isoformat()
        }


def main():
    """
    Main function to read input from stdin and perform login
    """
    try:
        # Read input from stdin (sent from Node.js)
        input_data = sys.stdin.read()
        
        # Parse JSON input
        payload = json.loads(input_data)
        
        # Extract required parameters
        username = payload.get('username')
        password = payload.get('password')
        api_url = payload.get('api_url', 'https://dev-ofr.pinusmerahabadi.co.id/apiacc/auth/login')
        
        # Validate required parameters
        if not username:
            raise ValueError("Username is required")
        
        if not password:
            raise ValueError("Password is required")
        
        print(f"[WORKER] Starting login process...", file=sys.stderr)
        
        # Perform login
        result = login(username, password, api_url)
        
        # Output result as JSON to stdout
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        # Exit with appropriate code
        if result['success']:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except json.JSONDecodeError as e:
        error_result = {
            'success': False,
            'error': f'Invalid JSON input: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }
        print(json.dumps(error_result), file=sys.stderr)
        sys.exit(1)
        
    except ValueError as e:
        error_result = {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }
        print(json.dumps(error_result), file=sys.stderr)
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