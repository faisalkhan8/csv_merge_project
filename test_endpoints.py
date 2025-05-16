import requests
import json
from datetime import datetime
import time

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {message}")

def test_sam_endpoint(api_key=None):
    """Test SAM.gov FAC API endpoints"""
    base_urls = [
        "https://api.sam.gov/data-services/v3/fac/single_audits/general",
        "https://api.sam.gov/data-services/v3/fac/single_audits/findings",
        "https://api.sam.gov/data-services/v3/fac/single_audits/findings_text",
        "https://api.sam.gov/data-services/v3/fac/single_audits/corrective_action_plans"
    ]
    
    headers = {
        'accept': 'application/json',
        'x-api-key': api_key or 'DEMO_KEY'
    }
    
    params = {
        'size': 1,
        'from': 0,
        'auditYear': 2024
    }
    
    results = []
    for url in base_urls:
        try:
            log_message(f"Testing endpoint: {url}")
            response = requests.get(url, headers=headers, params=params)
            log_message(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                log_message(f"✅ {url} is working")
                results.append((True, url, data))
            else:
                log_message(f"❌ {url} failed: {response.text}")
                results.append((False, url, response.text))
                
        except Exception as e:
            log_message(f"❌ Error for {url}: {str(e)}")
            results.append((False, url, str(e)))
        
        time.sleep(1)  # Add delay between requests
    
    return results

def main():
    log_message("Starting SAM.gov FAC API verification...")
    log_message("Note: Using DEMO_KEY - limited access. Register at https://sam.gov/data-services/")
    
    results = test_sam_endpoint()
    
    success_count = sum(1 for r in results if r[0])
    log_message(f"\nResults: {success_count}/{len(results)} endpoints successful")
    
    for success, url, data in results:
        if success:
            print(f"\nEndpoint: {url}")
            print("Sample data structure:")
            print(json.dumps(data, indent=2)[:200] + "...")
    
    log_message("\nTo access the full API:")
    log_message("1. Register at https://sam.gov/data-services/")
    log_message("2. Request an API key")
    log_message("3. Update config.yaml with your API key")

if __name__ == "__main__":
    main()