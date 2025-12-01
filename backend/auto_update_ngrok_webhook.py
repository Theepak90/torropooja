
import time
import json
import urllib.request
import urllib.error
import sys
import argparse
import requests
from typing import Optional, Dict, Any

NGROK_API_URL = "http://localhost:4040/api/tunnels"
BACKEND_API_URL = "http://localhost:8099"
CHECK_INTERVAL = 30

class NgrokWebhookUpdater:
    def __init__(self, connector_id: Optional[str] = None, check_interval: int = 30):
        self.connector_id = connector_id
        self.check_interval = check_interval
        self.last_ngrok_url = None
        self.backend_url = BACKEND_API_URL
        
    def get_ngrok_url(self) -> Optional[str]:
        try:
            response = urllib.request.urlopen(NGROK_API_URL, timeout=2)
            data = json.loads(response.read())
            tunnels = data.get('tunnels', [])
            
            if not tunnels:
                return None
            
            https_tunnel = next((t for t in tunnels if t.get('proto') == 'https'), None)
            if https_tunnel:
                return https_tunnel['public_url']
            
            http_tunnel = tunnels[0]
            return http_tunnel['public_url']
            
        except urllib.error.URLError:
            return None
        except Exception as e:
            print(f"  Error getting ngrok URL: {e}")
            return None
    
    def get_s3_connector_id(self) -> Optional[str]:
        try:
            response = requests.get(f"{self.backend_url}/api/connectors", timeout=5)
            if response.status_code != 200:
                print(f" Failed to get connectors: HTTP {response.status_code}")
                return None
            
            connectors = response.json()
            s3_connector = next(
                (c for c in connectors if c.get('type') == 'Amazon S3' and c.get('enabled')),
                None
            )
            
            if s3_connector:
                return s3_connector.get('id')
            
            print("  No enabled S3 connector found")
            return None
            
        except requests.exceptions.RequestException as e:
            print(f" Error connecting to backend: {e}")
            return None
        except Exception as e:
            print(f" Error getting connector: {e}")
            return None
    
    def update_sns_subscription(self, ngrok_url: str) -> bool:
        connector_id = self.connector_id or self.get_s3_connector_id()
        
        if not connector_id:
            print(" No S3 connector ID available")
            return False
        
        try:
            print(f" Updating SNS subscription for connector {connector_id}...")
            print(f"   New webhook URL: {ngrok_url}/api/s3/sns-webhook")
            
            response = requests.post(
                f"{self.backend_url}/api/s3/setup-events",
                json={"connector_id": connector_id},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    print(f" Successfully updated SNS subscription!")
                    print(f"   Configured buckets: {result.get('configured_buckets', [])}")
                    return True
                else:
                    print(f" Setup failed: {result.get('error', 'Unknown error')}")
                    return False
            else:
                print(f" HTTP {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f" Error updating subscription: {e}")
            return False
        except Exception as e:
            print(f" Unexpected error: {e}")
            return False
    
    def check_and_update(self) -> bool:
        current_url = self.get_ngrok_url()
        
        if not current_url:
            if self.last_ngrok_url:
                print("  ngrok appears to have stopped (URL no longer available)")
                self.last_ngrok_url = None
            return False
        
        if current_url != self.last_ngrok_url:
            if self.last_ngrok_url is None:
                print(f" ngrok detected: {current_url}")
                print(f"   Initializing SNS subscription...")
            else:
                print(f" ngrok URL changed!")
                print(f"   Old: {self.last_ngrok_url}")
                print(f"   New: {current_url}")
            
            success = self.update_sns_subscription(current_url)
            if success:
                self.last_ngrok_url = current_url
                return True
            else:
                print("  Failed to update subscription, will retry on next check")
                return False
        
        return False
    
    def run_once(self) -> bool:
        return self.check_and_update()
    
    def run_continuous(self):
        print(" Starting ngrok webhook auto-updater...")
        print(f"   Check interval: {self.check_interval} seconds")
        if self.connector_id:
            print(f"   Connector ID: {self.connector_id}")
        else:
            print(f"   Connector ID: Auto-detect (first enabled S3 connector)")
        print(f"   Backend URL: {self.backend_url}")
        print("")
        print("Press Ctrl+C to stop")
        print("")
        
        try:
            while True:
                self.check_and_update()
                time.sleep(self.check_interval)
        except KeyboardInterrupt:
            print("\n Stopping auto-updater...")
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(
        description="Auto-update S3 SNS webhook when ngrok URL changes"
    )
    parser.add_argument(
        '--connector-id',
        type=str,
        help='S3 connector ID to update (default: auto-detect first enabled)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=30,
        help='Check interval in seconds (default: 30)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - run once and exit'
    )
    
    args = parser.parse_args()
    
    updater = NgrokWebhookUpdater(
        connector_id=args.connector_id,
        check_interval=args.interval
    )
    
    if args.test:
        print("🧪 Running in test mode...")
        success = updater.run_once()
        sys.exit(0 if success else 1)
    else:
        updater.run_continuous()

if __name__ == "__main__":
    main()

