import sys
import os

backend_path = os.path.join(os.path.dirname(__file__), 'backend')
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

def manual_discovery():
    try:
        from api.s3 import discover_s3_assets  # type: ignore
        import db_helpers  # type: ignore
        from datetime import datetime
        import requests
        
        print("=" * 60)
        print("Manual S3 Asset Discovery")
        print(f"Time: {datetime.now().isoformat()}")
        print("=" * 60)
        
        connectors = db_helpers.load_connectors()
        print(f"Found {len(connectors)} total connectors")
        
        s3_connectors = [
            c for c in connectors 
            if c.get('type') == 'Amazon S3' and c.get('enabled', False)
        ]
        
        if not s3_connectors:
            print(" No enabled S3 connectors found!")
            return
        
        print(f"Processing {len(s3_connectors)} enabled S3 connector(s)\n")
        
        existing_assets = db_helpers.load_assets()
        existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
        print(f"Found {len(existing_asset_ids)} existing assets in database\n")
        
        total_new = 0
        all_new_assets = []
        
        for connector in s3_connectors:
            connector_name = connector.get('name', 'Unknown')
            connector_id = connector.get('id', 'Unknown')
            config = connector.get('config', {})
            
            print(f"{'='*60}")
            print(f"Processing: {connector_name}")
            print(f"{'='*60}")
            
            access_key_id = config.get('access_key_id') or config.get('accessKeyId')
            secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
            region = config.get('region', 'us-east-1')
            bucket_name = config.get('bucketName') or config.get('bucket_name')
            
            if not access_key_id or not secret_access_key:
                print("    Missing AWS credentials")
                continue
            
            try:
                print(f"\n Discovering assets from S3...")
                discovered_assets = discover_s3_assets(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    region=region,
                    bucket_name=bucket_name
                )
                
                print(f"  ✓ Discovered {len(discovered_assets)} assets")
                
                new_assets = []
                for asset in discovered_assets:
                    asset_id = asset.get('id')
                    if asset_id and asset_id not in existing_asset_ids:
                        new_assets.append(asset)
                        existing_asset_ids.add(asset_id)
                
                print(f"  🆕 Found {len(new_assets)} NEW assets")
                
                saved_count = 0
                for asset in new_assets:
                    asset['connector_id'] = connector_id
                    asset['discovered_at'] = datetime.utcnow().isoformat() + 'Z'
                    asset['status'] = 'active'
                    
                    if db_helpers.save_asset(asset):
                        saved_count += 1
                        all_new_assets.append({
                            'asset_id': asset.get('id'),
                            'id': asset.get('id'),
                            'name': asset.get('name'),
                            'type': asset.get('type'),
                            'catalog': asset.get('catalog')
                        })
                
                print(f"   Saved {saved_count} new assets")
                total_new += saved_count
                
            except Exception as e:
                print(f"   Error: {e}")
                import traceback
                traceback.print_exc()
        
        if all_new_assets:
            print(f"\n Sending notifications for {len(all_new_assets)} new asset(s)...")
            try:
                backend_url = os.environ.get('BACKEND_URL', 'http://localhost:8099')
                response = requests.post(
                    f"{backend_url}/api/s3/airflow-notification",
                    json={
                        'assets': all_new_assets,
                        'connector_name': 'Amazon S3',
                        'connector_id': 'manual_trigger'
                    },
                    timeout=10
                )
                if response.status_code == 200:
                    print(f"   Notifications sent successfully!")
                else:
                    print(f"    Notification failed: {response.status_code}")
            except Exception as e:
                print(f"    Error sending notifications: {e}")
        else:
            print("\n No new assets found")
        
        print("\n" + "=" * 60)
        print(f" Summary: {total_new} new asset(s) discovered and saved")
        print("=" * 60)
        
    except Exception as e:
        print(f" Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    manual_discovery()


