"""
Airflow DAG for S3 Asset Discovery
Checks for new assets in S3 every 1 minute and saves them to the database
REPLACES: Real-time S3 event triggers (SNS/SQS webhooks)

This DAG:
1. Loads all enabled S3 connectors from the database
2. Discovers assets from S3 using discover_s3_assets()
3. Compares discovered assets with existing ones
4. Saves new assets to the database
5. Sends notifications about newly discovered assets
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add the backend directory to Python path so we can import modules
backend_path = os.path.join(os.path.dirname(__file__), '..', '..', 'backend')
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    's3_asset_discovery',
    default_args=default_args,
    description='Discovers new S3 assets every 1 minute (replaces real-time event triggers)',
    schedule_interval='*/1 * * * *',  # Every 1 minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'asset-discovery', 'data-catalog', 'realtime-replacement'],
)

def check_new_s3_assets(**context):
    """
    Main function to discover and save new S3 assets
    This replaces the real-time SNS/SQS event triggers
    """
    import sys
    import os
    import requests
    
    # Ensure we can import from backend
    backend_path = os.path.join(os.path.dirname(__file__), '..', '..', 'backend')
    if backend_path not in sys.path:
        sys.path.insert(0, backend_path)
    
    try:
        # Import required modules
        from api.s3 import discover_s3_assets  # type: ignore
        import db_helpers  # type: ignore
        from datetime import datetime
        
        print("=" * 60)
        print("Starting S3 Asset Discovery (Airflow DAG)")
        print(f"Time: {datetime.now().isoformat()}")
        print("This replaces real-time S3 event triggers")
        print("=" * 60)
        
        # Load all connectors from database
        connectors = db_helpers.load_connectors()
        print(f"Found {len(connectors)} total connectors")
        
        # Filter for enabled S3 connectors only
        s3_connectors = [
            c for c in connectors 
            if c.get('type') == 'Amazon S3' and c.get('enabled', False)
        ]
        
        if not s3_connectors:
            print("No enabled S3 connectors found. Skipping discovery.")
            return {
                'status': 'skipped',
                'reason': 'no_connectors',
                'new_assets': 0,
                'total_discovered': 0
            }
        
        print(f"Processing {len(s3_connectors)} enabled S3 connector(s)")
        
        # Load existing assets from database
        print("\n📚 Loading existing assets from database...")
        existing_assets = db_helpers.load_assets()
        
        # Create a set of existing asset IDs for fast lookup
        existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
        
        # Also create a dict mapping asset_id -> full asset data for metadata comparison
        existing_assets_dict = {
            asset.get('id'): asset 
            for asset in existing_assets 
            if asset.get('id')
        }
        
        print(f"  ✓ Found {len(existing_asset_ids)} existing assets in database")
        
        total_new_assets = 0
        total_discovered = 0
        total_updated = 0
        connector_results = []
        all_new_assets_for_notification = []  # Collect all new assets for notifications
        
        # Process each S3 connector
        for connector in s3_connectors:
            connector_name = connector.get('name', 'Unknown')
            connector_id = connector.get('id', 'Unknown')
            config = connector.get('config', {})
            
            print(f"\n{'='*60}")
            print(f"Processing connector: {connector_name} (ID: {connector_id})")
            print(f"{'='*60}")
            
            # Get S3 credentials from connector config
            access_key_id = config.get('access_key_id') or config.get('accessKeyId')
            secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
            region = config.get('region', 'us-east-1')
            bucket_name = config.get('bucketName') or config.get('bucket_name')
            
            if not access_key_id or not secret_access_key:
                print(f"  ⚠️  Skipping: Missing AWS credentials")
                continue
            
            try:
                # Discover assets from S3 (includes ALL metadata)
                print(f"\n🔍 Discovering assets from S3...")
                if bucket_name:
                    print(f"   Bucket: {bucket_name}")
                else:
                    print(f"   Region: {region} (all buckets)")
                
                # This function automatically retrieves:
                # - Technical metadata (size, storage class, content type, ETag, last_modified)
                # - Operational metadata (status, owner, access info)
                # - Business metadata (description, tags, classification)
                discovered_assets = discover_s3_assets(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    region=region,
                    bucket_name=bucket_name
                )
                
                print(f"  ✓ Discovered {len(discovered_assets)} assets from S3")
                total_discovered += len(discovered_assets)
                
                # Compare discovered vs existing assets
                print(f"\n🔎 Comparing discovered assets with existing ones...")
                
                new_assets = []
                updated_assets = []
                
                for discovered_asset in discovered_assets:
                    asset_id = discovered_asset.get('id')
                    
                    if not asset_id:
                        print(f"  ⚠️  Skipping asset with no ID: {discovered_asset.get('name', 'Unknown')}")
                        continue
                    
                    # COMPARISON: Check if asset exists by ID
                    if asset_id not in existing_asset_ids:
                        # NEW ASSET: This asset doesn't exist in database
                        new_assets.append(discovered_asset)
                        print(f"  🆕 NEW: {asset_id}")
                        
                    else:
                        # EXISTING ASSET: Check if metadata has changed
                        existing_asset = existing_assets_dict.get(asset_id)
                        
                        if existing_asset:
                            # Compare metadata to see if asset was updated
                            discovered_last_modified = discovered_asset.get('technical_metadata', {}).get('last_modified')
                            existing_last_modified = existing_asset.get('technical_metadata', {}).get('last_modified')
                            
                            # Also compare ETag (unique file identifier) and size
                            discovered_etag = discovered_asset.get('technical_metadata', {}).get('etag')
                            existing_etag = existing_asset.get('technical_metadata', {}).get('etag')
                            
                            discovered_size = discovered_asset.get('size_bytes') or discovered_asset.get('technical_metadata', {}).get('size_bytes')
                            existing_size = existing_asset.get('size_bytes') or existing_asset.get('technical_metadata', {}).get('size_bytes')
                            
                            # If metadata changed, mark for update
                            if (discovered_etag != existing_etag or 
                                discovered_size != existing_size or
                                discovered_last_modified != existing_last_modified):
                                updated_assets.append(discovered_asset)
                                print(f"  🔄 UPDATED: {asset_id}")
                                if discovered_etag != existing_etag:
                                    print(f"     ETag changed: {existing_etag} → {discovered_etag}")
                                if discovered_size != existing_size:
                                    print(f"     Size changed: {existing_size} → {discovered_size}")
                            else:
                                print(f"  ✓ Unchanged: {asset_id}")
                
                print(f"\n📊 Comparison Results:")
                print(f"   • New assets: {len(new_assets)}")
                print(f"   • Updated assets: {len(updated_assets)}")
                print(f"   • Unchanged: {len(discovered_assets) - len(new_assets) - len(updated_assets)}")
                
                # Save new and updated assets with full metadata
                saved_count = 0
                updated_count = 0
                failed_count = 0
                saved_assets_for_notification = []
                
                # Process new assets
                for asset in new_assets:
                    try:
                        # Ensure required fields are set
                        asset['connector_id'] = connector_id
                        asset['discovered_at'] = datetime.utcnow().isoformat() + 'Z'
                        asset['status'] = 'active'
                        
                        # The asset already contains ALL metadata from discover_s3_assets():
                        # - technical_metadata (size, storage_class, content_type, etag, etc.)
                        # - operational_metadata (status, owner, access_count, etc.)
                        # - business_metadata (description, tags, classification, etc.)
                        
                        # Save to database (all metadata is preserved in extra_data field)
                        if db_helpers.save_asset(asset):
                            saved_count += 1
                            # Add to our tracking sets so we don't duplicate in same run
                            existing_asset_ids.add(asset.get('id'))
                            
                            # Prepare asset data for notification
                            saved_assets_for_notification.append({
                                'asset_id': asset.get('id'),
                                'id': asset.get('id'),
                                'name': asset.get('name'),
                                'type': asset.get('type'),
                                'catalog': asset.get('catalog'),
                                'connector_id': connector_id,
                            })
                            print(f"    ✓ Saved: {asset.get('name', 'Unknown')[:50]}...")
                        else:
                            failed_count += 1
                            print(f"    ✗ Failed to save: {asset.get('name', 'Unknown')[:50]}")
                            
                    except Exception as e:
                        failed_count += 1
                        print(f"    ✗ Error saving {asset.get('name', 'Unknown')[:50]}: {str(e)}")
                
                # Process updated assets
                for asset in updated_assets:
                    try:
                        asset['connector_id'] = connector_id
                        # Update discovered_at to reflect this is a new discovery
                        asset['discovered_at'] = datetime.utcnow().isoformat() + 'Z'
                        asset['status'] = 'active'
                        
                        # Save will update existing asset with new metadata
                        if db_helpers.save_asset(asset):
                            updated_count += 1
                        else:
                            failed_count += 1
                            
                    except Exception as e:
                        failed_count += 1
                        print(f"    ✗ Error updating {asset.get('name', 'Unknown')[:50]}: {str(e)}")
                
                total_new_assets += saved_count
                total_updated += updated_count
                
                # Collect assets for notification (only new ones)
                if saved_assets_for_notification:
                    all_new_assets_for_notification.extend(saved_assets_for_notification)
                
                print(f"\n💾 Save Results:")
                print(f"   • New assets saved: {saved_count}")
                print(f"   • Assets updated: {updated_count}")
                print(f"   • Failed: {failed_count}")
                
                connector_results.append({
                    'connector_id': connector_id,
                    'connector_name': connector_name,
                    'status': 'success',
                    'new_assets': saved_count,
                    'updated_assets': updated_count,
                    'discovered': len(discovered_assets),
                    'failed': failed_count
                })
                
            except Exception as e:
                print(f"  ❌ Error processing connector {connector_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                
                connector_results.append({
                    'connector_id': connector_id,
                    'connector_name': connector_name,
                    'status': 'error',
                    'error': str(e),
                    'new_assets': 0,
                    'discovered': 0
                })
        
        # Send notifications for all newly discovered assets
        if all_new_assets_for_notification:
            print(f"\n🔔 Sending notifications for {len(all_new_assets_for_notification)} new asset(s)...")
            try:
                # Get backend URL from environment or use default
                backend_url = os.environ.get('BACKEND_URL', 'http://localhost:8099')
                
                # Group assets by connector for better notifications
                connectors_processed = {}
                for asset in all_new_assets_for_notification:
                    conn_id = asset.get('connector_id', 'unknown')
                    if conn_id not in connectors_processed:
                        connectors_processed[conn_id] = {
                            'connector_id': conn_id,
                            'connector_name': next((c.get('name', 'S3') for c in s3_connectors if c.get('id') == conn_id), 'S3'),
                            'assets': []
                        }
                    connectors_processed[conn_id]['assets'].append(asset)
                
                # Send notifications for each connector
                for conn_data in connectors_processed.values():
                    notification_url = f"{backend_url}/api/s3/airflow-notification"
                    notification_payload = {
                        'assets': conn_data['assets'],
                        'connector_name': conn_data['connector_name'],
                        'connector_id': conn_data['connector_id']
                    }
                    
                    try:
                        response = requests.post(
                            notification_url,
                            json=notification_payload,
                            timeout=10,
                            headers={'Content-Type': 'application/json'}
                        )
                        
                        if response.status_code == 200:
                            print(f"  ✓ Notifications sent for {conn_data['connector_name']}: {len(conn_data['assets'])} asset(s)")
                        else:
                            print(f"  ⚠️  Failed to send notifications (HTTP {response.status_code}): {response.text[:100]}")
                    except requests.exceptions.RequestException as e:
                        print(f"  ⚠️  Failed to send notifications (connection error): {str(e)}")
                        # Don't fail the DAG if notifications fail
                        
            except Exception as e:
                print(f"  ⚠️  Error sending notifications: {str(e)}")
                import traceback
                traceback.print_exc()
                # Don't fail the DAG if notifications fail
        
        # Print final summary
        print("\n" + "=" * 60)
        print("📋 S3 Asset Discovery Complete")
        print("=" * 60)
        print(f"Connectors processed: {len(s3_connectors)}")
        print(f"Total discovered: {total_discovered}")
        print(f"New assets saved: {total_new_assets}")
        print(f"Assets updated: {total_updated}")
        print(f"Notifications sent: {len(all_new_assets_for_notification)}")
        print("=" * 60)
        
        return {
            'status': 'success',
            'connectors_processed': len(s3_connectors),
            'total_discovered': total_discovered,
            'new_assets': total_new_assets,
            'updated_assets': total_updated,
            'notifications_sent': len(all_new_assets_for_notification),
            'connector_results': connector_results
        }
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

# Define the task
discover_task = PythonOperator(
    task_id='discover_new_s3_assets',
    python_callable=check_new_s3_assets,
    dag=dag,
)


