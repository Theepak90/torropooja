from flask import Blueprint, request, jsonify, abort, current_app
from werkzeug.exceptions import HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import AzureError
from datetime import datetime
import json
import os
import db_helpers

azure_blob_bp = Blueprint('azure_blob_bp', __name__)

class AzureBlobConnectionTest(BaseModel):
    account_name: str
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    container_name: Optional[str] = None
    connection_name: str

def discover_azure_blob_assets_structured(account_name: Optional[str] = None, account_key: Optional[str] = None, 
                                          connection_string: Optional[str] = None, 
                                          container_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Discover Azure Blob Storage assets and return structured data organized by containers.
    Returns a dictionary with containers and their assets.
    """
    containers_data = []
    all_assets = []
    
    try:
        if connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            if not account_name:
                for part in connection_string.split(';'):
                    if part.startswith('AccountName='):
                        account_name = part.split('=', 1)[1]
                        break
                if not account_name:
                    try:
                        account_name = blob_service_client.account_name
                    except:
                        pass
        elif account_name and account_key:
            account_url = f"https://{account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided")
        
        if not account_name:
            try:
                account_name = blob_service_client.account_name
            except:
                pass
        if not account_name:
            raise Exception("Unable to determine account name from connection string or credentials")
        
        if container_name:
            try:
                containers = [blob_service_client.get_container_client(container_name)]
            except AzureError:
                raise Exception(f"Container {container_name} not found")
        else:
            containers = blob_service_client.list_containers()
            containers = [blob_service_client.get_container_client(c.name) for c in containers]
        
        for container_client in containers:
            container_name_actual = container_client.container_name
            container_assets = []
            
            try:
                print(f"  📦 Listing blobs in container: {container_name_actual}")
                # Use include=['metadata'] to get all blobs with metadata
                blobs = container_client.list_blobs(include=['metadata'])
                blob_list = list(blobs)  # Convert iterator to list to ensure we get all blobs
                print(f"  ✅ Found {len(blob_list)} blob(s) in container {container_name_actual}")
                
                # Log all blob names for debugging
                blob_names = [blob.name for blob in blob_list]
                print(f"  📋 Blob names in container: {blob_names}")
                
                for blob in blob_list:
                    key = blob.name
                    size = blob.size or 0
                    last_modified = blob.last_modified or datetime.now()
                    content_type = blob.content_settings.content_type if blob.content_settings else ''
                    etag = blob.etag or ''
                    
                    asset_type = 'File'
                    if key.endswith('/'):
                        asset_type = 'Folder'
                    elif key.lower().endswith(('.csv', '.tsv', '.json', '.parquet', '.avro', '.orc')):
                        asset_type = 'Data File'
                    elif key.lower().endswith(('.sql', '.py', '.scala', '.r')):
                        asset_type = 'Script'
                    elif key.lower().endswith(('.txt', '.log')):
                        asset_type = 'Text File'
                    elif key.lower().endswith(('.zip', '.gz', '.tar', '.bz2')):
                        asset_type = 'Archive'
                    
                    try:
                        blob_client = container_client.get_blob_client(key)
                        blob_properties = blob_client.get_blob_properties()
                        metadata = blob_properties.metadata or {}
                    except Exception:
                        metadata = {}
                    
                    asset_id = f"https://{account_name}.blob.core.windows.net/{container_name_actual}/{key}"
                    
                    asset = {
                        "id": asset_id,
                        "name": key.split('/')[-1] if '/' in key else key,
                        "type": asset_type,
                        "catalog": container_name_actual,
                        "schema": '/'.join(key.split('/')[:-1]) if '/' in key else '',
                        "discovered_at": datetime.now().isoformat(),
                        "status": "active",
                        "description": f"Azure Blob in container {container_name_actual}",
                        "size_bytes": size,
                        "columns": [],
                        "technical_metadata": {
                            "asset_id": asset_id,
                            "asset_type": asset_type,
                            "location": asset_id,
                            "format": content_type or "Unknown",
                            "size_bytes": size,
                            "source_system": "Azure Blob Storage",
                            "account_name": account_name,
                            "container_name": container_name_actual,
                            "blob_name": key,
                            "etag": etag,
                            "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified)
                        },
                        "operational_metadata": {
                            "status": "active",
                            "owner": "Unknown",
                            "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified),
                            "last_accessed": datetime.now().isoformat(),
                            "access_count": "N/A",
                            "data_quality_score": 95
                        },
                        "business_metadata": {
                            "description": f"Azure Blob: {key}",
                            "business_owner": "Unknown",
                            "department": container_name_actual,
                            "classification": "internal",
                            "sensitivity_level": "low",
                            "tags": list(metadata.keys()) if metadata else []
                        }
                    }
                    
                    container_assets.append(asset)
                    all_assets.append(asset)
                
                containers_data.append({
                    "name": container_name_actual,
                    "asset_count": len(container_assets),
                    "assets": container_assets
                })
                
            except AzureError as e:
                if 'Forbidden' in str(e) or 'Access' in str(e):
                    print(f"  Access denied to container {container_name_actual}")
                    containers_data.append({
                        "name": container_name_actual,
                        "asset_count": 0,
                        "assets": [],
                        "error": "Access denied"
                    })
                else:
                    print(f"  Error listing blobs in container {container_name_actual}: {e}")
                    containers_data.append({
                        "name": container_name_actual,
                        "asset_count": 0,
                        "assets": [],
                        "error": str(e)
                    })
                continue
            except Exception as e:
                print(f"  Error listing blobs in container {container_name_actual}: {e}")
                containers_data.append({
                    "name": container_name_actual,
                    "asset_count": 0,
                    "assets": [],
                    "error": str(e)
                })
                continue
        
        return {
            "account_name": account_name,
            "total_containers": len(containers_data),
            "total_assets": len(all_assets),
            "containers": containers_data,
            "all_assets": all_assets
        }
    
    except AzureError as e:
        raise Exception(f"Azure Blob Storage API error: {str(e)}")
    except Exception as e:
        raise Exception(f"Error discovering Azure Blob Storage assets: {str(e)}")

def discover_azure_blob_assets(account_name: Optional[str] = None, account_key: Optional[str] = None, 
                                connection_string: Optional[str] = None, 
                                container_name: Optional[str] = None) -> List[Dict[str, Any]]:
    assets = []
    
    try:
        if connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            # Extract account name from connection string if not provided
            if not account_name:
                # Connection string format: DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...
                for part in connection_string.split(';'):
                    if part.startswith('AccountName='):
                        account_name = part.split('=', 1)[1]
                        break
                # If still not found, try to get from the blob service client
                if not account_name:
                    try:
                        account_name = blob_service_client.account_name
                    except:
                        pass
        elif account_name and account_key:
            account_url = f"https://{account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided")
        
        # Ensure we have account_name for asset ID construction
        if not account_name:
            # Try to get from blob service client
            try:
                account_name = blob_service_client.account_name
            except:
                pass
        if not account_name:
            raise Exception("Unable to determine account name from connection string or credentials")
        
        if container_name:
            try:
                containers = [blob_service_client.get_container_client(container_name)]
            except AzureError:
                raise Exception(f"Container {container_name} not found")
        else:
            containers = blob_service_client.list_containers()
            containers = [blob_service_client.get_container_client(c.name) for c in containers]
        
        for container_client in containers:
            container_name_actual = container_client.container_name
            
            try:
                print(f"  📦 Listing blobs in container: {container_name_actual}")
                # Use include=['metadata'] to get all blobs with metadata
                blobs = container_client.list_blobs(include=['metadata'])
                blob_list = list(blobs)  # Convert iterator to list to ensure we get all blobs
                print(f"  ✅ Found {len(blob_list)} blob(s) in container {container_name_actual}")
                
                # Log all blob names for debugging
                blob_names = [blob.name for blob in blob_list]
                print(f"  📋 Blob names in container: {blob_names}")
                
                for blob in blob_list:
                    key = blob.name
                    size = blob.size or 0
                    last_modified = blob.last_modified or datetime.now()
                    content_type = blob.content_settings.content_type if blob.content_settings else ''
                    etag = blob.etag or ''
                    
                    asset_type = 'File'
                    if key.endswith('/'):
                        asset_type = 'Folder'
                    elif key.lower().endswith(('.csv', '.tsv', '.json', '.parquet', '.avro', '.orc')):
                        asset_type = 'Data File'
                    elif key.lower().endswith(('.sql', '.py', '.scala', '.r')):
                        asset_type = 'Script'
                    elif key.lower().endswith(('.txt', '.log')):
                        asset_type = 'Text File'
                    elif key.lower().endswith(('.zip', '.gz', '.tar', '.bz2')):
                        asset_type = 'Archive'
                    
                    try:
                        blob_client = container_client.get_blob_client(key)
                        blob_properties = blob_client.get_blob_properties()
                        metadata = blob_properties.metadata or {}
                    except Exception:
                        metadata = {}
                    
                    asset_id = f"https://{account_name}.blob.core.windows.net/{container_name_actual}/{key}"
                    
                    asset = {
                        "id": asset_id,
                        "name": key.split('/')[-1] if '/' in key else key,
                        "type": asset_type,
                        "catalog": container_name_actual,
                        "schema": '/'.join(key.split('/')[:-1]) if '/' in key else '',
                        "discovered_at": datetime.now().isoformat(),
                        "status": "active",
                        "description": f"Azure Blob in container {container_name_actual}",
                        "size_bytes": size,
                        "columns": [],
                        "technical_metadata": {
                            "asset_id": asset_id,
                            "asset_type": asset_type,
                            "location": asset_id,
                            "format": content_type or "Unknown",
                            "size_bytes": size,
                            "source_system": "Azure Blob Storage",
                            "account_name": account_name,
                            "container_name": container_name_actual,
                            "blob_name": key,
                            "etag": etag,
                            "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified)
                        },
                        "operational_metadata": {
                            "status": "active",
                            "owner": "Unknown",
                            "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified),
                            "last_accessed": datetime.now().isoformat(),
                            "access_count": "N/A",
                            "data_quality_score": 95
                        },
                        "business_metadata": {
                            "description": f"Azure Blob: {key}",
                            "business_owner": "Unknown",
                            "department": container_name_actual,
                            "classification": "internal",
                            "sensitivity_level": "low",
                            "tags": list(metadata.keys()) if metadata else []
                        }
                    }
                    
                    assets.append(asset)
                    
            except AzureError as e:
                if 'Forbidden' in str(e) or 'Access' in str(e):
                    print(f"  Access denied to container {container_name_actual}")
                else:
                    print(f"  Error listing blobs in container {container_name_actual}: {e}")
                continue
            except Exception as e:
                print(f"  Error listing blobs in container {container_name_actual}: {e}")
                continue
    
    except AzureError as e:
        raise Exception(f"Azure Blob Storage API error: {str(e)}")
    except Exception as e:
        raise Exception(f"Error discovering Azure Blob Storage assets: {str(e)}")
    
    return assets

@azure_blob_bp.route('/containers', methods=['GET'])
def list_azure_containers():
    try:
        connector_id = request.args.get('connector_id')
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Azure Blob Storage'), None)
        if not connector:
            return jsonify({'error': 'Azure Blob connector not found'}), 404
        
        config = connector.get('config', {})
        account_name = config.get('account_name') or config.get('accountName')
        account_key = config.get('account_key') or config.get('accountKey')
        connection_string = config.get('connection_string') or config.get('connectionString')
        
        if not connection_string and not (account_name and account_key):
            return jsonify({'error': 'Connector missing Azure credentials'}), 400
        
        try:
            if connection_string:
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                account_url = f"https://{account_name}.blob.core.windows.net"
                blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
            
            containers = [c.name for c in blob_service_client.list_containers()]
            return jsonify({'containers': containers}), 200
        except AzureError as e:
            return jsonify({'error': f'Azure API error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@azure_blob_bp.route('/test-rediscovery', methods=['POST'])
def test_rediscovery():
    """Test endpoint to manually trigger rediscovery for debugging"""
    try:
        connectors = db_helpers.load_connectors()
        azure_connectors = [
            c for c in connectors 
            if c.get('type') == 'Azure Blob Storage' and c.get('enabled', False)
        ]
        
        if not azure_connectors:
            return jsonify({
                'status': 'error',
                'message': 'No enabled Azure Blob Storage connectors found'
            }), 400
        
        results = []
        for connector in azure_connectors:
            connector_id = connector.get('id')
            connector_name = connector.get('name')
            config = connector.get('config', {})
            
            account_name = config.get('account_name') or config.get('accountName')
            account_key = config.get('account_key') or config.get('accountKey')
            connection_string = config.get('connection_string') or config.get('connectionString')
            container_name = config.get('container_name') or config.get('containerName')
            
            try:
                print(f"\n{'='*60}")
                print(f"MANUAL REDISCOVERY TEST for: {connector_name}")
                print(f"{'='*60}")
                
                structured_data = discover_azure_blob_assets_structured(
                    account_name=account_name,
                    account_key=account_key,
                    connection_string=connection_string,
                    container_name=container_name
                )
                
                assets = structured_data.get('all_assets', [])
                containers = structured_data.get('containers', [])
                
                existing_assets = db_helpers.load_assets()
                existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
                
                new_assets = [a for a in assets if a.get('id') and a.get('id') not in existing_asset_ids]
                
                results.append({
                    'connector_name': connector_name,
                    'discovered': len(assets),
                    'new': len(new_assets),
                    'containers': len(containers),
                    'asset_names': [a.get('name') for a in assets]
                })
                
            except Exception as e:
                results.append({
                    'connector_name': connector_name,
                    'error': str(e)
                })
        
        return jsonify({
            'status': 'success',
            'results': results
        }), 200
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@azure_blob_bp.route('/trigger-discovery', methods=['POST'])
def trigger_discovery():
    try:
        print("=" * 60)
        print("Manual Azure Blob Storage Asset Discovery Triggered")
        print(f"Time: {datetime.now().isoformat()}")
        print("=" * 60)
        
        connectors = db_helpers.load_connectors()
        print(f"Found {len(connectors)} total connectors")
        
        azure_connectors = [
            c for c in connectors 
            if c.get('type') == 'Azure Blob Storage' and c.get('enabled', False)
        ]
        
        if not azure_connectors:
            return jsonify({
                'status': 'error',
                'message': 'No enabled Azure Blob Storage connectors found'
            }), 400
        
        print(f"Processing {len(azure_connectors)} enabled Azure Blob Storage connector(s)")
        
        existing_assets = db_helpers.load_assets()
        existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
        print(f"Found {len(existing_asset_ids)} existing assets in database")
        
        total_new = 0
        all_new_assets = []
        total_discovered = 0
        
        for connector in azure_connectors:
            connector_name = connector.get('name', 'Unknown')
            connector_id = connector.get('id', 'Unknown')
            config = connector.get('config', {})
            
            print(f"\nProcessing: {connector_name}")
            
            account_name = config.get('account_name') or config.get('accountName')
            account_key = config.get('account_key') or config.get('accountKey')
            connection_string = config.get('connection_string') or config.get('connectionString')
            container_name = config.get('container_name') or config.get('containerName')
            
            if not connection_string and not (account_name and account_key):
                print("Missing Azure credentials")
                continue
            
            try:
                print(f"Discovering assets from Azure Blob Storage...")
                print(f"  Account: {account_name or 'N/A (using connection string)'}")
                print(f"  Container: {container_name or 'All containers'}")
                
                discovered_assets = discover_azure_blob_assets(
                    account_name=account_name,
                    account_key=account_key,
                    connection_string=connection_string,
                    container_name=container_name
                )
                
                print(f"  ✓ Discovered {len(discovered_assets)} assets")
                total_discovered += len(discovered_assets)
                
                new_assets = []
                for asset in discovered_assets:
                    asset_id = asset.get('id')
                    if asset_id and asset_id not in existing_asset_ids:
                        new_assets.append(asset)
                        existing_asset_ids.add(asset_id)
                
                print(f"   Found {len(new_assets)} NEW assets")
                
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
                            'catalog': asset.get('catalog'),
                            'connector_id': connector_id
                        })
                
                print(f"Saved {saved_count} new assets")
                total_new += saved_count
                
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        print("\n" + "=" * 60)
        print(f"Discovery Complete: {total_new} new asset(s) discovered")
        print("=" * 60)
        
        return jsonify({
            'status': 'success',
            'new_assets': total_new,
            'total_discovered': total_discovered,
            'message': f'Discovery complete: {total_new} new asset(s) found'
        }), 200
        
    except Exception as e:
        print(f"Error in trigger discovery: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': f'Discovery failed: {str(e)}'
        }), 500

