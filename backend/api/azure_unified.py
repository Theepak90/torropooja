from flask import Blueprint, request, jsonify
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareServiceClient
from azure.data.tables import TableServiceClient
from azure.storage.queue import QueueServiceClient
from azure.core.exceptions import AzureError
from azure.core.credentials import AzureNamedKeyCredential
from datetime import datetime
import db_helpers

azure_unified_bp = Blueprint('azure_unified_bp', __name__)

class AzureUnifiedConnectionTest(BaseModel):
    account_name: str
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    container_name: Optional[str] = None
    share_name: Optional[str] = None
    connection_name: str

def discover_azure_all_assets(account_name: Optional[str] = None, account_key: Optional[str] = None,
                               connection_string: Optional[str] = None,
                               container_name: Optional[str] = None,
                               share_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Unified Azure discovery that discovers Blob Storage, Files, Tables, and Queues together.
    Returns a dictionary with all discovered assets organized by type.
    """
    all_assets = []
    blob_containers = []
    file_shares = []
    tables_list = []
    queues_list = []
    
    # Extract account name from connection string if needed
    if connection_string and not account_name:
        for part in connection_string.split(';'):
            if part.startswith('AccountName='):
                account_name = part.split('=', 1)[1]
                break
    
    if not account_name:
        raise Exception("Unable to determine account name from connection string or credentials")
    
    # 1. Discover Blob Storage (containers and blobs)
    try:
        print(f"  📦 Discovering Azure Blob Storage in account: {account_name}")
        if connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        elif account_name and account_key:
            account_url = f"https://{account_name}.blob.core.windows.net"
            blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided")
        
        if container_name:
            containers_to_discover = [container_name]
        else:
            containers = list(blob_service_client.list_containers())
            containers_to_discover = [c.name for c in containers]
        
        print(f"  ✅ Found {len(containers_to_discover)} blob container(s)")
        
        for container_name_actual in containers_to_discover:
            try:
                container_client = blob_service_client.get_container_client(container_name_actual)
                blobs = container_client.list_blobs()
                blob_list = list(blobs)
                
                container_assets = []
                for blob in blob_list:
                    key = blob.name
                    size = blob.size or 0
                    last_modified = blob.last_modified or datetime.now()
                    
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
                    
                    asset = {
                        "id": f"https://{account_name}.blob.core.windows.net/{container_name_actual}/{key}",
                        "name": key.split('/')[-1] if '/' in key else key,
                        "type": asset_type,
                        "catalog": container_name_actual,
                        "schema": '/'.join(key.split('/')[:-1]) if '/' in key else "/",
                        "size_bytes": size,
                        "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified),
                        "data_source": "Azure Blob Storage"
                    }
                    container_assets.append(asset)
                    all_assets.append(asset)
                
                blob_containers.append({
                    "name": container_name_actual,
                    "asset_count": len(container_assets),
                    "assets": container_assets
                })
                
            except Exception as container_error:
                print(f"    ⚠️ Error discovering container {container_name_actual}: {container_error}")
                continue
                
    except Exception as blob_error:
        print(f"  ⚠️ Error discovering Blob Storage: {blob_error}")
    
    # 2. Discover Azure Files (file shares)
    try:
        print(f"  📁 Discovering Azure Files in account: {account_name}")
        if connection_string:
            share_service_client = ShareServiceClient.from_connection_string(connection_string)
        elif account_name and account_key:
            account_url = f"https://{account_name}.file.core.windows.net"
            share_service_client = ShareServiceClient(account_url=account_url, credential=account_key)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided")
        
        if share_name:
            shares_to_discover = [share_name]
        else:
            shares = list(share_service_client.list_shares())
            shares_to_discover = [share.name for share in shares]
        
        print(f"  ✅ Found {len(shares_to_discover)} file share(s)")
        
        for share_name_actual in shares_to_discover:
            try:
                share_client = share_service_client.get_share_client(share_name_actual)
                share_assets = []
                
                def list_files_recursive(directory_path: str = ""):
                    """Recursively list all files and directories"""
                    try:
                        if directory_path:
                            dir_client = share_client.get_directory_client(directory_path)
                        else:
                            dir_client = share_client.get_root_directory_client()
                        items = list(dir_client.list_directories_and_files())
                        
                        for item in items:
                            item_path = f"{directory_path}/{item.name}" if directory_path else item.name
                            
                            if hasattr(item, 'is_directory') and item.is_directory:
                                asset = {
                                    "id": f"https://{account_name}.file.core.windows.net/{share_name_actual}/{item_path}",
                                    "name": item.name,
                                    "type": "Folder",
                                    "catalog": share_name_actual,
                                    "schema": directory_path if directory_path else "/",
                                    "size_bytes": 0,
                                    "last_modified": datetime.now().isoformat(),
                                    "data_source": "Azure Files"
                                }
                                share_assets.append(asset)
                                all_assets.append(asset)
                                list_files_recursive(item_path)
                            else:
                                size = getattr(item, 'content_length', 0) or 0
                                last_modified = getattr(item, 'last_modified', None) or datetime.now()
                                
                                asset_type = 'File'
                                if item_path.lower().endswith(('.csv', '.tsv', '.json', '.parquet', '.avro', '.orc')):
                                    asset_type = 'Data File'
                                elif item_path.lower().endswith(('.sql', '.py', '.scala', '.r')):
                                    asset_type = 'Script'
                                elif item_path.lower().endswith(('.txt', '.log')):
                                    asset_type = 'Text File'
                                elif item_path.lower().endswith(('.zip', '.gz', '.tar', '.bz2')):
                                    asset_type = 'Archive'
                                
                                asset = {
                                    "id": f"https://{account_name}.file.core.windows.net/{share_name_actual}/{item_path}",
                                    "name": item.name,
                                    "type": asset_type,
                                    "catalog": share_name_actual,
                                    "schema": directory_path if directory_path else "/",
                                    "size_bytes": size,
                                    "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified),
                                    "data_source": "Azure Files"
                                }
                                share_assets.append(asset)
                                all_assets.append(asset)
                    except Exception as e:
                        print(f"      ⚠️ Error listing directory {directory_path}: {e}")
                
                list_files_recursive()
                
                file_shares.append({
                    "name": share_name_actual,
                    "asset_count": len(share_assets),
                    "assets": share_assets
                })
                
            except Exception as share_error:
                print(f"    ⚠️ Error discovering share {share_name_actual}: {share_error}")
                continue
                
    except Exception as files_error:
        print(f"  ⚠️ Error discovering Azure Files: {files_error}")
    
    # 3. Discover Azure Tables
    try:
        print(f"  📊 Discovering Azure Tables in account: {account_name}")
        if connection_string:
            table_service_client = TableServiceClient.from_connection_string(connection_string)
        elif account_name and account_key:
            account_url = f"https://{account_name}.table.core.windows.net"
            credential = AzureNamedKeyCredential(account_name, account_key)
            table_service_client = TableServiceClient(endpoint=account_url, credential=credential)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided for Tables")
        
        tables = list(table_service_client.list_tables())
        print(f"  ✅ Found {len(tables)} table(s)")
        
        for table in tables:
            table_name = table.name
            asset = {
                "id": f"https://{account_name}.table.core.windows.net/{table_name}",
                "name": table_name,
                "type": "Table",
                "catalog": account_name,
                "schema": "tables",
                "size_bytes": 0,
                "last_modified": datetime.now().isoformat(),
                "data_source": "Azure Tables"
            }
            tables_list.append(table_name)
            all_assets.append(asset)
            
    except Exception as table_error:
        print(f"  ⚠️ Error discovering tables: {table_error}")
    
    # 4. Discover Azure Queues
    try:
        print(f"  📬 Discovering Azure Queues in account: {account_name}")
        if connection_string:
            queue_service_client = QueueServiceClient.from_connection_string(connection_string)
        elif account_name and account_key:
            account_url = f"https://{account_name}.queue.core.windows.net"
            credential = AzureNamedKeyCredential(account_name, account_key)
            queue_service_client = QueueServiceClient(account_url=account_url, credential=credential)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided for Queues")
        
        queues = list(queue_service_client.list_queues())
        print(f"  ✅ Found {len(queues)} queue(s)")
        
        for queue in queues:
            queue_name = queue.name
            asset = {
                "id": f"https://{account_name}.queue.core.windows.net/{queue_name}",
                "name": queue_name,
                "type": "Queue",
                "catalog": account_name,
                "schema": "queues",
                "size_bytes": 0,
                "last_modified": datetime.now().isoformat(),
                "data_source": "Azure Queues"
            }
            queues_list.append(queue_name)
            all_assets.append(asset)
            
    except Exception as queue_error:
        print(f"  ⚠️ Error discovering queues: {queue_error}")
    
    return {
        "all_assets": all_assets,
        "total_assets": len(all_assets),
        "blob_containers": blob_containers,
        "file_shares": file_shares,
        "tables": tables_list,
        "queues": queues_list,
        "total_blob_containers": len(blob_containers),
        "total_file_shares": len(file_shares),
        "total_tables": len(tables_list),
        "total_queues": len(queues_list),
        "account_name": account_name,
        "summary": {
            "blob_assets": sum(c["asset_count"] for c in blob_containers),
            "file_assets": sum(s["asset_count"] for s in file_shares),
            "table_assets": len(tables_list),
            "queue_assets": len(queues_list)
        }
    }

