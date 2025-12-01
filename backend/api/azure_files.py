from flask import Blueprint, request, jsonify
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareDirectoryClient
from azure.core.exceptions import AzureError
from datetime import datetime
import db_helpers

azure_files_bp = Blueprint('azure_files_bp', __name__)

class AzureFilesConnectionTest(BaseModel):
    account_name: str
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    share_name: Optional[str] = None
    connection_name: str

def discover_azure_files_assets_structured(account_name: Optional[str] = None, account_key: Optional[str] = None,
                                            connection_string: Optional[str] = None,
                                            share_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Discover Azure Files assets and return structured data organized by file shares.
    Returns a dictionary with shares and their files/directories.
    """
    shares_data = []
    all_assets = []
    
    try:
        if connection_string:
            share_service_client = ShareServiceClient.from_connection_string(connection_string)
            if not account_name:
                for part in connection_string.split(';'):
                    if part.startswith('AccountName='):
                        account_name = part.split('=', 1)[1]
                        break
                if not account_name:
                    try:
                        account_name = share_service_client.account_name
                    except:
                        pass
        elif account_name and account_key:
            account_url = f"https://{account_name}.file.core.windows.net"
            share_service_client = ShareServiceClient(account_url=account_url, credential=account_key)
        else:
            raise Exception("Either connection_string or both account_name and account_key must be provided")
        
        if not account_name:
            try:
                account_name = share_service_client.account_name
            except:
                pass
        if not account_name:
            raise Exception("Unable to determine account name from connection string or credentials")
        
        if share_name:
            # Discover specific share
            shares_to_discover = [share_name]
        else:
            # Discover all shares
            print(f"  📁 Listing file shares in account: {account_name}")
            shares = list(share_service_client.list_shares())
            shares_to_discover = [share.name for share in shares]
            print(f"  ✅ Found {len(shares_to_discover)} file share(s)")
        
        for share_name_actual in shares_to_discover:
            try:
                print(f"  📁 Discovering files in share: {share_name_actual}")
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
                                # It's a directory
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
                                
                                # Recursively list subdirectories
                                list_files_recursive(item_path)
                            else:
                                # It's a file
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
                        print(f"    ⚠️ Error listing directory {directory_path}: {e}")
                
                # Start recursive listing from root
                list_files_recursive()
                
                print(f"  ✅ Found {len(share_assets)} asset(s) in share {share_name_actual}")
                
                shares_data.append({
                    "name": share_name_actual,
                    "asset_count": len(share_assets),
                    "assets": share_assets
                })
                
            except Exception as share_error:
                print(f"  ⚠️ Error discovering share {share_name_actual}: {share_error}")
                continue
        
        return {
            "shares": shares_data,
            "all_assets": all_assets,
            "total_assets": len(all_assets),
            "total_shares": len(shares_data),
            "account_name": account_name
        }
        
    except Exception as e:
        raise Exception(f"Error discovering Azure Files assets: {str(e)}")

def discover_azure_files_assets(account_name: Optional[str] = None, account_key: Optional[str] = None,
                               connection_string: Optional[str] = None,
                               share_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Discover Azure Files assets and return as a flat list"""
    structured_data = discover_azure_files_assets_structured(account_name, account_key, connection_string, share_name)
    return structured_data.get('all_assets', [])

