from flask import Blueprint, request, jsonify
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from azure.data.tables import TableServiceClient
from azure.storage.queue import QueueServiceClient
from azure.core.exceptions import AzureError
from datetime import datetime
import db_helpers

azure_tables_queues_bp = Blueprint('azure_tables_queues_bp', __name__)

class AzureTablesQueuesConnectionTest(BaseModel):
    account_name: str
    account_key: Optional[str] = None
    connection_string: Optional[str] = None
    connection_name: str

def discover_azure_tables_queues_assets(account_name: Optional[str] = None, account_key: Optional[str] = None,
                                        connection_string: Optional[str] = None) -> Dict[str, Any]:
    """
    Discover Azure Tables and Queues assets.
    Returns a dictionary with tables and queues.
    """
    all_assets = []
    tables_list = []
    queues_list = []
    
    try:
        # Determine account name
        if connection_string:
            # Extract account name from connection string
            if not account_name:
                for part in connection_string.split(';'):
                    if part.startswith('AccountName='):
                        account_name = part.split('=', 1)[1]
                        break
        elif not account_name:
            raise Exception("Either connection_string or account_name must be provided")
        
        # Discover Tables
        try:
            print(f"  📊 Discovering Azure Tables in account: {account_name}")
            if connection_string:
                table_service_client = TableServiceClient.from_connection_string(connection_string)
            elif account_name and account_key:
                account_url = f"https://{account_name}.table.core.windows.net"
                from azure.core.credentials import AzureNamedKeyCredential
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
        
        # Discover Queues
        try:
            print(f"  📬 Discovering Azure Queues in account: {account_name}")
            if connection_string:
                queue_service_client = QueueServiceClient.from_connection_string(connection_string)
            elif account_name and account_key:
                account_url = f"https://{account_name}.queue.core.windows.net"
                from azure.core.credentials import AzureNamedKeyCredential
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
            "tables": tables_list,
            "queues": queues_list,
            "all_assets": all_assets,
            "total_assets": len(all_assets),
            "total_tables": len(tables_list),
            "total_queues": len(queues_list),
            "account_name": account_name
        }
        
    except Exception as e:
        raise Exception(f"Error discovering Azure Tables/Queues assets: {str(e)}")

