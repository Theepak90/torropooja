from flask import Flask, request, jsonify, Blueprint, current_app, g, abort, redirect, url_for
from werkzeug.exceptions import HTTPException, NotFound
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import json
import tempfile
import os
import threading
import time
from functools import lru_cache
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask_socketio import SocketIO, emit
from concurrent.futures import ThreadPoolExecutor, as_completed
os.environ['GRPC_DNS_RESOLVER'] = 'native'
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions
from flask_cors import CORS
from api.bigquery import bigquery_bp
from api.starburst import starburst_bp
from api.lineage import lineage_bp
from api.s3 import s3_bp
from api.gcs import gcs_bp
from api.azure_blob import azure_blob_bp
from config import Config 
from flask_sqlalchemy import SQLAlchemy
from database import init_db, Base, HiveDB, HiveTable, HiveStorageDescriptor, HiveColumn
import db_helpers
from flask_login import LoginManager, login_user, logout_user, login_required, current_user
from database import User
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:5178",
    "http://localhost:8000",
    "http://localhost:8099",
]
db = SQLAlchemy()
def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = Config.SQLALCHEMY_ENGINE_OPTIONS
    db.init_app(app) 
    CORS(app, resources={r"/api/*": {"origins": origins, "supports_credentials": True}}, supports_credentials=True)
    socketio = SocketIO(
        app, 
        cors_allowed_origins="*",
        async_mode='threading',
        logger=False,
        engineio_logger=False,
        ping_timeout=60,
        ping_interval=25,
        max_http_buffer_size=1000000,
        always_connect=True,
        manage_session=False,
        websocket_compression=False,
        http_compression=False,
        allow_upgrades=True,
        transports=['polling', 'websocket'],
        async_handlers=True,
        cookie=None,
        engineio_logger_level='WARNING'
    )
    app.extensions['socketio'] = socketio 
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'login'
    
    discovered_assets = []
    active_connectors = []
    
    class SystemHealth(BaseModel):
        status: str
        monitoring_enabled: bool
        connectors_enabled: int
        connectors_total: int
        last_scan: Optional[datetime]

    class Asset(BaseModel):
        id: str
        name: str
        type: str
        catalog: str
        discovered_at: datetime
        status: str

    class Connector(BaseModel):
        id: str
        name: str
        type: str
        status: str
        enabled: bool
        last_run: Optional[datetime]

    class Activity(BaseModel):
        id: str
        type: str
        description: str
        timestamp: datetime
        status: str

    class DashboardStats(BaseModel):
        total_assets: int
        total_catalogs: int
        active_connectors: int
        last_scan: Optional[datetime]
        monitoring_status: str
    
    def save_connectors():
        nonlocal active_connectors
        try:
            saved_count = 0
            for connector in active_connectors:
                if db_helpers.save_connector(connector):
                    saved_count += 1
            print(f" Saved {saved_count}/{len(active_connectors)} connectors to database")
            app.config['active_connectors'] = active_connectors
        except Exception as e:
            print(f"Error saving connectors: {e}")
            import traceback
            traceback.print_exc()

    def load_connectors():
        nonlocal active_connectors
        try:
            active_connectors = db_helpers.load_connectors()
            print(f" Loaded {len(active_connectors)} connectors from database")
            app.config['active_connectors'] = active_connectors
            return active_connectors
        except Exception as e:
            print(f" Error loading connectors: {e}")
            import traceback
            traceback.print_exc()
            active_connectors = []
            app.config['active_connectors'] = []
        return active_connectors

    def save_assets():
        nonlocal discovered_assets
        try:
            saved_count = 0
            failed_count = 0
            failed_assets = []
            
            for asset in discovered_assets:
                db_asset = {
                    'id': asset.get('id'),
                    'name': asset.get('name'),
                    'type': asset.get('type'),
                    'catalog': asset.get('catalog'),
                    'schema': asset.get('schema'),
                    'connector_id': asset.get('connector_id'),
                    'discovered_at': asset.get('discovered_at'),
                    'status': asset.get('status', 'active'),
                    'extra_data': asset
                }
                
                if not db_asset.get('id'):
                    print(f"  Skipping asset with no ID: {asset.get('name', 'Unknown')}")
                    failed_count += 1
                    failed_assets.append(asset.get('name', 'Unknown'))
                    continue
                    
                try:
                    if db_helpers.save_asset(db_asset):
                        saved_count += 1
                    else:
                        failed_count += 1
                        failed_assets.append(db_asset.get('id', 'Unknown'))
                        if failed_count <= 20:
                            print(f"  Failed to save asset: {db_asset.get('id', 'Unknown')}")
                            if not db_asset.get('name'):
                                print(f"   Reason: Missing name field")
                            if not db_asset.get('type'):
                                print(f"   Reason: Missing type field")
                except Exception as save_error:
                    failed_count += 1
                    failed_assets.append(db_asset.get('id', 'Unknown'))
                    if failed_count <= 20:
                        print(f" Exception saving asset {db_asset.get('id', 'Unknown')}: {save_error}")
                        import traceback
                        traceback.print_exc()
            
            print(f" Saved {saved_count}/{len(discovered_assets)} assets to database")
            if failed_count > 0:
                print(f"  Failed to save {failed_count} assets")
                if failed_count <= 50:
                    print(f"   Failed asset IDs (first 50): {failed_assets[:50]}")
                else:
                    print(f"   Failed asset IDs (first 50): {failed_assets[:50]}")
                    print(f"   ... and {failed_count - 50} more failed assets")
            
            app.config['discovered_assets'] = discovered_assets
        except Exception as e:
            print(f" Error saving assets: {e}")
            import traceback
            traceback.print_exc()

    def load_assets():
        nonlocal discovered_assets
        try:
            discovered_assets = db_helpers.load_assets()
            print(f" Loaded {len(discovered_assets)} assets from database")
            app.config['discovered_assets'] = discovered_assets
            return discovered_assets
        except Exception as e:
            print(f" Error loading assets: {e}")
            import traceback
            traceback.print_exc()
            discovered_assets = []
            app.config['discovered_assets'] = []
        return discovered_assets
    
    @login_manager.user_loader
    def load_user(user_id):
        with app.app_context():
            return db.session.query(User).get(int(user_id))

    class BigQueryConnectionTest(BaseModel):
        project_id: str
        service_account_json: str
        connection_name: str

    class StarburstConnectionTest(BaseModel):
        account_domain: str
        client_id: str
        client_secret: str
        connection_name: str

    class S3ConnectionTest(BaseModel):
        access_key_id: str
        secret_access_key: str
        region: Optional[str] = None
        bucket_name: Optional[str] = None
        connection_name: str

    class ConnectionTestResponse(BaseModel):
        success: bool
        message: str
        discovered_assets: Optional[int] = 0
        connector_id: Optional[str] = None

    @app.route("/api/connectors/bigquery/test", methods=["POST"])
    def test_bigquery_connection():
        nonlocal discovered_assets, active_connectors
        connection_data = BigQueryConnectionTest(**request.get_json())
        try:
            service_account_info = json.loads(connection_data.service_account_json)
        except json.JSONDecodeError:
            abort(400, "Invalid service account JSON format")
            
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
            )
            
            client = bigquery.Client(
                credentials=credentials,
                project=connection_data.project_id
            )
            
            datasets = list(client.list_datasets())
            
            assets_discovered = 0
            connector_id = f"bq_{connection_data.project_id}_{datetime.now().timestamp()}"
            
            for dataset_ref in datasets:
                dataset = client.get_dataset(dataset_ref.dataset_id)
                
                try:
                    tables = list(client.list_tables(dataset.dataset_id))
                    for table_ref in tables:
                        table = client.get_table(table_ref)
                        
                        columns = []
                        if table.schema:
                            for field in table.schema:
                                columns.append({
                                    "name": field.name,
                                    "type": field.field_type,
                                    "mode": field.mode,
                                "description": field.description or "",
                                })
                        
                        table_labels = table.labels or {}
                        table_owner = table_labels.get('owner', dataset.labels.get('owner', service_account_info.get('client_email', 'Unknown')) if dataset.labels else service_account_info.get('client_email', 'Unknown'))
                        
                        dataset_location = dataset.location if hasattr(dataset, 'location') else 'Unknown'
                        
                        discovered_assets.append({
                            "id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                            "name": table.table_id,
                            "type": "Table" if table.table_type == "TABLE" else "View",
                            "catalog": f"{connection_data.project_id}.{dataset.dataset_id}",
                            "discovered_at": datetime.now().isoformat(),
                            "status": "active",
                            "description": table.description or "No description available",
                            "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                            "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                            "connector_id": connector_id,
                            "columns": columns,
                            "technical_metadata": {
                                "asset_id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                "asset_type": "Table" if table.table_type == "TABLE" else "View",
                                "location": f"{dataset_location} - {connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                "format": f"BigQuery {table.table_type}",
                                "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                                "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                                "created_at": table.created.isoformat() if hasattr(table, 'created') and table.created else datetime.now().isoformat(),
                                "source_system": "BigQuery",
                                "storage_location": f"{connection_data.project_id}/{dataset.dataset_id}/{table.table_id}",
                                "schema_name": dataset.dataset_id,
                                "table_name": table.table_id,
                                "column_count": len(columns),
                                "partitioning_strategy": str(table.partitioning_type) if hasattr(table, 'partitioning_type') and table.partitioning_type else "None",
                                "clustering_fields": ", ".join(table.clustering_fields) if hasattr(table, 'clustering_fields') and table.clustering_fields else "None"
                            },
                            "operational_metadata": {
                                "status": "active",
                                "owner": table_owner,
                                "last_modified": table.modified.isoformat() if hasattr(table, 'modified') and table.modified else datetime.now().isoformat(),
                                "last_accessed": datetime.now().isoformat(),
                                "access_count": "N/A",
                                "data_quality_score": 95
                            },
                            "business_metadata": {
                                "description": table.description or "No description available",
                                "business_owner": table_owner,
                                "department": dataset.dataset_id,
                                "classification": table_labels.get('classification', 'internal'),
                                "sensitivity_level": table_labels.get('sensitivity', 'low'),
                                "tags": list(table_labels.keys()) if table_labels else []
                            }
                        })
                        assets_discovered += 1
                except Exception as e:
                    print(f"Error listing tables in dataset {dataset.dataset_id}: {str(e)}")
                    continue
            
            active_connectors.append({
                "id": connector_id,
                "name": connection_data.connection_name,
                "type": "BigQuery",
                "status": "active",
                "enabled": True,
                "last_run": datetime.now().isoformat(),
                "config": {
                    "project_id": connection_data.project_id,
                    "service_account_json": connection_data.service_account_json
                },
                "assets_count": assets_discovered
            })
            
            
            save_connectors()
            save_assets()
            
            
            return jsonify(ConnectionTestResponse(
                success=True,
                message=f"Successfully connected to BigQuery project '{connection_data.project_id}'. Discovered {assets_discovered} assets.",
                discovered_assets=assets_discovered,
                connector_id=connector_id
            ).model_dump())
        
        except google_exceptions.GoogleAPIError as e:
            abort(401, f"BigQuery API error: {str(e)}")
        except Exception as e:
            abort(500, f"Connection failed: {str(e)}")

    @socketio.on('connect', namespace='/connectors')
    def handle_connect(auth):
        print("🔵 CLIENT CONNECTED TO /connectors NAMESPACE!")
        print(f"   Session ID: {request.sid}")
        print(f"   Auth: {auth}")
        return True
    
    @socketio.on('disconnect', namespace='/connectors')
    def handle_disconnect():
        print("🔴 CLIENT DISCONNECTED FROM /connectors NAMESPACE")
    
    @socketio.on('connect', namespace='/assets')
    def handle_assets_connect(auth=None):
        try:
            print("🔵 CLIENT CONNECTED TO /assets NAMESPACE!")
            print(f"   Session ID: {request.sid}")
            print(f"   Auth: {auth}")
            return True
        except Exception as e:
            print(f" Error in assets connect handler: {e}")
            import traceback
            traceback.print_exc()
            return True
    
    @socketio.on('disconnect', namespace='/assets')
    def handle_assets_disconnect():
        try:
            print("🔴 CLIENT DISCONNECTED FROM /assets NAMESPACE")
        except Exception as e:
            print(f" Error in assets disconnect handler: {e}")
    
    @socketio.on('test_connection', namespace='/connectors')
    def handle_test_connection_event(data):
        print("🟢 SOCKET EVENT RECEIVED: test_connection")
        print(f"   Raw Data: {data}")
        print(f"   Data type: {type(data)}")
        print(f"   Data keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
        nonlocal discovered_assets, active_connectors
        
        connection_type = data.get('connectionType') or data.get('connection_type') or data.get('connection-type')
        selected_connector_id = data.get('selectedConnectorId') or data.get('selected_connector_id') or data.get('selected-connector-id') or data.get('connectorId') or data.get('connector_id')
        config = data.get('config') or data.get('configuration') or {}
        
        if not connection_type:
            print(f" Missing connection_type. Available keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            emit('error', {'type': 'error', 'message': 'Missing connection type'}, namespace='/connectors')
            return
        if not selected_connector_id:
            print(f" Missing selected_connector_id. Available keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            emit('error', {'type': 'error', 'message': 'Missing connector ID'}, namespace='/connectors')
            return
        if not config:
            print(f"  Missing config, using empty dict")
            config = {}
        
        connection_type = str(connection_type).strip()
        selected_connector_id = str(selected_connector_id).strip().lower()
        
        print(f"   Connection Type: '{connection_type}' (type: {type(connection_type)})")
        print(f"   Connector ID: '{selected_connector_id}' (type: {type(selected_connector_id)})")
        print(f"   Config keys: {list(config.keys()) if config else 'None'}")
        print(f"   Checking conditions:")
        print(f"     - BigQuery: connection_type == 'Service Account' ({connection_type == 'Service Account'}) AND selected_connector_id == 'bigquery' ({selected_connector_id == 'bigquery'})")
        print(f"     - GCS: connection_type == 'Service Account' ({connection_type == 'Service Account'}) AND selected_connector_id == 'gcs' ({selected_connector_id == 'gcs'})")
        print(f"     - S3: connection_type == 'Access Key' ({connection_type == 'Access Key'}) AND selected_connector_id == 's3' ({selected_connector_id == 's3'})")
        print(f"     - Azure Blob: connection_type in ['Account Key', 'Connection String'] ({connection_type in ['Account Key', 'Connection String']}) AND selected_connector_id == 'azure-blob' ({selected_connector_id == 'azure-blob'})")

        if connection_type == 'Service Account' and selected_connector_id == 'bigquery':
            try:
                connection_data = BigQueryConnectionTest(
                    project_id=config.get('projectId'),
                    service_account_json=config.get('serviceAccount'),
                    connection_name=config.get('name')
                )
            except Exception as e:
                emit('error', {'type': 'error', 'message': f'Invalid BigQuery connection data: {str(e)}'}, namespace='/connectors')
                return

            try:
                service_account_info = json.loads(connection_data.service_account_json)
            except json.JSONDecodeError:
                emit('error', {'type': 'error', 'message': 'Invalid service account JSON format'}, namespace='/connectors')
                return
            
            try:
                emit('progress', {'type': 'progress', 'message': ' Authenticating with BigQuery...'}, namespace='/connectors')
                
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
                )
                
                client = bigquery.Client(
                    credentials=credentials,
                    project=connection_data.project_id
                )
                
                emit('progress', {'type': 'progress', 'message': ' Authentication successful! Connected to BigQuery'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' Fetching datasets from project: {connection_data.project_id}...'}, namespace='/connectors')
                
                datasets = list(client.list_datasets())
                emit('progress', {'type': 'progress', 'message': f' Found {len(datasets)} datasets! Starting asset discovery...'}, namespace='/connectors')
                
                assets_discovered = 0
                connector_id = f"bq_{connection_data.project_id}_{datetime.now().timestamp()}"
                
                for dataset_ref in datasets:
                    dataset = client.get_dataset(dataset_ref.dataset_id)
                    emit('progress', {'type': 'progress', 'message': f'  Discovering tables in dataset: {dataset.dataset_id}'}, namespace='/connectors')
                    
                    tables = []
                    bq_attempt = 0
                    while True:
                        try:
                            tables = list(client.list_tables(dataset.dataset_id))
                            emit('progress', {'type': 'progress', 'message': f' Successfully fetched {len(tables)} tables from {dataset.dataset_id}'}, namespace='/connectors')
                            break
                        except Exception as e:
                            bq_attempt += 1
                            wait_time = min(2 ** min(bq_attempt - 1, 5), 30)
                            print(f"  Attempt {bq_attempt} failed for tables in dataset {dataset.dataset_id}: {e}")
                            emit('progress', {'type': 'progress', 'message': f'  Retry {bq_attempt} for tables in {dataset.dataset_id} - waiting {wait_time}s...'}, namespace='/connectors')
                            time.sleep(wait_time)
                    
                    def fetch_bigquery_table(table_ref):
                        table_retry = 0
                        while True:
                            try:
                                table = client.get_table(table_ref)
                                
                                columns = []
                                if table.schema:
                                    for field in table.schema:
                                        columns.append({
                                            "name": field.name,
                                            "type": field.field_type,
                                            "mode": field.mode,
                                            "description": field.description or "",
                                        })
                                
                                table_labels = table.labels or {}
                                table_owner = table_labels.get('owner', dataset.labels.get('owner', service_account_info.get('client_email', 'Unknown')) if dataset.labels else service_account_info.get('client_email', 'Unknown'))
                                dataset_location = dataset.location if hasattr(dataset, 'location') else 'Unknown'
                                
                                return {
                                    "id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                    "name": table.table_id,
                                    "type": "Table" if table.table_type == "TABLE" else "View",
                                    "catalog": f"{connection_data.project_id}.{dataset.dataset_id}",
                                    "discovered_at": datetime.now().isoformat(),
                                    "status": "active",
                                    "description": table.description or "No description available",
                                    "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                                    "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                                    "connector_id": connector_id,
                                    "columns": columns,
                                    "technical_metadata": {
                                        "asset_id": f"{connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                        "asset_type": "Table" if table.table_type == "TABLE" else "View",
                                        "location": f"{dataset_location} - {connection_data.project_id}.{dataset.dataset_id}.{table.table_id}",
                                        "format": f"BigQuery {table.table_type}",
                                        "size_bytes": table.num_bytes if hasattr(table, 'num_bytes') else 0,
                                        "num_rows": table.num_rows if hasattr(table, 'num_rows') else 0,
                                        "created_at": table.created.isoformat() if hasattr(table, 'created') and table.created else datetime.now().isoformat(),
                                        "source_system": "BigQuery",
                                        "storage_location": f"{connection_data.project_id}/{dataset.dataset_id}/{table.table_id}",
                                        "schema_name": dataset.dataset_id,
                                        "table_name": table.table_id,
                                        "column_count": len(columns),
                                        "partitioning_strategy": str(table.partitioning_type) if hasattr(table, 'partitioning_type') and table.partitioning_type else "None",
                                        "clustering_fields": ", ".join(table.clustering_fields) if hasattr(table, 'clustering_fields') and table.clustering_fields else "None"
                                    },
                                    "operational_metadata": {
                                        "status": "active",
                                        "owner": table_owner,
                                        "last_modified": table.modified.isoformat() if hasattr(table, 'modified') and table.modified else datetime.now().isoformat(),
                                        "last_accessed": datetime.now().isoformat(),
                                        "access_count": "N/A",
                                        "data_quality_score": 95
                                    },
                                    "business_metadata": {
                                        "description": table.description or "No description available",
                                        "business_owner": table_owner,
                                        "department": dataset.dataset_id,
                                        "classification": table_labels.get('classification', 'internal'),
                                        "sensitivity_level": table_labels.get('sensitivity', 'low'),
                                        "tags": list(table_labels.keys()) if table_labels else []
                                    }
                                }
                            except Exception as retry_error:
                                table_retry += 1
                                wait_time = min(2 ** min(table_retry - 1, 5), 30)
                                print(f"  Retry {table_retry} for table {table_ref.table_id}: {retry_error}")
                                time.sleep(wait_time)
                    
                    emit('progress', {'type': 'progress', 'message': f' Fetching {len(tables)} tables in parallel (20 concurrent)...'}, namespace='/connectors')
                    with ThreadPoolExecutor(max_workers=20) as executor:
                        futures = [executor.submit(fetch_bigquery_table, table_ref) for table_ref in tables]
                        
                        for future in as_completed(futures):
                            try:
                                result = future.result()
                                if result:
                                    discovered_assets.append(result)
                                    assets_discovered += 1
                                    col_count = len(result.get('columns', []))
                                    emit('progress', {'type': 'progress', 'message': f' Discovered table: {result["name"]} ({col_count} columns)'}, namespace='/connectors')
                            except Exception as e:
                                print(f"  Error in future result: {e}")
                                continue
            
                import json as json_module
                try:
                    service_account_info = json_module.loads(connection_data.service_account_json)
                    placeholder_values = ["your-project-id", "YOUR_PRIVATE_KEY", "your-service-account@your-project.iam.gserviceaccount.com"]
                    
                    service_account_str = str(service_account_info).lower()
                    if any(placeholder.lower() in service_account_str for placeholder in placeholder_values):
                        emit('error', {'type': 'error', 'message': ' Placeholder credentials detected in service account JSON. Please enter your actual Google Cloud service account credentials.'}, namespace='/connectors')
                        return
                except json_module.JSONDecodeError:
                    emit('error', {'type': 'error', 'message': ' Invalid JSON format in service account credentials.'}, namespace='/connectors')
                    return
                
                active_connectors.append({
                    "id": connector_id,
                    "name": connection_data.connection_name,
                    "type": "BigQuery",
                    "status": "active",
                    "enabled": True,
                    "last_run": datetime.now().isoformat(),
                    "config": {
                        "project_id": connection_data.project_id,
                        "service_account_json": connection_data.service_account_json
                    },
                    "assets_count": assets_discovered
                })
                
                save_connectors()
                save_assets()

                total_columns = sum(len(asset.get('columns', [])) for asset in discovered_assets if asset.get('connector_id') == connector_id)
                emit('complete', {'type': 'complete', 'message': f' Successfully discovered {assets_discovered} assets with {total_columns} total columns!', 'discovered_assets': assets_discovered, 'connector_id': connector_id}, namespace='/connectors')
                return

            except Exception as e:
                print(f"Connection failed: {str(e)}")
                emit('error', {'type': 'error', 'message': f'Connection failed: {str(e)}'}, namespace='/connectors')
                return

        elif connection_type == 'API Token' and selected_connector_id == 'starburst':
            try:
                connection_data = StarburstConnectionTest(
                    account_domain=config.get('accountDomain'),
                    client_id=config.get('clientId'),
                    client_secret=config.get('clientSecret'),
                    connection_name=config.get('name')
                )
            except Exception as e:
                emit('error', {'type': 'error', 'message': f'Invalid Starburst connection data: {str(e)}'}, namespace='/connectors')
                return
            
            try:
                import requests
                import base64
                
                if not connection_data.account_domain or not connection_data.client_id or not connection_data.client_secret:
                    emit('error', {'type': 'error', 'message': 'Missing required connection parameters'}, namespace='/connectors')
                    return
                
                if not connection_data.account_domain.endswith('.galaxy.starburst.io'):
                    emit('error', {'type': 'error', 'message': 'Invalid Starburst Galaxy domain format'}, namespace='/connectors')
                    return
                
                base_url = f"https://{connection_data.account_domain}"
                
                auth_string = f"{connection_data.client_id}:{connection_data.client_secret}"
                auth_bytes = auth_string.encode('ascii')
                auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
                
                token_headers = {
                    'Authorization': f'Basic {auth_b64}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                token_data = 'grant_type=client_credentials'
                token_url = f"{base_url}/oauth/v2/token"
                
                
                emit('progress', {'type': 'progress', 'message': ' Authenticating with Starburst Galaxy...'}, namespace='/connectors')
                token_response = requests.post(token_url, headers=token_headers, data=token_data, timeout=30)
                
                
                if token_response.status_code != 200:
                    emit('error', {'type': 'error', 'message': f' Authentication failed: {token_response.text}'}, namespace='/connectors')
                    return
                
                try:
                    token_data = token_response.json()
                    access_token = token_data.get('access_token')
                except json.JSONDecodeError:
                    emit('error', {'type': 'error', 'message': f' Invalid JSON response from Starburst Galaxy: {token_response.text}'}, namespace='/connectors')
                    return
                
                if not access_token:
                    emit('error', {'type': 'error', 'message': ' No access token received from Starburst Galaxy'}, namespace='/connectors')
                    return
                
                emit('progress', {'type': 'progress', 'message': ' Authentication successful! Connected to Starburst Galaxy'}, namespace='/connectors')

                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
                
                emit('progress', {'type': 'progress', 'message': ' Fetching Starburst catalogs...'}, namespace='/connectors')
                catalogs_url = f"{base_url}/public/api/v1/catalog"
                catalogs_response = requests.get(catalogs_url, headers=headers, timeout=30)
                
                if catalogs_response.status_code == 401:
                    emit('error', {'type': 'error', 'message': 'Invalid Starburst Galaxy credentials. Please check your Client ID and Client Secret.'}, namespace='/connectors')
                    return
                elif catalogs_response.status_code == 403:
                    emit('error', {'type': 'error', 'message': 'Access forbidden. Please ensure your account has the necessary permissions.'}, namespace='/connectors')
                    return
                elif catalogs_response.status_code == 404:
                    emit('error', {'type': 'error', 'message': 'Starburst Galaxy domain not found. Please verify your account domain.'}, namespace='/connectors')
                    return
                elif catalogs_response.status_code != 200:
                    emit('error', {'type': 'error', 'message': f'Starburst Galaxy API error: {catalogs_response.text}'}, namespace='/connectors')
                    return
                
                catalogs_data = catalogs_response.json()
                catalogs = catalogs_data.get('result', [])
                
                emit('progress', {'type': 'progress', 'message': f' Found {len(catalogs)} catalogs! Starting asset discovery...'}, namespace='/connectors')

                placeholder_values = ["your-starburst-client-id", "your-starburst-client-secret"]
                if any(placeholder in connection_data.client_id for placeholder in placeholder_values) or \
                   any(placeholder in connection_data.client_secret for placeholder in placeholder_values):
                    emit('error', {'type': 'error', 'message': ' Placeholder credentials detected. Please enter your actual Starburst Galaxy Client ID and Client Secret.'}, namespace='/connectors')
                    return

                discovered_assets_count = 0
                connector_id = f"starburst_{connection_data.account_domain}_{datetime.now().timestamp()}"

                all_tables = []
                for catalog in catalogs:
                    catalog_name = catalog.get('catalogName', catalog.get('name'))
                    if catalog_name.lower() in ['galaxy', 'galaxy_telemetry', 'system', 'information_schema']:
                        continue
                    
                    emit('progress', {'type': 'progress', 'message': f' Discovering schemas in catalog: {catalog_name}'}, namespace='/connectors')
                    schemas_url = f"{base_url}/public/api/v1/catalog/{catalog.get('catalogId')}/schema"
                    
                    schemas = []
                    attempt = 0
                    while True:
                        try:
                            schemas_response = requests.get(schemas_url, headers=headers, timeout=30)
                            schemas_data = schemas_response.json()
                            schemas = schemas_data.get('result', [])
                            emit('progress', {'type': 'progress', 'message': f' Successfully fetched {len(schemas)} schemas from {catalog_name}'}, namespace='/connectors')
                            break
                        except Exception as e:
                            attempt += 1
                            wait_time = min(2 ** min(attempt - 1, 5), 30)
                            print(f"  Attempt {attempt} failed for schemas in {catalog_name}: {e}")
                            emit('progress', {'type': 'progress', 'message': f'  Retry {attempt} for schemas in {catalog_name} - waiting {wait_time}s...'}, namespace='/connectors')
                            time.sleep(wait_time)

                    for schema in schemas:
                        schema_name = (schema.get('schemaName') or 
                                      schema.get('name') or 
                                      schema.get('schema') or
                                      schema.get('schemaIdentifier', {}).get('schema') or
                                      schema.get('schemaId') or
                                      'UnknownSchema')
                        schema_id = (schema.get('schemaId') or 
                                    schema.get('id') or 
                                    schema.get('schemaIdentifier', {}).get('schema') or
                                    schema_name)
                        
                        emit('progress', {'type': 'progress', 'message': f'  Discovering tables in schema: {schema_name}'}, namespace='/connectors')
                        tables_url = f"{base_url}/public/api/v1/catalog/{catalog.get('catalogId')}/schema/{schema_id}/table"
                        
                        tables = []
                        table_attempt = 0
                        while True:
                            try:
                                tables_response = requests.get(tables_url, headers=headers, timeout=30)
                                if not tables_response.text or tables_response.text.strip() == '':
                                    print(f"  Empty response for tables in schema {schema_name}")
                                    tables = []
                                    break
                                tables_data = tables_response.json()
                                tables = tables_data.get('result', [])
                                emit('progress', {'type': 'progress', 'message': f' Successfully fetched {len(tables)} tables from {schema_name}'}, namespace='/connectors')
                                break
                            except (json.JSONDecodeError, ValueError) as e:
                                table_attempt += 1
                                wait_time = min(2 ** min(table_attempt - 1, 5), 30)
                                print(f"  Attempt {table_attempt}: JSON error for tables in schema {schema_name}: {e}")
                                emit('progress', {'type': 'progress', 'message': f'  Retry {table_attempt} for tables in {schema_name} - waiting {wait_time}s...'}, namespace='/connectors')
                                time.sleep(wait_time)
                            except Exception as e:
                                table_attempt += 1
                                wait_time = min(2 ** min(table_attempt - 1, 5), 30)
                                print(f"  Attempt {table_attempt}: Error for tables in schema {schema_name}: {e}")
                                emit('progress', {'type': 'progress', 'message': f'  Retry {table_attempt} for tables in {schema_name} - waiting {wait_time}s...'}, namespace='/connectors')
                                time.sleep(wait_time)

                        def fetch_starburst_table(table):
                            try:
                                table_name = table.get('tableName') or table.get('name') or table.get('tableIdentifier', {}).get('table') or str(table.get('tableId', 'Unknown'))
                                table_id = table.get('tableId') or table.get('id') or table_name
                                table_type = table.get('tableType') or table.get('type') or 'BASE TABLE'

                                columns_url = f"{base_url}/public/api/v1/catalog/{catalog.get('catalogId')}/schema/{schema_id}/table/{table_id}/column"
                                
                                columns = []
                                col_attempt = 0
                                while True:
                                    try:
                                        columns_response = requests.get(columns_url, headers=headers, timeout=30)
                                        if not columns_response.text or columns_response.text.strip() == '':
                                            print(f"  Empty response for columns in table {table_name} - table has 0 columns")
                                            columns = []
                                            break
                                        columns_data = columns_response.json()
                                        
                                        for col in columns_data.get('result', []):
                                            col_name = col.get('columnId') or col.get('name') or col.get('columnName') or 'unknown_column'
                                            columns.append({
                                                "name": col_name,
                                                "type": col.get('dataType') or col.get('type') or 'STRING',
                                                "description": col.get('description', ''),
                                                "tags": [tag.get('name') for tag in col.get('tags', [])]
                                            })
                                        break
                                    except (json.JSONDecodeError, ValueError) as e:
                                        col_attempt += 1
                                        wait_time = min(2 ** min(col_attempt - 1, 5), 30)
                                        print(f"  Attempt {col_attempt}: JSON error for columns in table {table_name}: {e}")
                                        time.sleep(wait_time)
                                    except Exception as e:
                                        col_attempt += 1
                                        wait_time = min(2 ** min(col_attempt - 1, 5), 30)
                                        print(f"  Attempt {col_attempt}: Error for columns in table {table_name}: {e}")
                                        time.sleep(wait_time)

                                return {
                                    "id": f"starburst.{catalog_name}.{schema_name}.{table_name}",
                                    "name": table_name,
                                    "type": table_type,
                                    "catalog": catalog_name,
                                    "schema": schema_name,
                                    "connector_id": connector_id,
                                    "discovered_at": datetime.now().isoformat(),
                                    "status": "active",
                                    "description": table.get('comment', '') or "No description available",
                                    "columns": columns,
                                    "num_columns": len(columns),
                                    "technical_metadata": {
                                        "asset_id": f"starburst.{catalog_name}.{schema_name}.{table_name}",
                                        "asset_type": table_type,
                                        "location": f"{connection_data.account_domain}/{catalog_name}/{schema_name}/{table_name}",
                                        "format": "Starburst Table",
                                        "source_system": "Starburst Galaxy",
                                        "schema_name": schema_name,
                                        "table_name": table_name,
                                        "column_count": len(columns),
                                        "owner": "account_admin"
                                    },
                                    "operational_metadata": {
                                        "status": "active",
                                        "owner": "account_admin",
                                        "last_modified": datetime.now().isoformat(),
                                        "last_accessed": datetime.now().isoformat(),
                                        "access_count": "N/A",
                                        "data_quality_score": 95
                                    },
                                    "business_metadata": {
                                        "description": table.get('comment', '') or "No description available",
                                        "business_owner": "account_admin",
                                        "owner": "account_admin",
                                        "department": schema_name,
                                        "classification": "internal",
                                        "sensitivity_level": "low",
                                        "tags": []
                                    }
                                }
                                
                            except Exception as table_error:
                                print(f"  Error processing table: {table_error}")
                                return None
                        
                        emit('progress', {'type': 'progress', 'message': f' Fetching {len(tables)} tables in parallel (20 concurrent)...'}, namespace='/connectors')
                        with ThreadPoolExecutor(max_workers=20) as executor:
                            futures = [executor.submit(fetch_starburst_table, table) for table in tables]
                            
                            for future in as_completed(futures):
                                try:
                                    result = future.result()
                                    if result:
                                        all_tables.append(result)
                                        discovered_assets.append(result)
                                        discovered_assets_count += 1
                                        emit('progress', {'type': 'progress', 'message': f' Discovered table: {result["name"]} ({result["num_columns"]} columns)'}, namespace='/connectors')
                                except Exception as e:
                                    print(f"  Error in future result: {e}")
                                continue

                active_connectors.append({
                    "id": connector_id,
                    "name": connection_data.connection_name,
                    "type": "Starburst Galaxy",
                    "status": "active",
                    "enabled": True,
                    "last_run": datetime.now().isoformat(),
                    "config": {
                        "account_domain": connection_data.account_domain,
                        "client_id": connection_data.client_id,
                        "client_secret": connection_data.client_secret
                    },
                    "assets_count": discovered_assets_count
                })
                
                save_connectors()
                save_assets()
                
                total_columns = sum(len(asset.get('columns', [])) for asset in all_tables)
                emit('complete', ConnectionTestResponse(
                    success=True,
                    message=f" Successfully connected to Starburst Galaxy! Discovered {discovered_assets_count} assets with {total_columns} total columns",
                    discovered_assets=discovered_assets_count,
                    connector_id=connector_id
                ).model_dump(), namespace='/connectors')
                return
            
            except requests.exceptions.RequestException as e:
                print(f"Starburst API request failed: {e}")
                emit('error', {'type': 'error', 'message': f'Starburst Galaxy API error: {str(e)}'}, namespace='/connectors')
                return
            except Exception as e:
                print(f"Starburst connection failed: {e}")
                emit('error', {'type': 'error', 'message': f'Connection failed: {str(e)}'}, namespace='/connectors')
                return
        
        elif connection_type == 'Access Key' and selected_connector_id == 's3':
            print(f" S3 connector detected! Processing S3 connection...")
            try:
                print(f"   Config keys: {list(config.keys()) if config else 'None'}")
                print(f"   accessKeyId: {config.get('accessKeyId')[:10] + '...' if config.get('accessKeyId') else 'None'}")
                print(f"   secretAccessKey: {'***' if config.get('secretAccessKey') else 'None'}")
                print(f"   region: {config.get('region')}")
                print(f"   bucketName: {config.get('bucketName')}")
                print(f"   name: {config.get('name')}")
                
                connection_data = S3ConnectionTest(
                    access_key_id=config.get('accessKeyId'),
                    secret_access_key=config.get('secretAccessKey'),
                    region=config.get('region'),
                    bucket_name=config.get('bucketName'),
                    connection_name=config.get('name')
                )
                print(f" S3ConnectionTest created successfully")
            except Exception as e:
                print(f" Error creating S3ConnectionTest: {e}")
                import traceback
                traceback.print_exc()
                emit('error', {'type': 'error', 'message': f'Invalid S3 connection data: {str(e)}'}, namespace='/connectors')
                return
            
            try:
                emit('progress', {'type': 'progress', 'message': ' Authenticating with AWS S3...'}, namespace='/connectors')
                
                import boto3
                from botocore.exceptions import ClientError, NoCredentialsError
                
                region = connection_data.region or 'us-east-1'
                
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=connection_data.access_key_id,
                    aws_secret_access_key=connection_data.secret_access_key,
                    region_name=region
                )
                
                try:
                    s3_client.list_buckets()
                    emit('progress', {'type': 'progress', 'message': ' Authentication successful! Connected to AWS S3'}, namespace='/connectors')
                except NoCredentialsError:
                    emit('error', {'type': 'error', 'message': ' Invalid AWS credentials. Please check your Access Key ID and Secret Access Key.'}, namespace='/connectors')
                    return
                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code', '')
                    if error_code == 'InvalidAccessKeyId':
                        emit('error', {'type': 'error', 'message': ' Invalid AWS Access Key ID'}, namespace='/connectors')
                    elif error_code == 'SignatureDoesNotMatch':
                        emit('error', {'type': 'error', 'message': ' Invalid AWS Secret Access Key'}, namespace='/connectors')
                    else:
                        emit('error', {'type': 'error', 'message': f' AWS authentication error: {str(e)}'}, namespace='/connectors')
                    return
                
                if connection_data.bucket_name:
                    emit('progress', {'type': 'progress', 'message': f' Discovering assets in bucket: {connection_data.bucket_name}...'}, namespace='/connectors')
                    buckets_to_scan = [{'Name': connection_data.bucket_name}]
                else:
                    emit('progress', {'type': 'progress', 'message': ' Listing all accessible S3 buckets...'}, namespace='/connectors')
                    buckets_response = s3_client.list_buckets()
                    buckets_to_scan = buckets_response.get('Buckets', [])
                    emit('progress', {'type': 'progress', 'message': f' Found {len(buckets_to_scan)} buckets! Starting asset discovery...'}, namespace='/connectors')
                
                assets_discovered = 0
                connector_id = f"s3_{region.replace('-', '_') if region else 'all_regions'}_{datetime.now().timestamp()}"
                
                for bucket_info in buckets_to_scan:
                    bucket_name_actual = bucket_info['Name']
                    
                    try:
                        bucket_location_response = s3_client.get_bucket_location(Bucket=bucket_name_actual)
                        bucket_region = bucket_location_response.get('LocationConstraint') or 'us-east-1'
                        if bucket_region is None or bucket_region == '':
                            bucket_region = 'us-east-1'
                        emit('progress', {'type': 'progress', 'message': f'  Discovering objects in bucket: {bucket_name_actual} (region: {bucket_region})...'}, namespace='/connectors')
                    except ClientError:
                        bucket_region = region
                        emit('progress', {'type': 'progress', 'message': f'  Discovering objects in bucket: {bucket_name_actual}...'}, namespace='/connectors')
                    
                    try:
                        bucket_s3_client = boto3.client(
                            's3',
                            aws_access_key_id=connection_data.access_key_id,
                            aws_secret_access_key=connection_data.secret_access_key,
                            region_name=bucket_region
                        )
                        
                        paginator = bucket_s3_client.get_paginator('list_objects_v2')
                        pages = paginator.paginate(Bucket=bucket_name_actual)
                        
                        bucket_objects = 0
                        for page in pages:
                            if 'Contents' not in page:
                                continue
                            
                            for obj in page['Contents']:
                                key = obj['Key']
                                object_name = key.split('/')[-1] if '/' in key else key
                                size = obj.get('Size', 0)
                                last_modified = obj.get('LastModified', datetime.now())
                                storage_class = obj.get('StorageClass', 'STANDARD')
                                
                                asset_type = 'File'
                                file_format = 'Unknown'
                                file_extension = ''
                                
                                if key.endswith('/'):
                                    asset_type = 'Folder'
                                    file_format = 'Directory'
                                else:
                                    if '.' in key:
                                        file_extension = key.lower().split('.')[-1]
                                    
                                    if file_extension in ['csv', 'tsv']:
                                        asset_type = 'Data File'
                                        file_format = f'{file_extension.upper()} (Comma/Tab Separated)'
                                    elif file_extension == 'json':
                                        asset_type = 'Data File'
                                        file_format = 'JSON (JavaScript Object Notation)'
                                    elif file_extension in ['parquet']:
                                        asset_type = 'Data File'
                                        file_format = 'Parquet (Columnar Storage)'
                                    elif file_extension in ['avro']:
                                        asset_type = 'Data File'
                                        file_format = 'Avro (Binary Format)'
                                    elif file_extension in ['orc']:
                                        asset_type = 'Data File'
                                        file_format = 'ORC (Optimized Row Columnar)'
                                    elif file_extension in ['sql']:
                                        asset_type = 'Script'
                                        file_format = 'SQL (Structured Query Language)'
                                    elif file_extension in ['py']:
                                        asset_type = 'Script'
                                        file_format = 'Python Script'
                                    elif file_extension in ['scala']:
                                        asset_type = 'Script'
                                        file_format = 'Scala Script'
                                    elif file_extension in ['r', 'rscript']:
                                        asset_type = 'Script'
                                        file_format = 'R Script'
                                    elif file_extension in ['txt', 'log']:
                                        asset_type = 'Text File'
                                        file_format = 'Text/Log File'
                                    elif file_extension in ['zip', 'gz', 'tar', 'bz2', '7z']:
                                        asset_type = 'Archive'
                                        file_format = f'{file_extension.upper()} Archive'
                                    elif file_extension in ['xlsx', 'xls']:
                                        asset_type = 'Data File'
                                        file_format = 'Excel Spreadsheet'
                                    elif file_extension in ['pdf']:
                                        asset_type = 'Document'
                                        file_format = 'PDF Document'
                                    elif file_extension in ['jpg', 'jpeg', 'png', 'gif', 'svg']:
                                        asset_type = 'Image'
                                        file_format = f'{file_extension.upper()} Image'
                                    elif file_extension in ['mp4', 'avi', 'mov', 'mkv']:
                                        asset_type = 'Video'
                                        file_format = f'{file_extension.upper()} Video'
                                    elif file_extension in ['mp3', 'wav', 'flac']:
                                        asset_type = 'Audio'
                                        file_format = f'{file_extension.upper()} Audio'
                                    elif file_extension:
                                        asset_type = 'File'
                                        file_format = f'{file_extension.upper()} File'
                                
                                try:
                                    metadata_response = bucket_s3_client.head_object(Bucket=bucket_name_actual, Key=key)
                                    content_type = metadata_response.get('ContentType', '')
                                    metadata = metadata_response.get('Metadata', {})
                                    
                                    if content_type and content_type != 'binary/octet-stream':
                                        mime_to_format = {
                                            'text/csv': 'CSV (Comma Separated Values)',
                                            'application/json': 'JSON (JavaScript Object Notation)',
                                            'application/x-parquet': 'Parquet (Columnar Storage)',
                                            'application/avro': 'Avro (Binary Format)',
                                            'application/x-orc': 'ORC (Optimized Row Columnar)',
                                            'text/plain': 'Text File',
                                            'application/zip': 'ZIP Archive',
                                            'application/x-gzip': 'GZIP Archive',
                                            'application/x-tar': 'TAR Archive',
                                            'application/pdf': 'PDF Document',
                                            'application/vnd.ms-excel': 'Excel Spreadsheet',
                                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'Excel Spreadsheet (XLSX)',
                                            'image/jpeg': 'JPEG Image',
                                            'image/png': 'PNG Image',
                                            'image/gif': 'GIF Image',
                                            'video/mp4': 'MP4 Video',
                                            'audio/mpeg': 'MP3 Audio',
                                        }
                                        
                                        if content_type in mime_to_format:
                                            file_format = mime_to_format[content_type]
                                        elif file_format == 'Unknown':
                                            file_format = content_type
                                    
                                except ClientError:
                                    content_type = ''
                                    metadata = {}
                                
                                columns = []
                                
                                data_file_extensions = {
                                    'csv': 'csv',
                                    'tsv': 'tsv',
                                    'psv': 'csv',
                                    'ssv': 'csv',
                                    'json': 'json',
                                    'jsonl': 'jsonl',
                                    'ndjson': 'jsonl',
                                    'jsonc': 'json',
                                    'json5': 'json',
                                    'xml': 'xml',
                                    'html': 'xml',
                                    'xhtml': 'xml',
                                    'yaml': 'yaml',
                                    'yml': 'yaml',
                                    'toml': 'toml',
                                    'ini': 'ini',
                                    'cfg': 'ini',
                                    'conf': 'ini',
                                    'properties': 'properties',
                                    'txt': 'text',
                                    'log': 'text',
                                    'md': 'text',
                                    'markdown': 'text',
                                    
                                    'parquet': 'parquet',
                                    'orc': 'orc',
                                    'feather': 'feather',
                                    'arrow': 'arrow',
                                    'ipc': 'arrow',
                                    
                                    'avro': 'avro',
                                    'avsc': 'avro',
                                    'protobuf': 'protobuf',
                                    'proto': 'protobuf',
                                    'pb': 'protobuf',
                                    'msgpack': 'msgpack',
                                    'bson': 'bson',
                                    'cbor': 'cbor',
                                    'thrift': 'thrift',
                                    'flatbuffers': 'flatbuffers',
                                    'fbs': 'flatbuffers',
                                    
                                    'delta': 'delta',
                                    'iceberg': 'iceberg',
                                    'hudi': 'hudi',
                                    
                                    'xlsx': 'excel',
                                    'xls': 'excel',
                                    'xlsm': 'excel',
                                    'xlsb': 'excel',
                                    'ods': 'excel',
                                    'fods': 'excel',
                                    'numbers': 'excel',
                                    
                                    'sql': 'sql',
                                    'dump': 'sql',
                                    'mysqldump': 'sql',
                                    'pgdump': 'sql',
                                    'dbf': 'dbf',
                                    'mdb': 'mdb',
                                    'accdb': 'mdb',
                                    
                                    'hdf5': 'hdf5',
                                    'h5': 'hdf5',
                                    'hdf': 'hdf5',
                                    'nc': 'netcdf',
                                    'netcdf': 'netcdf',
                                    'cdf': 'netcdf',
                                    'fits': 'fits',
                                    'fits.gz': 'fits',
                                    
                                    'tfrecord': 'tfrecord',
                                    'tfrecords': 'tfrecord',
                                    'tf': 'tfrecord',
                                    'pkl': 'pickle',
                                    'pickle': 'pickle',
                                    'joblib': 'joblib',
                                    'pt': 'pytorch',
                                    'pth': 'pytorch',
                                    'onnx': 'onnx',
                                    'h5': 'keras',
                                    'keras': 'keras',
                                    'pb': 'tensorflow',
                                    'savedmodel': 'tensorflow',
                                    'ckpt': 'tensorflow',
                                    'weights': 'tensorflow',
                                    
                                    'sas7bdat': 'sas',
                                    'sas': 'sas',
                                    'sav': 'spss',
                                    'spss': 'spss',
                                    'dta': 'stata',
                                    'stata': 'stata',
                                    'rdata': 'r',
                                    'rds': 'r',
                                    
                                    'graphml': 'graphml',
                                    'gml': 'graphml',
                                    'gexf': 'gexf',
                                    'neo4j': 'neo4j',
                                    'cypher': 'cypher',
                                    
                                    'tsv': 'timeseries',
                                    'ts': 'timeseries',
                                    'influx': 'influx',
                                    
                                    'shp': 'shapefile',
                                    'geojson': 'geojson',
                                    'kml': 'kml',
                                    'kmz': 'kml',
                                    'gpx': 'gpx',
                                    'topojson': 'topojson',
                                    
                                    'jpg': 'image',
                                    'jpeg': 'image',
                                    'png': 'image',
                                    'gif': 'image',
                                    'bmp': 'image',
                                    'tiff': 'image',
                                    'tif': 'image',
                                    'webp': 'image',
                                    'svg': 'image',
                                    'ico': 'image',
                                    'heic': 'image',
                                    'heif': 'image',
                                    
                                    'mp4': 'video',
                                    'avi': 'video',
                                    'mov': 'video',
                                    'mkv': 'video',
                                    'wmv': 'video',
                                    'flv': 'video',
                                    'webm': 'video',
                                    'm4v': 'video',
                                    
                                    'mp3': 'audio',
                                    'wav': 'audio',
                                    'flac': 'audio',
                                    'aac': 'audio',
                                    'ogg': 'audio',
                                    'wma': 'audio',
                                    'm4a': 'audio',
                                    
                                    'pdf': 'pdf',
                                    'doc': 'doc',
                                    'docx': 'doc',
                                    'rtf': 'rtf',
                                    'odt': 'odt',
                                    'epub': 'epub',
                                    'mobi': 'mobi',
                                    
                                    'gz': 'compressed',
                                    'gzip': 'compressed',
                                    'zip': 'compressed',
                                    'bz2': 'compressed',
                                    'xz': 'compressed',
                                    'lz4': 'compressed',
                                    'zstd': 'compressed',
                                    'zst': 'compressed',
                                    '7z': 'compressed',
                                    'rar': 'compressed',
                                    'tar': 'compressed',
                                    'tar.gz': 'compressed',
                                    'tgz': 'compressed',
                                    'tar.bz2': 'compressed',
                                    'tbz2': 'compressed',
                                    'tar.xz': 'compressed',
                                    'txz': 'compressed',
                                    'snappy': 'compressed',
                                    'lzo': 'compressed',
                                    'lzma': 'compressed',
                                    
                                    'deb': 'archive',
                                    'rpm': 'archive',
                                    'apk': 'archive',
                                    'dmg': 'archive',
                                    'iso': 'archive',
                                    'bin': 'archive',
                                    'exe': 'archive',
                                    'msi': 'archive',
                                    
                                    'rss': 'xml',
                                    'atom': 'xml',
                                    'sitemap': 'xml',
                                    'sitemap.xml': 'xml',
                                    
                                    'env': 'env',
                                    'dockerfile': 'text',
                                    'makefile': 'text',
                                    'cmake': 'text',
                                    
                                    'r': 'r',
                                    'py': 'text',
                                    'ipynb': 'jupyter',
                                    'jupyter': 'jupyter',
                                    
                                    'edf': 'edf',
                                    'edf+': 'edf',
                                    'bdf': 'edf',
                                    'mat': 'matlab',
                                    'matlab': 'matlab',
                                    'vtk': 'vtk',
                                    'vtp': 'vtk',
                                    'ply': 'ply',
                                    'obj': 'obj',
                                    'stl': 'stl',
                                    'fbx': 'fbx',
                                    'dae': 'dae',
                                    'x3d': 'x3d',
                                    'gltf': 'gltf',
                                    'glb': 'gltf',
                                    
                                    'ofx': 'ofx',
                                    'qif': 'qif',
                                    'mt940': 'mt940',
                                    'mt942': 'mt942',
                                    
                                    'fasta': 'fasta',
                                    'fa': 'fasta',
                                    'fastq': 'fastq',
                                    'fq': 'fastq',
                                    'sam': 'sam',
                                    'bam': 'bam',
                                    'vcf': 'vcf',
                                    'gff': 'gff',
                                    'gtf': 'gtf',
                                    'bed': 'bed',
                                    'wig': 'wig',
                                    'bigwig': 'bigwig',
                                    'bigbed': 'bigbed',
                                    
                                    'yaml': 'yaml',
                                    'yml': 'yaml',
                                    'toml': 'toml',
                                    'plist': 'plist',
                                    'xplist': 'plist',
                                    'binary': 'binary',
                                    'dat': 'binary',
                                }
                                
                                if asset_type == 'Data File' and file_extension in data_file_extensions:
                                    try:
                                        file_type = data_file_extensions[file_extension]
                                        
                                        if file_type in ['csv', 'tsv']:
                                            emit('progress', {'type': 'progress', 'message': f'📖 Reading entire {file_extension.upper()} file: {object_name}...'}, namespace='/connectors')
                                            csv_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                            csv_content = csv_obj['Body'].read().decode('utf-8', errors='ignore')
                                            csv_lines = csv_content.split('\n')
                                            
                                            if csv_lines:
                                                import csv as csv_module
                                                import io
                                                delimiter = ',' if file_extension == 'csv' else '\t'
                                                
                                                reader = csv_module.reader(io.StringIO(csv_lines[0]), delimiter=delimiter)
                                                header_row = next(reader, [])
                                                
                                                all_data_rows = []
                                                for line in csv_lines[1:]:
                                                    if line.strip():
                                                        try:
                                                            reader_row = csv_module.reader(io.StringIO(line), delimiter=delimiter)
                                                            row_data = next(reader_row, [])
                                                            if row_data:
                                                                all_data_rows.append(row_data)
                                                        except:
                                                            continue
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzing {len(all_data_rows)} rows in {object_name}...'}, namespace='/connectors')
                                                
                                                for i, col_name in enumerate(header_row):
                                                    col_name = col_name.strip()
                                                    if col_name:
                                                        col_type = 'STRING'
                                                        integer_count = 0
                                                        float_count = 0
                                                        boolean_count = 0
                                                        non_empty_count = 0
                                                        
                                                        for row in all_data_rows:
                                                            if i < len(row):
                                                                val = row[i].strip()
                                                                if val:
                                                                    non_empty_count += 1
                                                                    if val.replace('-', '').replace('+', '').isdigit():
                                                                        integer_count += 1
                                                                    elif val.replace('.', '').replace('-', '').replace('+', '').isdigit() and '.' in val:
                                                                        float_count += 1
                                                                    elif val.lower() in ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n']:
                                                                        boolean_count += 1
                                                        
                                                        if non_empty_count > 0:
                                                            if boolean_count / non_empty_count > 0.8:
                                                                col_type = 'BOOLEAN'
                                                            elif float_count / non_empty_count > 0.5:
                                                                col_type = 'FLOAT'
                                                            elif integer_count / non_empty_count > 0.8:
                                                                col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzed {len(all_data_rows)} rows, found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                        
                                        elif file_type == 'parquet':
                                            try:
                                                import pyarrow.parquet as pq
                                                import io
                                                
                                                parquet_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                parquet_buffer = io.BytesIO(parquet_obj['Body'].read())
                                                parquet_file = pq.ParquetFile(parquet_buffer)
                                                schema = parquet_file.schema_arrow
                                                
                                                for field in schema:
                                                    pii_detected, pii_type = detect_pii_in_column(field.name, str(field.type))
                                                    columns.append({
                                                        "name": field.name,
                                                        "type": str(field.type),
                                                        "mode": "NULLABLE" if field.nullable else "REQUIRED",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyarrow not installed, skipping Parquet column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Parquet schema: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'json':
                                            json_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                            json_content = json_obj['Body'].read().decode('utf-8', errors='ignore')
                                            
                                            try:
                                                json_data = json.loads(json_content)
                                                
                                                all_fields = {}
                                                
                                                if isinstance(json_data, list):
                                                    for obj in json_data:
                                                        if isinstance(obj, dict):
                                                            for col_name, col_value in obj.items():
                                                                if col_name not in all_fields:
                                                                    all_fields[col_name] = []
                                                                all_fields[col_name].append(col_value)
                                                elif isinstance(json_data, dict):
                                                    for col_name, col_value in json_data.items():
                                                        all_fields[col_name] = [col_value]
                                                
                                                for col_name, values in all_fields.items():
                                                    col_type = 'STRING'
                                                    integer_count = 0
                                                    float_count = 0
                                                    boolean_count = 0
                                                    non_empty_count = 0
                                                    
                                                    for val in values:
                                                        if val is not None:
                                                            non_empty_count += 1
                                                            val_type = type(val).__name__.upper()
                                                            if val_type == 'INT' or val_type == 'INTEGER':
                                                                integer_count += 1
                                                            elif val_type == 'FLOAT':
                                                                float_count += 1
                                                            elif val_type == 'BOOL' or val_type == 'BOOLEAN':
                                                                boolean_count += 1
                                                    
                                                    if non_empty_count > 0:
                                                        if boolean_count / non_empty_count > 0.8:
                                                            col_type = 'BOOLEAN'
                                                        elif float_count / non_empty_count > 0.5:
                                                            col_type = 'FLOAT'
                                                        elif integer_count / non_empty_count > 0.8:
                                                            col_type = 'INTEGER'
                                                    
                                                    pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                    columns.append({
                                                        "name": col_name,
                                                        "type": col_type,
                                                        "mode": "NULLABLE",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzed {len(json_data) if isinstance(json_data, list) else 1} JSON records in {object_name}'}, namespace='/connectors')
                                            except json.JSONDecodeError:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse JSON structure'}, namespace='/connectors')
                                        
                                        elif file_type == 'jsonl':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading entire JSONL file: {object_name}...'}, namespace='/connectors')
                                                jsonl_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                jsonl_content = jsonl_obj['Body'].read().decode('utf-8', errors='ignore')
                                                jsonl_lines = jsonl_content.split('\n')
                                                
                                                all_fields = {}
                                                record_count = 0
                                                
                                                for line in jsonl_lines:
                                                    if line.strip():
                                                        try:
                                                            obj = json.loads(line)
                                                            if isinstance(obj, dict):
                                                                record_count += 1
                                                                for col_name, col_value in obj.items():
                                                                    if col_name not in all_fields:
                                                                        all_fields[col_name] = []
                                                                    all_fields[col_name].append(col_value)
                                                        except json.JSONDecodeError:
                                                            continue
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzing {record_count} JSONL records in {object_name}...'}, namespace='/connectors')
                                                
                                                for col_name, values in all_fields.items():
                                                    col_type = 'STRING'
                                                    integer_count = 0
                                                    float_count = 0
                                                    boolean_count = 0
                                                    non_empty_count = 0
                                                    
                                                    for val in values:
                                                        if val is not None:
                                                            non_empty_count += 1
                                                            val_type = type(val).__name__.upper()
                                                            if val_type == 'INT' or val_type == 'INTEGER':
                                                                integer_count += 1
                                                            elif val_type == 'FLOAT':
                                                                float_count += 1
                                                            elif val_type == 'BOOL' or val_type == 'BOOLEAN':
                                                                boolean_count += 1
                                                    
                                                    if non_empty_count > 0:
                                                        if boolean_count / non_empty_count > 0.8:
                                                            col_type = 'BOOLEAN'
                                                        elif float_count / non_empty_count > 0.5:
                                                            col_type = 'FLOAT'
                                                        elif integer_count / non_empty_count > 0.8:
                                                            col_type = 'INTEGER'
                                                    
                                                    pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                    columns.append({
                                                        "name": col_name,
                                                        "type": col_type,
                                                        "mode": "NULLABLE",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzed {record_count} JSONL records, found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse JSONL file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'xml':
                                            try:
                                                import xml.etree.ElementTree as ET
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading XML file: {object_name}...'}, namespace='/connectors')
                                                xml_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                xml_content = xml_obj['Body'].read().decode('utf-8', errors='ignore')
                                                
                                                root = ET.fromstring(xml_content)
                                                
                                                all_elements = {}
                                                
                                                def extract_elements(element, path=''):
                                                    current_path = f"{path}/{element.tag}" if path else element.tag
                                                    if element.text and element.text.strip():
                                                        if current_path not in all_elements:
                                                            all_elements[current_path] = []
                                                        all_elements[current_path].append(element.text.strip())
                                                    for child in element:
                                                        extract_elements(child, current_path)
                                                
                                                extract_elements(root)
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzing XML structure in {object_name}...'}, namespace='/connectors')
                                                
                                                for col_name, values in all_elements.items():
                                                    col_type = 'STRING'
                                                    if values:
                                                        integer_count = sum(1 for v in values if v.replace('-', '').isdigit())
                                                        float_count = sum(1 for v in values if v.replace('.', '').replace('-', '').isdigit() and '.' in v)
                                                        if len(values) > 0:
                                                            if float_count / len(values) > 0.5:
                                                                col_type = 'FLOAT'
                                                            elif integer_count / len(values) > 0.8:
                                                                col_type = 'INTEGER'
                                                    
                                                    pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                    columns.append({
                                                        "name": col_name,
                                                        "type": col_type,
                                                        "mode": "NULLABLE",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} XML elements in {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse XML file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'feather':
                                            try:
                                                import pyarrow.feather as feather
                                                import io
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Feather file: {object_name}...'}, namespace='/connectors')
                                                feather_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                feather_buffer = io.BytesIO(feather_obj['Body'].read())
                                                
                                                table = feather.read_table(feather_buffer)
                                                schema = table.schema
                                                
                                                for field in schema:
                                                    pii_detected, pii_type = detect_pii_in_column(field.name, str(field.type))
                                                    columns.append({
                                                        "name": field.name,
                                                        "type": str(field.type),
                                                        "mode": "NULLABLE" if field.nullable else "REQUIRED",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyarrow not installed, skipping Feather column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Feather file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'arrow':
                                            try:
                                                import pyarrow as pa
                                                import io
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Arrow file: {object_name}...'}, namespace='/connectors')
                                                arrow_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                arrow_buffer = io.BytesIO(arrow_obj['Body'].read())
                                                
                                                reader = pa.ipc.open_stream(arrow_buffer)
                                                table = reader.read_all()
                                                schema = table.schema
                                                
                                                for field in schema:
                                                    pii_detected, pii_type = detect_pii_in_column(field.name, str(field.type))
                                                    columns.append({
                                                        "name": field.name,
                                                        "type": str(field.type),
                                                        "mode": "NULLABLE" if field.nullable else "REQUIRED",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyarrow not installed, skipping Arrow column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Arrow file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'delta':
                                            try:
                                                from deltalake import DeltaTable  # type: ignore
                                                import tempfile
                                                import os
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Delta Lake table: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                                                    delta_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(delta_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    dt = DeltaTable(tmp_path)
                                                    schema = dt.schema()
                                                    
                                                    for field in schema.fields:
                                                        pii_detected, pii_type = detect_pii_in_column(field.name, str(field.type))
                                                        columns.append({
                                                            "name": field.name,
                                                            "type": str(field.type),
                                                            "mode": "NULLABLE" if field.nullable else "REQUIRED",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in Delta table {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  deltalake not installed, skipping Delta Lake column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Delta Lake table: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'iceberg':
                                            try:
                                                from pyiceberg.catalog import load_catalog  # type: ignore
                                                from pyiceberg.table import Table  # type: ignore
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Iceberg table: {object_name}...'}, namespace='/connectors')
                                                emit('progress', {'type': 'progress', 'message': f'  Iceberg requires catalog configuration - skipping for now'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyiceberg not installed, skipping Iceberg column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Iceberg table: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'protobuf':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Protobuf file: {object_name}...'}, namespace='/connectors')
                                                emit('progress', {'type': 'progress', 'message': f'  Protobuf requires schema file - skipping column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Protobuf file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'msgpack':
                                            try:
                                                import msgpack
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading MessagePack file: {object_name}...'}, namespace='/connectors')
                                                msgpack_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                msgpack_data = msgpack.unpackb(msgpack_obj['Body'].read(), raw=False)
                                                
                                                if isinstance(msgpack_data, dict):
                                                    for col_name, col_value in msgpack_data.items():
                                                        col_type = type(col_value).__name__.upper()
                                                        if col_type == 'STR':
                                                            col_type = 'STRING'
                                                        elif col_type == 'INT':
                                                            col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  msgpack not installed, skipping MessagePack column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read MessagePack file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'bson':
                                            try:
                                                import bson
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading BSON file: {object_name}...'}, namespace='/connectors')
                                                bson_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                bson_data = bson.loads(bson_obj['Body'].read())
                                                
                                                if isinstance(bson_data, dict):
                                                    for col_name, col_value in bson_data.items():
                                                        col_type = type(col_value).__name__.upper()
                                                        if col_type == 'STR':
                                                            col_type = 'STRING'
                                                        elif col_type == 'INT':
                                                            col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  bson not installed, skipping BSON column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read BSON file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'sql':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading SQL dump: {object_name}...'}, namespace='/connectors')
                                                sql_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                sql_content = sql_obj['Body'].read().decode('utf-8', errors='ignore')
                                                
                                                import re
                                                create_table_pattern = r'CREATE\s+TABLE\s+(?:\w+\.)?(\w+)\s*\((.*?)\);'
                                                matches = re.findall(create_table_pattern, sql_content, re.IGNORECASE | re.DOTALL)
                                                
                                                for table_name, table_def in matches:
                                                    col_pattern = r'(\w+)\s+(\w+(?:\([^)]+\))?)'
                                                    col_matches = re.findall(col_pattern, table_def)
                                                    
                                                    for col_name, col_type in col_matches:
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type.upper())
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type.upper(),
                                                            "mode": "NULLABLE",
                                                            "description": f"From table {table_name}",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in SQL dump {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse SQL dump: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'text':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading text file: {object_name}...'}, namespace='/connectors')
                                                text_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                text_content = text_obj['Body'].read().decode('utf-8', errors='ignore')
                                                text_lines = text_content.split('\n')[:1000]
                                                
                                                import re
                                                
                                                kv_pattern = r'(\w+)\s*[=:]\s*([^\s]+)'
                                                all_keys = set()
                                                
                                                for line in text_lines:
                                                    matches = re.findall(kv_pattern, line)
                                                    for key, value in matches:
                                                        all_keys.add(key)
                                                
                                                if all_keys:
                                                    for key in all_keys:
                                                        pii_detected, pii_type = detect_pii_in_column(key, 'STRING')
                                                        columns.append({
                                                            "name": key,
                                                            "type": "STRING",
                                                            "mode": "NULLABLE",
                                                            "description": "Extracted from text patterns",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} patterns in {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not analyze text file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'compressed':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f' Detecting compressed file format: {object_name}...'}, namespace='/connectors')
                                                
                                                base_key = key.rsplit('.', 1)[0] if '.' in key else key
                                                inner_extension = base_key.split('.')[-1].lower() if '.' in base_key else ''
                                                
                                                compressed_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                compressed_data = compressed_obj['Body'].read()
                                                
                                                if file_extension in ['gz', 'gzip']:
                                                    import gzip
                                                    decompressed = gzip.decompress(compressed_data)
                                                elif file_extension == 'bz2':
                                                    import bz2
                                                    decompressed = bz2.decompress(compressed_data)
                                                elif file_extension == 'xz':
                                                    import lzma
                                                    decompressed = lzma.decompress(compressed_data)
                                                elif file_extension == 'zip':
                                                    import zipfile
                                                    import io
                                                    zip_file = zipfile.ZipFile(io.BytesIO(compressed_data))
                                                    if zip_file.namelist():
                                                        decompressed = zip_file.read(zip_file.namelist()[0])
                                                    else:
                                                        decompressed = None
                                                else:
                                                    decompressed = None
                                                
                                                if decompressed and inner_extension in data_file_extensions:
                                                    emit('progress', {'type': 'progress', 'message': f'📖 Processing inner {inner_extension.upper()} file from compressed archive...'}, namespace='/connectors')
                                                    emit('progress', {'type': 'progress', 'message': f'  Nested compression processing not yet fully implemented'}, namespace='/connectors')
                                                else:
                                                    emit('progress', {'type': 'progress', 'message': f'  Could not determine inner format or unsupported compression'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not decompress file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'yaml':
                                            try:
                                                import yaml
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading YAML file: {object_name}...'}, namespace='/connectors')
                                                yaml_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                yaml_content = yaml_obj['Body'].read().decode('utf-8', errors='ignore')
                                                yaml_data = yaml.safe_load(yaml_content)
                                                
                                                if isinstance(yaml_data, dict):
                                                    for col_name, col_value in yaml_data.items():
                                                        col_type = type(col_value).__name__.upper()
                                                        if col_type == 'STR':
                                                            col_type = 'STRING'
                                                        elif col_type == 'INT':
                                                            col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyyaml not installed, skipping YAML column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse YAML: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'toml':
                                            try:
                                                import tomli
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading TOML file: {object_name}...'}, namespace='/connectors')
                                                toml_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                toml_content = toml_obj['Body'].read().decode('utf-8', errors='ignore')
                                                toml_data = tomli.loads(toml_content)
                                                
                                                if isinstance(toml_data, dict):
                                                    for col_name, col_value in toml_data.items():
                                                        col_type = type(col_value).__name__.upper()
                                                        if col_type == 'STR':
                                                            col_type = 'STRING'
                                                        elif col_type == 'INT':
                                                            col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  tomli not installed, skipping TOML column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse TOML: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'hudi':
                                            try:
                                                from pyhudi import HudiTable  # type: ignore
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Hudi table: {object_name}...'}, namespace='/connectors')
                                                emit('progress', {'type': 'progress', 'message': f'  Hudi requires table configuration - skipping for now'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyhudi not installed, skipping Hudi column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Hudi table: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'hdf5':
                                            try:
                                                import h5py
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading HDF5 file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    hdf5_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(hdf5_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    with h5py.File(tmp_path, 'r') as f:
                                                        def extract_datasets(name, obj):
                                                            if isinstance(obj, h5py.Dataset):
                                                                pii_detected, pii_type = detect_pii_in_column(name, str(obj.dtype))
                                                                columns.append({
                                                                    "name": name,
                                                                    "type": str(obj.dtype),
                                                                    "mode": "NULLABLE",
                                                                    "description": f"HDF5 dataset: {obj.shape}",
                                                                    "pii_detected": pii_detected,
                                                                    "pii_type": pii_type
                                                                })
                                                        
                                                        f.visititems(extract_datasets)
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} datasets in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  h5py not installed, skipping HDF5 column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read HDF5 file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'netcdf':
                                            try:
                                                from netCDF4 import Dataset
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading NetCDF file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    nc_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(nc_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    with Dataset(tmp_path, 'r') as nc:
                                                        for var_name in nc.variables:
                                                            var = nc.variables[var_name]
                                                            pii_detected, pii_type = detect_pii_in_column(var_name, str(var.dtype))
                                                            columns.append({
                                                                "name": var_name,
                                                                "type": str(var.dtype),
                                                                "mode": "NULLABLE",
                                                                "description": f"NetCDF variable: {var.shape}",
                                                                "pii_detected": pii_detected,
                                                                "pii_type": pii_type
                                                            })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} variables in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  netCDF4 not installed, skipping NetCDF column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read NetCDF file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'tfrecord':
                                            try:
                                                import tensorflow as tf  # type: ignore
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading TFRecord file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    tf_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(tf_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    dataset = tf.data.TFRecordDataset(tmp_path)
                                                    for raw_record in dataset.take(1):
                                                        example = tf.train.Example()
                                                        example.ParseFromString(raw_record.numpy())
                                                        feature_dict = example.features.feature
                                                        
                                                        for feature_name, feature in feature_dict.items():
                                                            col_type = 'STRING'
                                                            if feature.HasField('int64_list'):
                                                                col_type = 'INTEGER'
                                                            elif feature.HasField('float_list'):
                                                                col_type = 'FLOAT'
                                                            elif feature.HasField('bytes_list'):
                                                                col_type = 'BYTES'
                                                            
                                                            pii_detected, pii_type = detect_pii_in_column(feature_name, col_type)
                                                            columns.append({
                                                                "name": feature_name,
                                                                "type": col_type,
                                                                "mode": "NULLABLE",
                                                                "description": "TensorFlow feature",
                                                                "pii_detected": pii_detected,
                                                                "pii_type": pii_type
                                                            })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} features in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  tensorflow not installed, skipping TFRecord column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read TFRecord: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'pickle':
                                            try:
                                                import pickle
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading pickle file: {object_name}...'}, namespace='/connectors')
                                                pickle_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                pickle_data = pickle.loads(pickle_obj['Body'].read())
                                                
                                                if isinstance(pickle_data, dict):
                                                    for col_name, col_value in pickle_data.items():
                                                        col_type = type(col_value).__name__.upper()
                                                        if col_type == 'STR':
                                                            col_type = 'STRING'
                                                        elif col_type == 'INT':
                                                            col_type = 'INTEGER'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "Pickled data",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                elif hasattr(pickle_data, '__dict__'):
                                                    for attr_name in dir(pickle_data):
                                                        if not attr_name.startswith('_'):
                                                            pii_detected, pii_type = detect_pii_in_column(attr_name, 'STRING')
                                                            columns.append({
                                                                "name": attr_name,
                                                                "type": "STRING",
                                                                "mode": "NULLABLE",
                                                                "description": "Pickled object attribute",
                                                                "pii_detected": pii_detected,
                                                                "pii_type": pii_type
                                                            })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} fields in {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read pickle file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'sas':
                                            try:
                                                import pandas as pd
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading SAS file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    sas_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(sas_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    df = pd.read_sas(tmp_path)
                                                    for col_name in df.columns:
                                                        col_type = str(df[col_name].dtype).upper()
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "SAS data",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pandas not installed, skipping SAS column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read SAS file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'spss':
                                            try:
                                                import pyreadstat
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading SPSS file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    spss_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(spss_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    df, meta = pyreadstat.read_sav(tmp_path)
                                                    for col_name in df.columns:
                                                        col_type = str(df[col_name].dtype).upper()
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "SPSS data",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyreadstat not installed, skipping SPSS column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read SPSS file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'stata':
                                            try:
                                                import pandas as pd
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Stata file: {object_name}...'}, namespace='/connectors')
                                                
                                                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                                    stata_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                    tmp_file.write(stata_obj['Body'].read())
                                                    tmp_path = tmp_file.name
                                                
                                                try:
                                                    df = pd.read_stata(tmp_path)
                                                    for col_name in df.columns:
                                                        col_type = str(df[col_name].dtype).upper()
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "Stata data",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                    
                                                    emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                                finally:
                                                    os.unlink(tmp_path)
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pandas not installed, skipping Stata column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Stata file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'geojson':
                                            try:
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading GeoJSON file: {object_name}...'}, namespace='/connectors')
                                                geojson_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                geojson_content = geojson_obj['Body'].read().decode('utf-8', errors='ignore')
                                                geojson_data = json.loads(geojson_content)
                                                
                                                if 'features' in geojson_data:
                                                    all_properties = {}
                                                    for feature in geojson_data['features']:
                                                        if 'properties' in feature:
                                                            for prop_name, prop_value in feature['properties'].items():
                                                                if prop_name not in all_properties:
                                                                    all_properties[prop_name] = []
                                                                all_properties[prop_name].append(prop_value)
                                                    
                                                    for col_name, values in all_properties.items():
                                                        col_type = 'STRING'
                                                        if values:
                                                            integer_count = sum(1 for v in values if isinstance(v, (int, float)) and not isinstance(v, bool))
                                                            if integer_count / len(values) > 0.8:
                                                                col_type = 'INTEGER' if all(isinstance(v, int) for v in values if isinstance(v, (int, float))) else 'FLOAT'
                                                        
                                                        pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                        columns.append({
                                                            "name": col_name,
                                                            "type": col_type,
                                                            "mode": "NULLABLE",
                                                            "description": "GeoJSON property",
                                                            "pii_detected": pii_detected,
                                                            "pii_type": pii_type
                                                        })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} properties in {object_name}'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not parse GeoJSON: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type in ['image', 'video', 'audio', 'pdf', 'doc', 'archive', 'binary']:
                                            emit('progress', {'type': 'progress', 'message': f'📄 {file_type.upper()} file detected - extracting metadata for {object_name}...'}, namespace='/connectors')
                                        
                                        elif file_type == 'excel':
                                            try:
                                                import pandas as pd
                                                import io
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading entire Excel file: {object_name}...'}, namespace='/connectors')
                                                excel_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                excel_buffer = io.BytesIO(excel_obj['Body'].read())
                                                
                                                df = pd.read_excel(excel_buffer, sheet_name=0, nrows=None)
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzing {len(df)} rows in {object_name}...'}, namespace='/connectors')
                                                
                                                for col_name in df.columns:
                                                    col_series = df[col_name]
                                                    
                                                    col_type = 'STRING'
                                                    if pd.api.types.is_integer_dtype(col_series):
                                                        col_type = 'INTEGER'
                                                    elif pd.api.types.is_float_dtype(col_series):
                                                        col_type = 'FLOAT'
                                                    elif pd.api.types.is_bool_dtype(col_series):
                                                        col_type = 'BOOLEAN'
                                                    elif pd.api.types.is_datetime64_any_dtype(col_series):
                                                        col_type = 'TIMESTAMP'
                                                    
                                                    pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                                                    
                                                    columns.append({
                                                        "name": str(col_name),
                                                        "type": col_type,
                                                        "mode": "NULLABLE",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Analyzed {len(df)} rows, found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pandas/openpyxl not installed, skipping Excel column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Excel file: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'avro':
                                            try:
                                                import fastavro
                                                import io
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading Avro file schema: {object_name}...'}, namespace='/connectors')
                                                avro_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                avro_buffer = io.BytesIO(avro_obj['Body'].read())
                                                
                                                avro_file = fastavro.schemaless_reader(avro_buffer)
                                                schema = fastavro.schema.load_schema(avro_file)
                                                
                                                for field in schema.get('fields', []):
                                                    field_name = field.get('name', '')
                                                    field_type = str(field.get('type', 'string'))
                                                    
                                                    pii_detected, pii_type = detect_pii_in_column(field_name, field_type)
                                                    columns.append({
                                                        "name": field_name,
                                                        "type": field_type,
                                                        "mode": "NULLABLE",
                                                        "description": field.get('doc', ''),
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  fastavro not installed, skipping Avro column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read Avro schema: {str(e)}'}, namespace='/connectors')
                                        
                                        elif file_type == 'orc':
                                            try:
                                                import pyarrow.orc as orc
                                                import io
                                                
                                                emit('progress', {'type': 'progress', 'message': f'📖 Reading ORC file schema: {object_name}...'}, namespace='/connectors')
                                                orc_obj = bucket_s3_client.get_object(Bucket=bucket_name_actual, Key=key)
                                                orc_buffer = io.BytesIO(orc_obj['Body'].read())
                                                
                                                orc_file = orc.ORCFile(orc_buffer)
                                                schema = orc_file.schema
                                                
                                                for field in schema:
                                                    pii_detected, pii_type = detect_pii_in_column(field.name, str(field.type))
                                                    columns.append({
                                                        "name": field.name,
                                                        "type": str(field.type),
                                                        "mode": "NULLABLE" if field.nullable else "REQUIRED",
                                                        "description": "",
                                                        "pii_detected": pii_detected,
                                                        "pii_type": pii_type
                                                    })
                                                
                                                emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                            except ImportError:
                                                emit('progress', {'type': 'progress', 'message': f'  pyarrow not installed, skipping ORC column detection'}, namespace='/connectors')
                                            except Exception as e:
                                                emit('progress', {'type': 'progress', 'message': f'  Could not read ORC schema: {str(e)}'}, namespace='/connectors')
                                        
                                        if columns:
                                            emit('progress', {'type': 'progress', 'message': f' Found {len(columns)} columns in {object_name}'}, namespace='/connectors')
                                    
                                    except Exception as col_error:
                                        emit('progress', {'type': 'progress', 'message': f'  Could not extract columns from {object_name}: {str(col_error)}'}, namespace='/connectors')
                                        columns = []
                                
                                asset_id = f"s3://{bucket_name_actual}/{key}"
                                
                                asset = {
                                    "id": asset_id,
                                    "name": key.split('/')[-1] if '/' in key else key,
                                    "type": asset_type,
                                    "catalog": bucket_name_actual,
                                    "schema": '/'.join(key.split('/')[:-1]) if '/' in key else '',
                                    "discovered_at": datetime.now().isoformat(),
                                    "status": "active",
                                    "description": f"S3 object in bucket {bucket_name_actual}",
                                    "size_bytes": size,
                                    "columns": columns,
                                    "connector_id": connector_id,
                                    "technical_metadata": {
                                        "asset_id": asset_id,
                                        "asset_type": asset_type,
                                        "location": f"s3://{bucket_name_actual}/{key}",
                                        "format": file_format,
                                        "content_type": content_type,
                                        "file_extension": file_extension,
                                        "size_bytes": size,
                                        "storage_class": storage_class,
                                        "source_system": "Amazon S3",
                                        "bucket_name": bucket_name_actual,
                                        "object_key": key,
                                        "object_path": key,
                                        "region": bucket_region,
                                        "last_modified": last_modified.isoformat() if isinstance(last_modified, datetime) else str(last_modified),
                                        "etag": obj.get('ETag', '').strip('"') if 'ETag' in obj else ''
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
                                        "description": f"S3 object: {key}",
                                        "business_owner": "Unknown",
                                        "department": bucket_name_actual,
                                        "classification": "internal",
                                        "sensitivity_level": "low",
                                        "tags": list(metadata.keys()) if metadata else []
                                    }
                                }
                                
                                discovered_assets.append(asset)
                                assets_discovered += 1
                                bucket_objects += 1
                                
                                try:
                                    db_asset = {
                                        'id': asset.get('id'),
                                        'name': asset.get('name'),
                                        'type': asset.get('type'),
                                        'catalog': asset.get('catalog'),
                                        'schema': asset.get('schema'),
                                        'connector_id': asset.get('connector_id'),
                                        'discovered_at': asset.get('discovered_at'),
                                        'status': asset.get('status', 'active'),
                                        'extra_data': asset
                                    }
                                    db_helpers.save_asset(db_asset)
                                except Exception as save_err:
                                    if bucket_objects <= 5:
                                        print(f"  Failed to save asset immediately: {save_err}")
                                
                                format_display = file_format if file_format != 'Unknown' else (content_type if content_type else 'Unknown Format')
                                
                                emit('progress', {'type': 'progress', 'message': f' Discovered: {object_name} | Type: {asset_type} | Format: {format_display}'}, namespace='/connectors')
                                
                                if bucket_objects <= 50:
                                    time.sleep(0.1)
                                
                                if bucket_objects % 100 == 0:
                                    emit('progress', {'type': 'progress', 'message': f' Progress: {bucket_objects} objects discovered in {bucket_name_actual}...'}, namespace='/connectors')
                        
                        emit('progress', {'type': 'progress', 'message': f' Completed bucket {bucket_name_actual}: {bucket_objects} objects discovered'}, namespace='/connectors')
                        
                    except ClientError as e:
                        error_code = e.response.get('Error', {}).get('Code', '')
                        if error_code == 'AccessDenied':
                            emit('progress', {'type': 'progress', 'message': f'  Access denied to bucket {bucket_name_actual}, skipping...'}, namespace='/connectors')
                        else:
                            emit('progress', {'type': 'progress', 'message': f'  Error accessing bucket {bucket_name_actual}: {str(e)}'}, namespace='/connectors')
                        continue
                
                active_connectors.append({
                    "id": connector_id,
                    "name": connection_data.connection_name,
                    "type": "Amazon S3",
                    "status": "active",
                    "enabled": True,
                    "last_run": datetime.now().isoformat(),
                    "config": {
                        "access_key_id": connection_data.access_key_id,
                        "secret_access_key": connection_data.secret_access_key,
                        "region": region,
                        "bucket_name": connection_data.bucket_name
                    },
                    "assets_count": assets_discovered
                })
                
                save_connectors()
                
                try:
                    from api.s3 import trigger_airflow_dag
                    trigger_result = trigger_airflow_dag('s3_asset_discovery')
                    if trigger_result.get('success'):
                        emit('progress', {'type': 'progress', 'message': ' Airflow DAG triggered - asset discovery will start immediately!'}, namespace='/connectors')
                    else:
                        print(f"  Airflow DAG trigger failed (will run on schedule): {trigger_result.get('message')}")
                except Exception as e:
                    print(f"  Could not trigger Airflow DAG (will run on schedule): {e}")
                
                print(f" Discovery complete: {assets_discovered} S3 assets discovered")
                
                with app.app_context():
                    from database import Asset
                    saved_count = db.session.query(Asset).filter(Asset.connector_id == connector_id).count()
                    print(f" Verified: {saved_count} assets saved to database for connector {connector_id}")
                
                connector = next((c for c in active_connectors if c['id'] == connector_id), None)
                if connector:
                    connector['assets_count'] = assets_discovered
                    save_connectors()
                
                save_assets()
                
                emit('progress', {'type': 'progress', 'message': 'ℹ  Asset discovery will run via Airflow DAG (every 1 minute)'}, namespace='/connectors')
                
                emit('complete', {'type': 'complete', 'message': f' Successfully discovered {assets_discovered} S3 assets!', 'discovered_assets': assets_discovered, 'connector_id': connector_id}, namespace='/connectors')
                return
                
            except Exception as e:
                print(f"S3 connection failed: {str(e)}")
                emit('error', {'type': 'error', 'message': f'Connection failed: {str(e)}'}, namespace='/connectors')
                return
        
        elif connection_type == 'Service Account' and (selected_connector_id == 'gcs' or selected_connector_id == 'google-cloud-storage'):
            print(f" GCS CONNECTOR MATCHED! Processing GCS connection...")
            print(f"   connection_type='{connection_type}' (type: {type(connection_type)})")
            print(f"   selected_connector_id='{selected_connector_id}' (type: {type(selected_connector_id)})")
            print(f"   Condition check: connection_type == 'Service Account' = {connection_type == 'Service Account'}")
            print(f"   Condition check: selected_connector_id == 'gcs' = {selected_connector_id == 'gcs'}")
            try:
                from api.gcs import GCSConnectionTest, discover_gcs_assets
                
                print(f"   Config keys: {list(config.keys()) if config else 'None'}")
                print(f"   serviceAccount: {'***' if config.get('serviceAccount') else 'None'}")
                print(f"   projectId: {config.get('projectId')}")
                print(f"   bucketName: {config.get('bucketName')}")
                print(f"   name: {config.get('name')}")
                
                connection_data = GCSConnectionTest(
                    service_account_json=config.get('serviceAccount'),
                    project_id=config.get('projectId'),
                    bucket_name=config.get('bucketName'),
                    connection_name=config.get('name')
                )
                print(f" GCSConnectionTest created successfully")
            except Exception as e:
                print(f" Error creating GCSConnectionTest: {e}")
                import traceback
                traceback.print_exc()
                emit('error', {'type': 'error', 'message': f'Invalid GCS connection data: {str(e)}'}, namespace='/connectors')
                return
            
            try:
                emit('progress', {'type': 'progress', 'message': ' Authenticating with Google Cloud Storage...'}, namespace='/connectors')
                
                import json as json_module
                try:
                    service_account_info = json_module.loads(connection_data.service_account_json)
                    placeholder_values = ["your-project-id", "YOUR_PRIVATE_KEY", "your-service-account@your-project.iam.gserviceaccount.com"]
                    
                    service_account_str = str(service_account_info).lower()
                    if any(placeholder.lower() in service_account_str for placeholder in placeholder_values):
                        emit('error', {'type': 'error', 'message': ' Placeholder credentials detected in service account JSON. Please enter your actual Google Cloud service account credentials.'}, namespace='/connectors')
                        return
                except json_module.JSONDecodeError:
                    emit('error', {'type': 'error', 'message': ' Invalid JSON format in service account credentials.'}, namespace='/connectors')
                    return
                
                emit('progress', {'type': 'progress', 'message': f' Connecting to GCS project: {connection_data.project_id}...'}, namespace='/connectors')
                
                def progress_callback(message):
                    emit('progress', {'type': 'progress', 'message': message}, namespace='/connectors')
                
                assets = discover_gcs_assets(
                    connection_data.service_account_json,
                    connection_data.project_id,
                    connection_data.bucket_name,
                    progress_callback=progress_callback
                )
                
                emit('progress', {'type': 'progress', 'message': f' Authentication successful! Connected to Google Cloud Storage'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' Discovered {len(assets)} total assets from GCS'}, namespace='/connectors')
                
                connector_id = f"gcs_{connection_data.project_id}_{datetime.now().timestamp()}"
                
                emit('progress', {'type': 'progress', 'message': f' Saving {len(assets)} assets to database...'}, namespace='/connectors')
                saved_count = 0
                failed_count = 0
                
                for idx, asset in enumerate(assets):
                    asset['connector_id'] = connector_id
                    if 'discovered_at' not in asset or not asset.get('discovered_at'):
                        asset['discovered_at'] = datetime.now().isoformat()
                    if 'status' not in asset:
                        asset['status'] = 'active'
                    if 'name' not in asset or not asset.get('name'):
                        asset_id = asset.get('id', '')
                        if '/' in asset_id:
                            asset['name'] = asset_id.split('/')[-1]
                        else:
                            asset['name'] = asset_id
                    
                    db_asset = {
                        'id': asset.get('id'),
                        'name': asset.get('name', 'Unknown'),
                        'type': asset.get('type', 'File'),
                        'catalog': asset.get('catalog', ''),
                        'schema': asset.get('schema', ''),
                        'connector_id': connector_id,
                        'discovered_at': asset.get('discovered_at', datetime.now().isoformat()),
                        'status': asset.get('status', 'active'),
                        'extra_data': asset
                    }
                    
                    try:
                        if idx == 0:
                            print(f"   ID: {db_asset.get('id')}")
                            print(f"   Name: {db_asset.get('name')}")
                            print(f"   Type: {db_asset.get('type')}")
                            print(f"   Catalog: {db_asset.get('catalog')}")
                            print(f"   Connector ID: {db_asset.get('connector_id')}")
                            print(f"   Has extra_data: {'extra_data' in db_asset}")
                        
                        if db_helpers.save_asset(db_asset):
                            saved_count += 1
                            if idx < 3:
                                print(f" Saved asset {idx+1}: {db_asset.get('id', 'Unknown')}")
                        else:
                            failed_count += 1
                            print(f"  save_asset returned False for: {db_asset.get('id', 'Unknown')}")
                            if failed_count <= 3:
                                print(f"   Asset data: {db_asset}")
                    except Exception as save_error:
                        failed_count += 1
                        print(f" Exception saving asset {db_asset.get('id', 'Unknown')}: {save_error}")
                        import traceback
                        if failed_count <= 3:
                            traceback.print_exc()
                    
                    discovered_assets.append(asset)
                    
                    if (idx + 1) % 10 == 0:
                        emit('progress', {'type': 'progress', 'message': f'   Saved {idx + 1}/{len(assets)} assets...'}, namespace='/connectors')
                
                emit('progress', {'type': 'progress', 'message': f' Saved {saved_count} assets to database' + (f' ({failed_count} failed)' if failed_count > 0 else '')}, namespace='/connectors')
                
                connector_config = {
                    "service_account_json": connection_data.service_account_json,
                    "project_id": connection_data.project_id,
                    "bucket_name": connection_data.bucket_name
                }
                
                if connection_data.bucket_name:
                    bucket_names = [connection_data.bucket_name]
                else:
                    bucket_names = list(set(asset.get('catalog') for asset in assets if asset.get('catalog')))
                
                active_connectors.append({
                    "id": connector_id,
                    "name": connection_data.connection_name,
                    "type": "Google Cloud Storage",
                    "status": "active",
                    "enabled": True,
                    "last_run": datetime.now().isoformat(),
                    "config": connector_config,
                    "assets_count": len(assets)
                })
                
                save_connectors()
                save_assets()
                
                try:
                    emit('progress', {'type': 'progress', 'message': ' Setting up real-time event monitoring...'}, namespace='/connectors')
                    
                    from api.gcs import setup_gcs_event_notifications
                    
                    event_result = setup_gcs_event_notifications(
                        connection_data.service_account_json,
                        connection_data.project_id,
                        bucket_names if bucket_names else []
                    )
                    
                    if event_result['success']:
                        connector = next((c for c in active_connectors if c['id'] == connector_id), None)
                        if connector:
                            connector['config']['pubsub_topic_name'] = event_result.get('pubsub_topic_name')
                            connector['config']['pubsub_topic_path'] = event_result.get('pubsub_topic_path')
                            connector['config']['subscription_name'] = event_result.get('subscription_name')
                            connector['config']['subscription_path'] = event_result.get('subscription_path')
                            connector['config']['webhook_url'] = event_result.get('webhook_url')
                            connector['config']['configured_buckets'] = event_result.get('configured_buckets', [])
                            save_connectors()
                        
                        emit('progress', {'type': 'progress', 'message': f' Real-time monitoring active via Pub/Sub! Listening for changes in {len(event_result["configured_buckets"])} bucket(s)...'}, namespace='/connectors')
                    else:
                        error_msg = event_result.get("error", "Unknown error")
                        if "ngrok" in error_msg.lower():
                            emit('progress', {'type': 'progress', 'message': f'  Event monitoring setup skipped: {error_msg}. You can set it up later via the API.'}, namespace='/connectors')
                        else:
                            emit('progress', {'type': 'progress', 'message': f'  Could not set up event monitoring: {error_msg}'}, namespace='/connectors')
                except Exception as e:
                    print(f"  Error setting up GCS event notifications: {e}")
                    import traceback
                    traceback.print_exc()
                    emit('progress', {'type': 'progress', 'message': '  Real-time monitoring setup failed, but connector is active'}, namespace='/connectors')
                
                emit('complete', {'type': 'complete', 'message': f' Successfully discovered {len(assets)} GCS assets!', 'discovered_assets': len(assets), 'connector_id': connector_id}, namespace='/connectors')
                return
                
            except Exception as e:
                print(f"GCS connection failed: {str(e)}")
                emit('error', {'type': 'error', 'message': f'Connection failed: {str(e)}'}, namespace='/connectors')
                return
        
        elif (connection_type == 'Account Key' or connection_type == 'Connection String') and selected_connector_id == 'azure-data-storage':
            try:
                from api.azure_unified import AzureUnifiedConnectionTest, discover_azure_all_assets
                
                connection_data = AzureUnifiedConnectionTest(
                    account_name=config.get('accountName'),
                    account_key=config.get('accountKey'),
                    connection_string=config.get('connectionString'),
                    container_name=config.get('containerName'),
                    share_name=config.get('shareName'),
                    connection_name=config.get('name')
                )
            except Exception as e:
                emit('error', {'type': 'error', 'message': f'Invalid Azure Data Storage connection data: {str(e)}'}, namespace='/connectors')
                return
            
            try:
                emit('progress', {'type': 'progress', 'message': ' 🔐 Authenticating with Azure Storage Account...'}, namespace='/connectors')
                
                result = discover_azure_all_assets(
                    connection_data.account_name,
                    connection_data.account_key,
                    connection_data.connection_string,
                    connection_data.container_name,
                    connection_data.share_name
                )
                
                assets = result.get('all_assets', [])
                blob_containers = result.get('blob_containers', [])
                file_shares = result.get('file_shares', [])
                tables = result.get('tables', [])
                queues = result.get('queues', [])
                summary = result.get('summary', {})
                
                emit('progress', {'type': 'progress', 'message': f' ✅ Authentication successful!'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' 📦 Blob Storage: {len(blob_containers)} container(s), {summary.get("blob_assets", 0)} asset(s)'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' 📁 Azure Files: {len(file_shares)} file share(s), {summary.get("file_assets", 0)} asset(s)'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' 📊 Azure Tables: {len(tables)} table(s)'}, namespace='/connectors')
                emit('progress', {'type': 'progress', 'message': f' 📬 Azure Queues: {len(queues)} queue(s)'}, namespace='/connectors')
                
                # Show blob containers
                for container in blob_containers[:5]:
                    emit('progress', {'type': 'progress', 'message': f'   📦 Container: {container["name"]} ({container["asset_count"]} asset(s))'}, namespace='/connectors')
                if len(blob_containers) > 5:
                    emit('progress', {'type': 'progress', 'message': f'   ... and {len(blob_containers) - 5} more container(s)'}, namespace='/connectors')
                
                # Show file shares
                for share in file_shares[:5]:
                    emit('progress', {'type': 'progress', 'message': f'   📁 File Share: {share["name"]} ({share["asset_count"]} asset(s))'}, namespace='/connectors')
                if len(file_shares) > 5:
                    emit('progress', {'type': 'progress', 'message': f'   ... and {len(file_shares) - 5} more file share(s)'}, namespace='/connectors')
                
                # Show tables
                for table_name in tables[:10]:
                    emit('progress', {'type': 'progress', 'message': f'   📊 Table: {table_name}'}, namespace='/connectors')
                if len(tables) > 10:
                    emit('progress', {'type': 'progress', 'message': f'   ... and {len(tables) - 10} more table(s)'}, namespace='/connectors')
                
                # Show queues
                for queue_name in queues[:10]:
                    emit('progress', {'type': 'progress', 'message': f'   📬 Queue: {queue_name}'}, namespace='/connectors')
                if len(queues) > 10:
                    emit('progress', {'type': 'progress', 'message': f'   ... and {len(queues) - 10} more queue(s)'}, namespace='/connectors')
                
                emit('progress', {'type': 'progress', 'message': f' ✅ Discovered {len(assets)} total assets across all Azure storage types'}, namespace='/connectors')
                
                connector_id = f"azure_data_storage_{connection_data.account_name}_{datetime.now().timestamp()}"
                
                for asset in assets:
                    asset['connector_id'] = connector_id
                    asset['discovered_at'] = datetime.now().isoformat()
                    db_helpers.save_asset(asset)
                    discovered_assets.append(asset)
                
                active_connectors.append({
                    "id": connector_id,
                    "name": connection_data.connection_name,
                    "type": "Azure Data Storage",
                    "status": "active",
                    "enabled": True,
                    "last_run": datetime.now().isoformat(),
                    "config": {
                        "account_name": connection_data.account_name,
                        "account_key": connection_data.account_key,
                        "connection_string": connection_data.connection_string,
                        "container_name": connection_data.container_name,
                        "share_name": connection_data.share_name,
                        "rediscovery_interval_minutes": config.get("rediscovery_interval_minutes", 5)
                    },
                    "assets_count": len(assets)
                })
                
                save_connectors()
                save_assets()
                
                emit('complete', {
                    'type': 'complete',
                    'message': f' Successfully discovered {len(assets)} Azure assets! (Blob: {summary.get("blob_assets", 0)}, Files: {summary.get("file_assets", 0)}, Tables: {len(tables)}, Queues: {len(queues)})',
                    'discovered_assets': len(assets),
                    'connector_id': connector_id,
                    'blob_containers': blob_containers,
                    'file_shares': file_shares,
                    'tables': tables,
                    'queues': queues,
                    'account_name': result.get('account_name'),
                    'summary': summary
                }, namespace='/connectors')
                return
                
            except Exception as e:
                print(f"Azure Data Storage connection failed: {str(e)}")
                import traceback
                traceback.print_exc()
                emit('error', {'type': 'error', 'message': f'Connection failed: {str(e)}'}, namespace='/connectors')
                return
        
        else:
            print(f" NO MATCHING CONNECTOR HANDLER FOUND!")
            print(f"   Received connection_type: '{connection_type}' (type: {type(connection_type)}, repr: {repr(connection_type)})")
            print(f"   Received selected_connector_id: '{selected_connector_id}' (type: {type(selected_connector_id)}, repr: {repr(selected_connector_id)})")
            print(f"   Raw data received: {data}")
            print(f"   Available handlers:")
            print(f"     - BigQuery: connection_type == 'Service Account' ({connection_type == 'Service Account'}) AND selected_connector_id == 'bigquery' ({selected_connector_id == 'bigquery'})")
            print(f"     - GCS: connection_type == 'Service Account' ({connection_type == 'Service Account'}) AND selected_connector_id == 'gcs' ({selected_connector_id == 'gcs'})")
            print(f"     - Starburst: connection_type == 'API Token' ({connection_type == 'API Token'}) AND selected_connector_id == 'starburst' ({selected_connector_id == 'starburst'})")
            print(f"     - S3: connection_type == 'Access Key' ({connection_type == 'Access Key'}) AND selected_connector_id == 's3' ({selected_connector_id == 's3'})")
            print(f"     - Azure Blob: connection_type in ['Account Key', 'Connection String'] ({connection_type in ['Account Key', 'Connection String']}) AND selected_connector_id == 'azure-blob' ({selected_connector_id == 'azure-blob'})")
            print(f"   Checking GCS condition explicitly:")
            print(f"     connection_type == 'Service Account': {connection_type == 'Service Account'}")
            print(f"     selected_connector_id == 'gcs': {selected_connector_id == 'gcs'}")
            print(f"     Both conditions: {connection_type == 'Service Account' and selected_connector_id == 'gcs'}")
            emit('error', {'type': 'error', 'message': f'Unknown connector type or invalid connection request. Received: connection_type="{connection_type}", connector_id="{selected_connector_id}"'}, namespace='/connectors')
            return
            
    app.register_blueprint(bigquery_bp, url_prefix='/api/bigquery')
    app.register_blueprint(starburst_bp, url_prefix='/api/starburst')
    app.register_blueprint(lineage_bp, url_prefix='/api')
    app.register_blueprint(s3_bp, url_prefix='/api/s3')
    app.register_blueprint(gcs_bp, url_prefix='/api/gcs')
    app.register_blueprint(azure_blob_bp, url_prefix='/api/azure-blob')
    from api.azure_unified import azure_unified_bp
    app.register_blueprint(azure_unified_bp, url_prefix='/api/azure-data-storage')

    @app.route("/api/dashboard/stats", methods=["GET"])
    def get_dashboard_stats():
        nonlocal active_connectors, discovered_assets
        try:
            connectors_list = load_connectors()
            assets_list = load_assets()
            
            if connectors_list:
                active_connectors = connectors_list
            if assets_list:
                discovered_assets = assets_list
            
            enabled_connectors = [c for c in active_connectors if c.get("enabled")]
            active_connector_ids = set(conn['id'] for conn in enabled_connectors)
            
            filtered_assets = []
            catalogs = set()
            
            for asset in discovered_assets:
                asset_connector_id = asset.get('connector_id', '')
                
                if asset_connector_id in active_connector_ids:
                    filtered_assets.append(asset)
                    catalogs.add(asset.get("catalog", "Unknown"))
                elif asset_connector_id.startswith('s3_') and any(conn.get('type') == 'Amazon S3' and conn.get('enabled') for conn in active_connectors):
                    filtered_assets.append(asset)
                    catalogs.add(asset.get("catalog", "Unknown"))
                elif asset_connector_id.startswith('starburst_') and any(conn.get('type') == 'Starburst Galaxy' and conn.get('enabled') for conn in active_connectors):
                    filtered_assets.append(asset)
                    catalogs.add(asset.get("catalog", "Unknown"))
                elif asset_connector_id.startswith('bq_') and any(conn.get('type') == 'BigQuery' and conn.get('enabled') for conn in active_connectors):
                    filtered_assets.append(asset)
                    catalogs.add(asset.get("catalog", "Unknown"))
            
            last_scan = None
            if enabled_connectors:
                try:
                    last_runs = [datetime.fromisoformat(c["last_run"]) for c in enabled_connectors if c.get("last_run")]
                    if last_runs:
                        last_scan = max(last_runs)
                except (ValueError, KeyError, TypeError) as e:
                    print(f"Warning: Error parsing last_run dates: {e}")
                    last_scan = None
            
            monitoring_status = "Active" if enabled_connectors else "Disabled"
            
            stats = DashboardStats(
                total_assets=len(filtered_assets),
                total_catalogs=len(catalogs),
                active_connectors=len(enabled_connectors),
                last_scan=last_scan,
                monitoring_status=monitoring_status
            )
            
            print(f" Dashboard stats - assets: {len(filtered_assets)}, connectors: {len(enabled_connectors)}, catalogs: {len(catalogs)}")
            
            result = stats.model_dump()
            return jsonify(result)
        except Exception as e:
            print(f" Error in get_dashboard_stats: {str(e)}")
            import traceback
            traceback.print_exc()
            try:
                return jsonify(DashboardStats(
                    total_assets=0,
                    total_catalogs=0,
                    active_connectors=0,
                    last_scan=None,
                    monitoring_status="Error"
                ).model_dump())
            except Exception as e2:
                print(f" Error creating error response: {e2}")
                return jsonify({
                    "total_assets": 0,
                    "total_catalogs": 0,
                    "active_connectors": 0,
                    "last_scan": None,
                    "monitoring_status": "Error"
                }), 500

    @app.route("/api/system/health", methods=["GET"])
    def get_system_health():
        nonlocal active_connectors
        enabled_connectors = len([c for c in active_connectors if c["enabled"]])
        last_scan = None
        if active_connectors:
            last_scan = max([datetime.fromisoformat(c["last_run"]) for c in active_connectors])
        
        return jsonify(SystemHealth(
            status="healthy",
            monitoring_enabled=len(active_connectors) > 0,
            connectors_enabled=enabled_connectors,
            connectors_total=len(active_connectors),
            last_scan=last_scan
        ).model_dump())

    @app.route("/api/catalog/completeness", methods=["GET"])
    def get_catalog_completeness():
        nonlocal active_connectors, discovered_assets
        try:
            active_connector_ids = set(conn['id'] for conn in active_connectors)
            filtered_assets = [a for a in discovered_assets if a.get('connector_id', '') in active_connector_ids]
            
            entity_types = {
                'data_sources': {'types': ['Connector'], 'field': 'connector_id'},
                'schemas': {'types': ['Table', 'View'], 'field': 'schema'},
                'tables': {'types': ['Table', 'BASE TABLE'], 'field': 'type'},
                'reports': {'types': ['View'], 'field': 'type'},
                'queries': {'types': ['View'], 'field': 'type'},
                'articles': {'types': ['Table', 'View'], 'field': 'type'}
            }
            
            completeness_metrics = {}
            
            for entity_name, entity_config in entity_types.items():
                relevant_assets = []
                if entity_name == 'data_sources':
                    relevant_assets = [{'id': c['id'], 'name': c['name'], 'type': c.get('type', 'Unknown')} for c in active_connectors]
                else:
                    relevant_assets = [
                        a for a in filtered_assets 
                        if a.get('type', '').upper() in [t.upper() for t in entity_config['types']]
                    ]
                
                total_count = len(relevant_assets)
                if total_count == 0:
                    completeness_metrics[entity_name] = {
                        'total': 0,
                        'documented': 0,
                        'completeness_percentage': 0.0,
                        'missing_fields': [],
                        'missing_count': 0
                    }
                    continue
                
                documented_count = 0
                missing_fields_summary = {
                    'description': 0,
                    'owner': 0,
                    'classification': 0,
                    'column_descriptions': 0,
                    'tags': 0
                }
                
                for asset in relevant_assets:
                    is_documented = True
                    doc_fields = []
                    
                    has_description = bool(
                        asset.get('description') or 
                        asset.get('business_metadata', {}).get('description') or
                        asset.get('extra_data', {}).get('description')
                    )
                    if not has_description:
                        is_documented = False
                        missing_fields_summary['description'] += 1
                        doc_fields.append('description')
                    
                    has_owner = bool(
                        asset.get('owner') or
                        asset.get('business_metadata', {}).get('business_owner') or
                        asset.get('operational_metadata', {}).get('owner')
                    )
                    if not has_owner:
                        missing_fields_summary['owner'] += 1
                        doc_fields.append('owner')
                    
                    has_classification = bool(
                        asset.get('classification') or
                        asset.get('business_metadata', {}).get('classification')
                    )
                    if not has_classification:
                        missing_fields_summary['classification'] += 1
                        doc_fields.append('classification')
                    
                    columns = asset.get('columns', []) or asset.get('extra_data', {}).get('columns', [])
                    if columns:
                        columns_with_desc = sum(1 for col in columns if col.get('description') and col.get('description').strip() and col.get('description') != '-')
                        if columns_with_desc == 0:
                            missing_fields_summary['column_descriptions'] += 1
                        column_completeness = (columns_with_desc / len(columns)) * 100 if columns else 0
                        if column_completeness < 50:
                            doc_fields.append('column_descriptions')
                    
                    has_tags = bool(
                        asset.get('tags') or
                        asset.get('business_metadata', {}).get('tags')
                    )
                    if not has_tags:
                        missing_fields_summary['tags'] += 1
                        doc_fields.append('tags')
                    
                    if has_description and (has_owner or has_classification or has_tags):
                        documented_count += 1
                
                completeness_pct = (documented_count / total_count * 100) if total_count > 0 else 0.0
                
                completeness_metrics[entity_name] = {
                    'total': total_count,
                    'documented': documented_count,
                    'completeness_percentage': round(completeness_pct, 2),
                    'missing_fields': missing_fields_summary,
                    'missing_count': total_count - documented_count
                }
            
            total_entities = sum(m['total'] for m in completeness_metrics.values())
            total_documented = sum(m['documented'] for m in completeness_metrics.values())
            overall_completeness = (total_documented / total_entities * 100) if total_entities > 0 else 0.0
            
            return jsonify({
                'overall_completeness': round(overall_completeness, 2),
                'total_entities': total_entities,
                'total_documented': total_documented,
                'by_entity_type': completeness_metrics,
                'last_updated': datetime.now().isoformat()
            })
        except Exception as e:
            print(f"Error calculating catalog completeness: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'overall_completeness': 0.0,
                'total_entities': 0,
                'total_documented': 0,
                'by_entity_type': {},
                'error': str(e)
            }), 500

    @app.route("/api/assets", methods=["GET"])
    def get_assets():
        try:
            nonlocal active_connectors, discovered_assets
            import math
            
            with app.app_context():
                active_connectors = load_connectors()
                discovered_assets = load_assets()
                app.config['discovered_assets'] = discovered_assets
                app.config['active_connectors'] = active_connectors
        except Exception as e:
            print(f" Error in get_assets (loading data): {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Error loading assets: {str(e)}'}), 500
        
        page = int(request.args.get("page", 0))
        size = int(request.args.get("size", 50))
        search = request.args.get("search")
        catalog = request.args.get("catalog")
        asset_type = request.args.get("asset_type")
        
        enabled_connectors = [c for c in active_connectors if c.get("enabled", True)]
        active_connector_ids = set(conn['id'] for conn in enabled_connectors)
        filtered_assets = []
        
        
        enabled_s3_connectors = any(conn.get('type') == 'Amazon S3' and conn.get('enabled', True) for conn in enabled_connectors)
        enabled_starburst_connectors = any(conn.get('type') == 'Starburst Galaxy' and conn.get('enabled', True) for conn in enabled_connectors)
        enabled_bigquery_connectors = any(conn.get('type') == 'BigQuery' and conn.get('enabled', True) for conn in enabled_connectors)
        
        
        starburst_asset_ids = [a.get('connector_id', '') for a in discovered_assets if a.get('connector_id', '').startswith('starburst_')]
        
        for asset in discovered_assets:
            asset_connector_id = asset.get('connector_id', '')
            
            if asset_connector_id in active_connector_ids:
                filtered_assets.append(asset)
            elif asset_connector_id.startswith('s3_') and enabled_s3_connectors:
                filtered_assets.append(asset)
            elif asset_connector_id.startswith('starburst_') and enabled_starburst_connectors:
                filtered_assets.append(asset)
            elif asset_connector_id.startswith('bq_') and enabled_bigquery_connectors:
                filtered_assets.append(asset)
            elif 'test' in asset_connector_id.lower() and enabled_s3_connectors:
                filtered_assets.append(asset)
        
        starburst_count = len([a for a in filtered_assets if a.get('connector_id', '').startswith('starburst_')])
        
        if search:
            search_lower = search.lower()
            filtered_assets = [
                asset for asset in filtered_assets
                if (search_lower in asset.get('name', '').lower() or
                    search_lower in asset.get('type', '').lower() or
                    search_lower in asset.get('catalog', '').lower())
            ]
        
        if catalog:
            filtered_assets = [
                asset for asset in filtered_assets
                if asset.get('catalog') == catalog
            ]
        
        if asset_type:
            filtered_assets = [
                asset for asset in filtered_assets
                if asset.get('type') == asset_type
            ]
        
        def get_sort_key(asset):
            sort_order = asset.get('sort_order')
            if sort_order is not None:
                try:
                    sort_order_int = int(sort_order)
                    return (1, sort_order_int)
                except (ValueError, TypeError):
                    pass
            
            discovered = get_discovered_at(asset)
            if isinstance(discovered, datetime):
                timestamp_ms = int(discovered.timestamp() * 1000)
                return (0, timestamp_ms)
            
            return (-1, 0)
        
        def get_discovered_at(asset):
            discovered = asset.get('discovered_at')
            if discovered is None:
                return datetime.min
            
            if isinstance(discovered, datetime):
                return discovered
            
            if isinstance(discovered, str):
                try:
                    if 'Z' in discovered or '+' in discovered:
                        cleaned = discovered.replace('Z', '+00:00')
                        if '+' not in cleaned and '-' in cleaned and 'T' in cleaned:
                            cleaned = discovered + '+00:00'
                        try:
                            return datetime.fromisoformat(cleaned)
                        except:
                            cleaned_no_micro = cleaned.split('.')[0] + '+00:00' if '+' in cleaned else cleaned.split('.')[0]
                            return datetime.fromisoformat(cleaned_no_micro)
                    
                    cleaned = discovered.split('.')[0] if '.' in discovered else discovered
                    cleaned = cleaned.replace('Z', '').replace('+00:00', '').strip()
                    
                    if 'T' in cleaned:
                        try:
                            if '.' in cleaned:
                                return datetime.strptime(cleaned, '%Y-%m-%dT%H:%M:%S.%f')
                            else:
                                return datetime.strptime(cleaned, '%Y-%m-%dT%H:%M:%S')
                        except:
                            try:
                                return datetime.fromisoformat(cleaned)
                            except:
                                cleaned_no_micro = cleaned.split('.')[0]
                                return datetime.strptime(cleaned_no_micro, '%Y-%m-%dT%H:%M:%S')
                    elif ' ' in cleaned:
                        try:
                            if '.' in cleaned:
                                return datetime.strptime(cleaned, '%Y-%m-%d %H:%M:%S.%f')
                            else:
                                return datetime.strptime(cleaned, '%Y-%m-%d %H:%M:%S')
                        except:
                            cleaned_no_micro = cleaned.split('.')[0]
                            return datetime.strptime(cleaned_no_micro, '%Y-%m-%d %H:%M:%S')
                    else:
                        return datetime.strptime(cleaned, '%Y-%m-%d')
                except Exception as e:
                    print(f"  Error parsing discovered_at '{discovered}': {e}")
                    return datetime.min
            
            return datetime.min
        
        try:
            filtered_assets.sort(key=get_sort_key, reverse=True)
            
            test_assets_at_top = [a for a in filtered_assets[:10] if 'TEST_FILE' in a.get('name', '')]
            
            if len(filtered_assets) > 0:
                for i, asset in enumerate(filtered_assets[:5], 1):
                    name = asset.get('name', 'Unknown')[:50]
                    discovered = asset.get('discovered_at', 'No date')
                    print(f"   {i}. {name} - Discovered: {discovered}")
        except Exception as sort_error:
            print(f"  Error sorting assets: {sort_error}")
            import traceback
            traceback.print_exc()
            print("  Continuing without sorting...")
        
        total_assets = len(filtered_assets)
        total_pages = math.ceil(total_assets / size) if total_assets > 0 else 0
        start_idx = page * size
        end_idx = start_idx + size
        paginated_assets = filtered_assets[start_idx:end_idx]
        
        try:
            return jsonify({
                "assets": paginated_assets,
                "pagination": {
                    "page": page,
                    "size": size,
                    "total": total_assets,
                    "total_pages": total_pages,
                    "has_next": page < total_pages - 1,
                    "has_prev": page > 0
                }
            })
        except Exception as e:
            print(f" Error in get_assets (returning response): {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Error serializing response: {str(e)}'}), 500

    def detect_pii_in_column(column_name: str, column_type: str) -> tuple[bool, Optional[str]]:
        column_name_lower = column_name.lower()
        
        if any(pattern in column_name_lower for pattern in ['email', 'e_mail', 'mail']):
            return True, "EMAIL"
        
        if any(pattern in column_name_lower for pattern in ['first_name', 'firstname', 'last_name', 'lastname', 
                                                              'full_name', 'fullname', 'name', 'customer_name']):
            return True, "NAME"
        
        if any(pattern in column_name_lower for pattern in ['phone', 'mobile', 'cell', 'telephone', 'contact_number']):
            return True, "PHONE"
        
        if any(pattern in column_name_lower for pattern in ['address', 'street', 'city', 'zipcode', 'zip_code', 'postal']):
            return True, "ADDRESS"
        
        if any(pattern in column_name_lower for pattern in ['ssn', 'social_security', 'national_id', 'passport', 'license']):
            return True, "SENSITIVE_ID"
        
        if any(pattern in column_name_lower for pattern in ['credit_card', 'card_number', 'ccn', 'payment_card']):
            return True, "CREDIT_CARD"
        
        if any(pattern in column_name_lower for pattern in ['birth_date', 'birthdate', 'dob', 'date_of_birth']):
            return True, "DATE_OF_BIRTH"
        
        if any(pattern in column_name_lower for pattern in ['account_number', 'account_no', 'bank_account']):
            return True, "ACCOUNT_NUMBER"
        
        return False, None

    @app.route("/api/assets/<path:asset_id>", methods=["GET", "PUT", "PATCH"])
    def get_asset_detail(asset_id: str):
        nonlocal active_connectors, discovered_assets
        
        if request.method in ["PUT", "PATCH"]:
            try:
                data = request.get_json()
                if not data:
                    return jsonify({"error": "No data provided"}), 400
                
                with app.app_context():
                    discovered_assets = load_assets()
                
                asset = None
                for a in discovered_assets:
                    if a.get("id") == asset_id:
                        asset = a
                        break
                
                if not asset:
                    return jsonify({"error": "Asset not found"}), 404
                
                extra_data = asset.get('extra_data', {})
                if isinstance(extra_data, str):
                    import json
                    extra_data = json.loads(extra_data)
                
                if 'business_metadata' in data or 'classification' in data or 'sensitivity_level' in data:
                    if 'business_metadata' not in extra_data:
                        extra_data['business_metadata'] = {}
                    
                    business_meta = extra_data['business_metadata']
                    
                    if 'classification' in data:
                        business_meta['classification'] = data['classification']
                    elif 'business_metadata' in data and 'classification' in data['business_metadata']:
                        business_meta['classification'] = data['business_metadata']['classification']
                    
                    if 'sensitivity_level' in data:
                        business_meta['sensitivity_level'] = data['sensitivity_level']
                    elif 'business_metadata' in data and 'sensitivity_level' in data['business_metadata']:
                        business_meta['sensitivity_level'] = data['business_metadata']['sensitivity_level']
                    
                    if 'business_metadata' in data:
                        business_meta.update(data['business_metadata'])
                
                db_asset = {
                    'id': asset.get('id'),
                    'name': asset.get('name', 'Unknown'),
                    'type': asset.get('type', 'Table'),
                    'catalog': asset.get('catalog'),
                    'schema': asset.get('schema'),
                    'connector_id': asset.get('connector_id'),
                    'discovered_at': asset.get('discovered_at'),
                    'status': asset.get('status', 'active'),
                    'extra_data': extra_data
                }
                
                if db_helpers.save_asset(db_asset):
                    with app.app_context():
                        discovered_assets = load_assets()
                    
                    updated_asset = None
                    for a in discovered_assets:
                        if a.get("id") == asset_id:
                            updated_asset = a
                            break
                    
                    return jsonify({
                        "success": True,
                        "message": "Asset metadata updated successfully",
                        "asset": updated_asset
                    }), 200
                else:
                    return jsonify({"error": "Failed to update asset"}), 500
                    
            except Exception as e:
                print(f"Error updating asset metadata: {e}")
                import traceback
                traceback.print_exc()
                return jsonify({"error": str(e)}), 500
        
        active_connector_ids = set(conn['id'] for conn in active_connectors)
        
        asset = None
        for a in discovered_assets:
            if a["id"] == asset_id:
                asset_connector_id = a.get("connector_id", "")
                if asset_connector_id in active_connector_ids:
                    asset = a
                    break
                elif asset_connector_id.startswith('s3_') and any(conn.get('type') == 'Amazon S3' for conn in active_connectors):
                    asset = a
                    break
                elif asset_connector_id.startswith('starburst_') and any(conn.get('type') == 'Starburst Galaxy' for conn in active_connectors):
                    asset = a
                    break
                elif asset_connector_id.startswith('bq_') and any(conn.get('type') == 'BigQuery' for conn in active_connectors):
                    asset = a
                    break
        
        if not asset:
            abort(404, "Asset not found")
        
        detailed_asset = asset.copy()
        
        if "columns" not in detailed_asset:
            if asset.get("columns"):
                detailed_asset["columns"] = asset.get("columns", [])
            elif isinstance(asset.get("extra_data"), dict) and asset.get("extra_data", {}).get("columns"):
                detailed_asset["columns"] = asset.get("extra_data", {}).get("columns", [])
        
        if "technical_metadata" not in detailed_asset:
            detailed_asset["technical_metadata"] = {
                "asset_id": asset["id"],
                "asset_type": asset["type"],
                "created_at": asset.get("discovered_at"),
                "location": asset.get("location", "N/A"),
                "size_bytes": asset.get("size_bytes", 0),
                "num_rows": asset.get("num_rows", 0),
                "format": "BigQuery Native" if asset.get("connector_id", "").startswith("bq_") else "Starburst" if asset.get("connector_id", "").startswith("starburst_") else "N/A"
            }
        
        if "operational_metadata" not in detailed_asset:
            detailed_asset["operational_metadata"] = {
                "last_modified": asset.get("discovered_at", datetime.now().isoformat()),
                "last_accessed": datetime.now().isoformat(),
                "access_count": "N/A",
                "owner": "Unknown",
                "status": asset.get("status", "active"),
                "data_quality_score": 90
            }
        
        if "business_metadata" not in detailed_asset:
            detailed_asset["business_metadata"] = {
                "description": asset.get("description", "No description available"),
                "business_owner": "Unknown",
                "department": "N/A",
                "classification": "internal",
                "tags": [],
                "sensitivity_level": "medium"
            }
        
        columns = []
        asset_type = (asset.get("type") or "").upper()
        stored_columns = detailed_asset.get("columns", []) or asset.get("columns", [])
        
        if stored_columns and len(stored_columns) > 0:
            for col in stored_columns:
                if not isinstance(col, dict):
                    continue
                    
                col_name = col.get("name", "")
                col_type = col.get("type", "STRING")
                col_mode = col.get("mode", "NULLABLE")
                col_nullable = col.get("nullable")
                if col_nullable is None:
                    col_nullable = col_mode in ["NULLABLE", "REPEATED"] if col_mode else True
                
                pii_detected = col.get("pii_detected", False)
                pii_type = col.get("pii_type")
                
                if not pii_detected:
                    pii_detected, pii_type = detect_pii_in_column(col_name, col_type)
                
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "nullable": col_nullable,
                    "description": col.get("description", ""),
                    "pii_detected": pii_detected,
                    "pii_type": pii_type,
                    "tags": col.get("tags", [])
                })
        elif asset_type in ["TABLE", "VIEW", "BASE TABLE", "DATA FILE", "FILE"]:
            connector_id = asset.get("connector_id")
            if connector_id and connector_id.startswith("starburst_"):
                table_full_id = asset["id"]
                column_assets = [a for a in discovered_assets if a.get("type") == "Column" and a.get("catalog", "").startswith(table_full_id)]
                
                for col_asset in column_assets:
                    col_name = col_asset["name"]
                    pii_detected, pii_type = detect_pii_in_column(col_name, "STRING")
                    columns.append({
                        "name": col_name,
                        "type": "STRING",
                        "nullable": True,
                        "description": col_asset.get("description", ""),
                        "pii_detected": pii_detected,
                        "pii_type": pii_type
                    })
        
        detailed_asset["columns"] = columns
        
        connector_id = asset.get("connector_id", "")
        pipeline_stage = asset.get("technical_metadata", {}).get("pipeline_stage")
        is_pipeline_asset = pipeline_stage is not None
        
        if is_pipeline_asset:
            asset_name_lower = asset.get("name", "").lower()
            catalog_lower = asset.get("catalog", "").lower()
            
            pipeline_type = "ETL"
            stage = None
            
            if "raw" in asset_name_lower or "staging" in catalog_lower:
                stage = "extract"
                pipeline_type = "ETL"
            elif "warehouse" in catalog_lower or "dim" in asset_name_lower or "fact" in asset_name_lower:
                stage = "transform"
                pipeline_type = "ETL"
            elif "analytics" in catalog_lower or "report" in catalog_lower:
                stage = "load"
                pipeline_type = "ETL"
            
            upstream_assets = []
            downstream_assets = []
            
            for other_asset in discovered_assets:
                other_connector = other_asset.get("connector_id", "")
                if other_connector == connector_id:
                    other_name_lower = other_asset.get("name", "").lower()
                    other_catalog_lower = other_asset.get("catalog", "").lower()
                    
                    if stage == "extract":
                        if "warehouse" in other_catalog_lower:
                            downstream_assets.append({
                                "id": other_asset.get("id"),
                                "name": other_asset.get("name"),
                                "stage": "transform",
                                "type": other_asset.get("type")
                            })
                    elif stage == "transform":
                        if "staging" in other_catalog_lower or "raw" in other_name_lower:
                            upstream_assets.append({
                                "id": other_asset.get("id"),
                                "name": other_asset.get("name"),
                                "stage": "extract",
                                "type": other_asset.get("type")
                            })
                        elif "analytics" in other_catalog_lower:
                            downstream_assets.append({
                                "id": other_asset.get("id"),
                                "name": other_asset.get("name"),
                                "stage": "load",
                                "type": other_asset.get("type")
                            })
                    elif stage == "load":
                        if "warehouse" in other_catalog_lower:
                            upstream_assets.append({
                                "id": other_asset.get("id"),
                                "name": other_asset.get("name"),
                                "stage": "transform",
                                "type": other_asset.get("type")
                            })
            
            detailed_asset["pipeline_metadata"] = {
                "is_pipeline_asset": True,
                "pipeline_type": pipeline_type,
                "pipeline_stage": stage or pipeline_stage,
                "pipeline_name": "Demo ETL Pipeline",
                "upstream_assets": upstream_assets,
                "downstream_assets": downstream_assets,
                "total_stages": 3,
                "current_stage_index": {"extract": 1, "transform": 2, "load": 3}.get(stage or pipeline_stage, 0),
                "pipeline_description": f"Part of {pipeline_type} pipeline - {stage or pipeline_stage} stage"
            }
        else:
            detailed_asset["pipeline_metadata"] = {
                "is_pipeline_asset": False
            }
        
        return jsonify(detailed_asset)

    @app.route("/api/s3/pending-assets", methods=["GET"])
    def get_pending_assets():
        from database import PendingAsset
        
        try:
            connector_id = request.args.get('connector_id')
            
            query = db.session.query(PendingAsset).filter(
                PendingAsset.status == 'pending'
            )
            
            if connector_id:
                query = query.filter(PendingAsset.connector_id == connector_id)
                
            pending_assets = query.order_by(PendingAsset.created_at.desc()).all()
            
            result = []
            for asset in pending_assets:
                asset_dict = {
                    'id': asset.id,
                    'name': asset.name,
                    'type': asset.type,
                    'catalog': asset.catalog,
                    'connector_id': asset.connector_id,
                    'change_type': asset.change_type,
                    's3_event_type': asset.s3_event_type,
                    'asset_id': asset.asset_id,
                    'asset_data': asset.asset_data,
                    'created_at': asset.created_at.isoformat() if asset.created_at else None
                }
                result.append(asset_dict)
                
            return jsonify({'pending_assets': result, 'count': len(result)}), 200
        except Exception as e:
            print(f"Error getting pending assets: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500

    def accept_pending_asset_old():
        from database import PendingAsset
        nonlocal discovered_assets
        
        try:
            data = request.json
            pending_id = data.get('pending_id')
            
            if not pending_id:
                return jsonify({'error': 'pending_id is required'}), 400
                
            with app.app_context():
                pending_asset = db.session.query(PendingAsset).filter(
                    PendingAsset.id == pending_id,
                    PendingAsset.status == 'pending'
                ).first()
                
                if not pending_asset:
                    return jsonify({'error': 'Pending asset not found'}), 404
                
                if pending_asset.change_type == 'deleted':
                    pending_asset.status = 'accepted'
                    pending_asset.processed_at = datetime.utcnow()
                    db.session.commit()
                    
                    asset_deleted = False
                    try:
                        from database import Asset
                        existing_asset = db.session.query(Asset).filter(
                            Asset.id == pending_asset.asset_id
                        ).first()
                        if existing_asset:
                            db.session.delete(existing_asset)
                            db.session.commit()
                            asset_deleted = True
                            print(f" Deleted asset {pending_asset.asset_id} from database")
                    except Exception as e:
                        print(f"Warning: Could not delete asset {pending_asset.asset_id} from database: {e}")
                    
                    discovered_assets = db_helpers.load_assets()
                    app.config['discovered_assets'] = discovered_assets
                    save_assets()
                    print(f" Reloaded {len(discovered_assets)} assets from database after removing asset")
                    
                    return jsonify({
                        'success': True, 
                        'message': 'Asset removed from inventory',
                        'assets_count': len(discovered_assets)
                    }), 200
                
                if pending_asset.asset_data:
                    asset_data = pending_asset.asset_data.copy()
                    
                    if 'id' not in asset_data or not asset_data['id']:
                        asset_data['id'] = pending_asset.asset_id
                    
                    asset_data['discovered_at'] = datetime.now().isoformat()
                    asset_data['status'] = 'active'
                    
                    if 'connector_id' not in asset_data:
                        asset_data['connector_id'] = pending_asset.connector_id
                    
                    if 'name' not in asset_data:
                        asset_data['name'] = pending_asset.name
                    
                    if 'type' not in asset_data:
                        asset_data['type'] = pending_asset.type
                    
                    if 'catalog' not in asset_data:
                        asset_data['catalog'] = pending_asset.catalog
                    
                    print(f" Saving asset with ID: {asset_data.get('id')}")
                    print(f"   Name: {asset_data.get('name')}")
                    print(f"   Type: {asset_data.get('type')}")
                    print(f"   Connector ID: {asset_data.get('connector_id')}")
                    
                    save_success = db_helpers.save_asset(asset_data)
                    if save_success:
                        print(f" Saved asset {asset_data.get('id')} to database")
                    else:
                        print(f" Failed to save asset {asset_data.get('id')} to database")
                        import traceback
                        traceback.print_exc()
                
                pending_asset.status = 'accepted'
                pending_asset.processed_at = datetime.utcnow()
                db.session.commit()
                
                discovered_assets = db_helpers.load_assets()
                app.config['discovered_assets'] = discovered_assets
                save_assets()
                print(f" Reloaded {len(discovered_assets)} assets from database after adding new asset")
                
                return jsonify({
                    'success': True, 
                    'message': 'Asset accepted and added to inventory',
                    'assets_count': len(discovered_assets)
                }), 200
        except Exception as e:
            with app.app_context():
                db.session.rollback()
            print(f"Error accepting pending asset: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': str(e)}), 500

    @app.route("/api/s3/dismiss-asset", methods=["POST"])
    def dismiss_pending_asset():
        from database import PendingAsset
        
        try:
            data = request.json
            pending_id = data.get('pending_id')
            
            if not pending_id:
                return jsonify({'error': 'pending_id is required'}), 400
                
            with app.app_context():
                pending_asset = db.session.query(PendingAsset).filter(
                    PendingAsset.id == pending_id,
                    PendingAsset.status == 'pending'
                ).first()
                
                if not pending_asset:
                    return jsonify({'error': 'Pending asset not found'}), 404
                
                pending_asset.status = 'dismissed'
                pending_asset.processed_at = datetime.utcnow()
                db.session.commit()
                
                return jsonify({'success': True, 'message': 'Asset notification dismissed'}), 200
        except Exception as e:
            with app.app_context():
                db.session.rollback()
            print(f"Error dismissing pending asset: {e}")
            return jsonify({'error': str(e)}), 500

    @app.route("/api/connectors", methods=["GET"])
    def get_connectors():
        nonlocal active_connectors
        return jsonify(active_connectors)

    @app.route("/api/activities", methods=["GET"])
    def get_activities():
        return jsonify([])

    @app.route("/api/connectors/<string:connector_id>/toggle", methods=["POST"])
    def toggle_connector(connector_id: str):
        nonlocal active_connectors
        connector = next((c for c in active_connectors if c["id"] == connector_id), None)
        if not connector:
            abort(404, "Connector not found")
        
        was_enabled = connector.get("enabled", False)
        connector["enabled"] = not connector["enabled"]
        is_now_enabled = connector["enabled"]
        
        save_connectors()
        
        if connector.get("type") == "Amazon S3" and is_now_enabled and not was_enabled:
            try:
                from api.s3 import trigger_airflow_dag
                trigger_result = trigger_airflow_dag('s3_asset_discovery')
                if trigger_result.get('success'):
                    print(f"  Airflow DAG triggered after enabling S3 connector: {connector_id}")
                else:
                    print(f"   Could not trigger Airflow DAG (will run on schedule): {trigger_result.get('message')}")
            except Exception as e:
                print(f"   Error triggering Airflow DAG (will run on schedule): {e}")
        
        return jsonify({"message": f"Connector {connector_id} {'enabled' if connector['enabled'] else 'disabled'}"})

    @app.route("/api/connectors/<string:connector_id>", methods=["DELETE"])
    def delete_connector(connector_id: str):
        nonlocal active_connectors, discovered_assets
        
        connector = None
        for i, conn in enumerate(active_connectors):
            if conn["id"] == connector_id:
                connector = conn
                break
        
        if not connector:
            abort(404, "Connector not found")
        
        assets_before = len(discovered_assets)
        assets_to_delete = [asset for asset in discovered_assets if asset.get("connector_id") == connector_id]
        assets_deleted = len(assets_to_delete)
        
        
        db_success = db_helpers.delete_connector(connector_id)
        assets_db_success = db_helpers.delete_assets_by_connector(connector_id)
        
        pending_assets_deleted = 0
        try:
            from database import PendingAsset
            with app.app_context():
                pending_assets_deleted = db.session.query(PendingAsset).filter(
                    PendingAsset.connector_id == connector_id
                ).delete()
                db.session.commit()
                if pending_assets_deleted > 0:
                    print(f" Deleted {pending_assets_deleted} pending asset(s) for connector {connector_id}")
        except Exception as e:
            print(f" Warning: Failed to delete pending assets for connector {connector_id}: {e}")
        
        try:
            from database import PendingAsset
            with app.app_context():
                pending_count = db.session.query(PendingAsset).filter(
                    PendingAsset.connector_id == connector_id
                ).delete()
                db.session.commit()
                if pending_count > 0:
                    print(f" Deleted {pending_count} pending asset(s) for connector {connector_id}")
        except Exception as e:
            print(f" Warning: Failed to delete pending assets for connector {connector_id}: {e}")
        
        if not db_success:
            print(f" Warning: Failed to delete connector {connector_id} from database")
        if not assets_db_success:
            print(f" Warning: Failed to delete assets for connector {connector_id} from database")
        
        active_connectors = [conn for conn in active_connectors if conn["id"] != connector_id]
        discovered_assets = [asset for asset in discovered_assets if asset.get("connector_id") != connector_id]
        
        app.config['active_connectors'] = active_connectors
        app.config['discovered_assets'] = discovered_assets
        
        assets_after = len(discovered_assets)
        
        
        return jsonify({
            "message": f"Connector '{connector['name']}' and {assets_deleted} associated assets have been deleted successfully",
            "assets_deleted": assets_deleted,
            "deleted_from_db": db_success,
            "assets_deleted_from_db": assets_db_success
        })

    @app.route("/api/scan/start", methods=["POST"])
    def start_scan():
        return jsonify({"message": "Scan started", "scan_id": "scan_123"})

    @app.route("/api/data/reload", methods=["POST"])
    def reload_data():
        nonlocal discovered_assets, active_connectors
        
        save_connectors()
        save_assets()
        
        
        return jsonify({
            "message": "Data reloaded successfully",
            "connectors": len(active_connectors),
            "assets": len(discovered_assets)
        })

    @app.route("/api/data/clear-all", methods=["DELETE"])
    def clear_all_data():
        nonlocal discovered_assets, active_connectors
        
        assets_count = len(discovered_assets)
        connectors_count = len(active_connectors)
        
        discovered_assets = []
        active_connectors = []
        
        save_connectors()
        save_assets()
        
        
        return jsonify({
            "message": "All data cleared successfully",
            "assets_deleted": assets_count,
            "connectors_deleted": connectors_count
        })

    @app.route("/api/scheduler/status", methods=["GET"])
    def get_scheduler_status():
        nonlocal scheduler_running, scheduler
        next_run = None
        if scheduler_running and scheduler.running:
            jobs = scheduler.get_jobs()
            if jobs:
                next_run = jobs[0].next_run_time.isoformat() if jobs[0].next_run_time else None
        return jsonify({
            "scheduler_running": scheduler_running and scheduler.running if scheduler_running else False, 
            "next_run": next_run
        })

    @app.route("/api/scheduler/start", methods=["POST"])
    def start_background_scheduler():
        try:
            start_scheduler()
            return jsonify({"message": "Scheduler started", "scheduler_running": scheduler_running}), 200
        except Exception as e:
            return jsonify({"message": f"Error starting scheduler: {str(e)}"}), 500

    @app.route("/api/scheduler/stop", methods=["POST"])
    def stop_background_scheduler():
        try:
            stop_scheduler()
            return jsonify({"message": "Scheduler stopped", "scheduler_running": scheduler_running}), 200
        except Exception as e:
            return jsonify({"message": f"Error stopping scheduler: {str(e)}"}), 500
    
    @app.route("/api/scheduler/sync-now", methods=["POST"])
    def trigger_sync_now():
        try:
            sync_connectors()
            return jsonify({"message": "Sync completed", "assets_count": len(discovered_assets)}), 200
        except Exception as e:
            import traceback
            traceback.print_exc()
            return jsonify({"message": f"Error during sync: {str(e)}"}), 500

    @app.route("/api/marketplace/assets", methods=["GET"])
    def get_marketplace_assets():
        nonlocal discovered_assets
        return jsonify([])
    
    @app.route("/api/marketplace/search-table", methods=["POST"])
    def search_marketplace_table():
        nonlocal discovered_assets, active_connectors
        data = request.get_json()
        resource_type = data.get('resourceType')
        
        if resource_type == 'GCP':
            project_id = data.get('projectId')
            dataset_id = data.get('datasetId')
            table_id = data.get('tableId')
            
            if not all([project_id, dataset_id, table_id]):
                return jsonify({"error": "Missing required fields"}), 400
            
            table_full_id = f"{project_id}.{dataset_id}.{table_id}"
            asset = None
            for a in discovered_assets:
                if a.get('id') == table_full_id:
                    asset = a
                    break
            
            if not asset:
                return jsonify({"error": f"Table {table_full_id} not found in discovered assets. Please run asset discovery first."}), 404
            
            columns = []
            for col in asset.get('columns', []):
                columns.append({
                    "name": col.get('name'),
                    "type": col.get('type'),
                    "mode": col.get('mode', 'NULLABLE'),
                    "description": col.get('description', ''),
                    "tags": [],
                    "piiFound": False,
                    "piiType": ""
                })
            
            return jsonify({
                "tableName": table_id,
                "projectId": project_id,
                "datasetId": dataset_id,
                "columns": columns,
                "tableTags": []
            })
        
        elif resource_type == 'Starburst Galaxy':
            catalog = data.get('catalog')
            schema_name = data.get('schema_name')
            table_name = data.get('tableName')
            
            if not all([catalog, schema_name, table_name]):
                return jsonify({"error": "Missing required fields"}), 400
            
            table_full_id = f"starburst.{catalog}.{schema_name}.{table_name}"
            asset = None
            for a in discovered_assets:
                if a.get('id') == table_full_id:
                    asset = a
                    break
            
            if not asset:
                return jsonify({"error": f"Table {catalog}.{schema_name}.{table_name} not found in discovered assets. Please run asset discovery first."}), 404
            
            import requests
            import base64
            
            try:
                starburst_connector = next((c for c in active_connectors if c.get('type') == 'Starburst Galaxy' and c.get('enabled')), None)
                
                if starburst_connector:
                    config = starburst_connector.get('config', {})
                    if config.get('account_domain'):
                        base_url = f"https://{config.get('account_domain')}"
                    else:
                        base_url = config.get('url', '').rstrip('/')
                    client_id = config.get('client_id')
                    client_secret = config.get('client_secret')
                    username = config.get('username')
                    password = config.get('password')
                    access_token = config.get('access_token')
                    
                    if not access_token:
                        if client_id and client_secret:
                            token_url = f"{base_url}/oauth/v2/token"
                            auth_string = f"{client_id}:{client_secret}"
                            auth_b64 = base64.b64encode(auth_string.encode()).decode()
                            token_headers = {'Authorization': f'Basic {auth_b64}', 'Content-Type': 'application/x-www-form-urlencoded'}
                            token_response = requests.post(token_url, headers=token_headers, data='grant_type=client_credentials', timeout=30, verify=False)
                            
                            if token_response.status_code == 200:
                                access_token = token_response.json().get('access_token')
                    
                    headers = {}
                    auth = None
                    if access_token:
                        headers['Authorization'] = f'Bearer {access_token}'
                        headers['Content-Type'] = 'application/json'
                    elif username and password:
                        auth = (username, password)
                        headers['Content-Type'] = 'application/json'
                    
                    if access_token or auth:
                        
                        catalogs_url = f"{base_url}/public/api/v1/catalog"
                        cat_response = requests.get(catalogs_url, headers=headers, timeout=30)
                        if cat_response.status_code == 200:
                            catalogs_data = cat_response.json()
                            catalogs_list = catalogs_data.get('result', []) if isinstance(catalogs_data, dict) else catalogs_data
                            
                            catalog_id = None
                            for cat in catalogs_list:
                                cat_name = cat.get('catalogName') or cat.get('name', '')
                                if cat_name == catalog:
                                    catalog_id = cat.get('catalogId') or cat.get('id')
                                    break
                            
                            if catalog_id:
                                schemas_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema"
                                sch_response = requests.get(schemas_url, headers=headers, auth=auth, timeout=30, verify=False)
                                
                                if sch_response.status_code != 200:
                                    schemas_url = f"{base_url}/v1/catalog/{catalog}/schema"
                                    sch_response = requests.get(schemas_url, headers=headers, auth=auth, timeout=30, verify=False)
                                if sch_response.status_code == 200:
                                    schemas_data = sch_response.json()
                                    schemas_list = schemas_data.get('result', []) if isinstance(schemas_data, dict) else schemas_data
                                    
                                    schema_id = None
                                    for sch in schemas_list:
                                        sch_name = sch.get('schemaName') or sch.get('name', '')
                                        sch_id = sch.get('schemaId') or sch.get('id')
                                        if sch_name == schema_name or sch_id == schema_name:
                                            schema_id = sch_id
                                            break
                                    
                                    if schema_id:
                                        columns_url = f"{base_url}/public/api/v1/catalog/{catalog_id}/schema/{schema_id}/table/{table_name}/column"
                                        columns_response = requests.get(columns_url, headers=headers, auth=auth, timeout=30, verify=False)
                                        
                                        if columns_response.status_code != 200:
                                            columns_url = f"{base_url}/v1/catalog/{catalog}/schema/{schema_name}/table/{table_name}"
                                            columns_response = requests.get(columns_url, headers=headers, auth=auth, timeout=30, verify=False)
                                
                                        if columns_response.status_code == 200:
                                            columns_data = columns_response.json()
                                            if isinstance(columns_data, dict):
                                                starburst_columns = columns_data.get('result', []) or columns_data.get('columns', [])
                                            else:
                                                starburst_columns = []
                                    
                                    column_tags_map = {}
                                    for sb_col in starburst_columns:
                                        col_id = sb_col.get('columnId')
                                        col_tags = sb_col.get('tags', [])
                                        tag_names = [tag.get('name') for tag in col_tags if tag.get('name')]
                                        column_tags_map[col_id] = tag_names
                                    
                                    columns = []
                                    for col in asset.get('columns', []):
                                        column_name = col.get('name')
                                        real_tags = column_tags_map.get(column_name, [])
                                        
                                        columns.append({
                                            "name": column_name,
                                            "type": col.get('type'),
                                            "mode": "NULLABLE",
                                            "description": col.get('description', ''),
                                            "tags": real_tags,
                                            "piiFound": False,
                                            "piiType": ""
                                        })
                                    
                                    return jsonify({
                                        "tableName": table_name,
                                        "catalog": catalog,
                                        "schema": schema_name,
                                        "columns": columns,
                                        "tableTags": []
                                    })
            except Exception as e:
                print(f"Warning: Could not fetch real tags from Starburst: {e}")
            
            columns = []
            for col in asset.get('columns', []):
                columns.append({
                    "name": col.get('name'),
                    "type": col.get('type'),
                    "mode": "NULLABLE",
                    "description": col.get('description', ''),
                    "tags": [],
                    "piiFound": False,
                    "piiType": ""
                })
            
            return jsonify({
                "tableName": table_name,
                "catalog": catalog,
                "schema": schema_name,
                "columns": columns,
                "tableTags": []
            })
        
        return jsonify({"error": "Unsupported resource type"}), 400
    
    @app.route("/api/marketplace/publish", methods=["POST"])
    def publish_to_marketplace():
        data = request.get_json()
        return jsonify({"message": "Asset published to marketplace", "asset_id": data.get('asset_id')}), 200
    
    @app.route("/api/governance/policies", methods=["GET"])
    def get_governance_policies():
        return jsonify([])

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for('get_dashboard_stats')) 

        if request.method == 'POST':
            username = request.json.get('username')
            password = request.json.get('password')
            user = db.session.query(User).filter_by(username=username).first()
            if user and user.check_password(password):
                login_user(user)
                return jsonify({"message": "Login successful", "user": user.username}), 200
            return jsonify({"message": "Invalid username or password"}), 401
        return jsonify({"message": "Please login", "login_required": True}), 401

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return jsonify({"message": "Logged out successfully"}), 200

    @app.route("/api/protected_resource")
    @login_required
    def protected_resource():
        return jsonify({"message": f"Hello, {current_user.username}! This is a protected resource.", "role": current_user.role})

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    print(" Initializing MySQL database...")
    if init_db():
        print(" Database initialized successfully")
        with app.app_context():
            db.create_all()
            Base.metadata.create_all(bind=db.engine)
            print(" All database tables created (including Hive Metastore tables)")
            load_connectors()
            load_assets()
            
            print(" S3 event monitoring DISABLED - using Airflow DAG for asset discovery")
            
    else:
        print(" Failed to initialize database - check MySQL connection")
        print(f"   Connection details: root@localhost:3306/torro_db")
        print(f"   Make sure MySQL is running and credentials are correct")

    scheduler = BackgroundScheduler(daemon=True)
    scheduler_running = False

    def sync_connectors():
        nonlocal active_connectors, discovered_assets
        
        # Reload connectors from database to get latest state
        try:
            loaded_connectors = load_connectors()
            if loaded_connectors:
                active_connectors = loaded_connectors
        except Exception as e:
            print(f"  Error reloading connectors: {e}")
        
        if not active_connectors:
            print("  No connectors configured, skipping sync")
            return
        
        print(f" [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Starting continuous API sync for {len(active_connectors)} connectors...")
        
        # Debug: Log connector types
        connector_types = [c.get("type", "Unknown") for c in active_connectors]
        print(f" [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Connector types found: {connector_types}")
        
        for connector in active_connectors:
            if not connector.get("enabled", False):
                print(f" [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Skipping disabled connector: {connector.get('name', 'Unknown')}")
                continue
            
            connector_type = connector.get("type", "").lower()
            connector_id = connector.get("id", "")
            connector_name = connector.get("name", "Unknown")
            
            print(f" [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Processing connector: {connector_name} (type: {connector.get('type')}, lower: {connector_type})")
            
            current_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f" [{current_time}] Fetching API data for {connector_name} (Type: {connector_type})...")
            
            # Don't update last_run here - it's updated AFTER successful rediscovery for each connector type
            # This was causing Azure Blob Storage to always skip because last_run was set to "now" before checking
            
            if connector_type == "bigquery":
                print(f" [{current_time}] Fetching BigQuery API for {connector_name}...")
                
                config = connector.get("config", {})
                service_account_json = config.get("service_account_json")
                project_id = config.get("project_id")
                
                if service_account_json and project_id:
                    try:
                        service_account_info = json.loads(service_account_json)
                        credentials = service_account.Credentials.from_service_account_info(
                            service_account_info,
                            scopes=["https://www.googleapis.com/auth/bigquery.readonly"]
                        )
                        client = bigquery.Client(credentials=credentials, project=project_id)
                        
                        new_assets = []
                        datasets = list(client.list_datasets())
                        
                        for dataset in datasets:
                            dataset_id = dataset.dataset_id
                            dataset_ref = client.dataset(dataset_id)
                            
                            tables = list(client.list_tables(dataset_ref))
                            
                            for table_item in tables:
                                try:
                                    table_ref = dataset_ref.table(table_item.table_id)
                                    table = client.get_table(table_ref)
                                    
                                    asset_id = f"{project_id}.{dataset_id}.{table_item.table_id}"
                                    
                                    columns = []
                                    for field in table.schema:
                                        columns.append({
                                            "name": field.name,
                                            "type": field.field_type,
                                            "mode": field.mode,
                                            "description": field.description or ""
                                        })
                                    
                                    extra_data = {
                                        "source_system": "BigQuery",
                                        "dataset": dataset_id,  
                                        "project_id": project_id,
                                        "columns": columns,
                                        "row_count": table.num_rows,
                                        "size": table.num_bytes,
                                        "created": table.created.isoformat() if table.created else None,
                                        "modified": table.modified.isoformat() if table.modified else None
                                    }
                                    
                                    if table_item.table_type == "VIEW" and hasattr(table, 'view_query'):
                                        extra_data["sql"] = table.view_query
                                        extra_data["definition"] = table.view_query
                                        extra_data["view_definition"] = table.view_query
                                    
                                    asset = {
                                        "id": asset_id,
                                        "name": table_item.table_id,
                                        "type": table_item.table_type,
                                        "catalog": dataset_id,  
                                        "connector_id": connector_id,
                                        "extra_data": extra_data
                                    }
                                    
                                    new_assets.append(asset)
                                
                                except Exception as table_error:
                                    print(f"  Error fetching table {table_item.table_id}: {table_error}")
                                    continue
                        
                        for asset in new_assets:
                            db_helpers.save_asset(asset)
                        
                        discovered_assets[:] = load_assets()
                        
                        connector["assets_count"] = len(new_assets)
                        connector["status"] = "active"
                        connector["last_run"] = datetime.now().isoformat()  # Update last_run after successful discovery
                        print(f" [{current_time}] BigQuery connector {connector_name} discovered {len(new_assets)} assets")
                    
                    except Exception as bq_error:
                        print(f" [{current_time}] Error discovering BigQuery assets: {bq_error}")
                        import traceback
                        traceback.print_exc()
                        connector["assets_count"] = 0
                        connector["status"] = "error"
                else:
                    print(f"  [{current_time}] BigQuery connector {connector_name} missing credentials")
                    connector["assets_count"] = 0
                    connector["status"] = "error"
            
            elif connector_type == "starburst galaxy":
                print(f" [{current_time}] Fetching Starburst API for {connector_name}...")
                
                config = connector.get("config", {})
                account_domain = config.get("account_domain")
                
                client_id = config.get("client_id")
                client_secret = config.get("client_secret") or config.get("secret_key")
                access_token = config.get("access_token")
                
                if account_domain and (access_token or (client_id and client_secret)):
                    try:
                        from api.starburst import discover_all_starburst_connectors, get_starburst_access_token
                        
                        if not access_token and client_id and client_secret:
                            print(f" [{current_time}] Exchanging client credentials for access token...")
                            access_token = get_starburst_access_token(account_domain, client_id, client_secret)
                            if not access_token:
                                print(f" [{current_time}] Failed to get access token")
                                connector["assets_count"] = 0
                                connector["status"] = "error"
                                continue
                        
                        print(f" [{current_time}] Starting Starburst discovery for {connector_name}...")
                        connectors_info = discover_all_starburst_connectors(account_domain, access_token)
                        print(f" [{current_time}] Got {len(connectors_info)} catalogs from Starburst")
                        
                        total_tables_discovered = 0
                        for cat_info in connectors_info:
                            for schema_info in cat_info.get('schemas', []):
                                total_tables_discovered += len(schema_info.get('tables', []))
                        print(f" [{current_time}] Discovered {total_tables_discovered} total tables from Starburst")
                        
                        new_assets = []
                        for catalog_info in connectors_info:
                            catalog_id = catalog_info['catalog_id']
                            catalog_name = catalog_info['catalog_name']
                            schema_count = len(catalog_info.get('schemas', []))
                            tables_in_catalog = sum(len(s.get('tables', [])) for s in catalog_info.get('schemas', []))
                            print(f" [{current_time}] Processing catalog {catalog_name}: {schema_count} schemas, {tables_in_catalog} tables")
                            
                            for schem_info in catalog_info.get('schemas', []):
                                schema_name = schem_info['schema_name']
                                tables_in_schema = len(schem_info.get('tables', []))
                                
                                for table_info in schem_info.get('tables', []):
                                    table_name = table_info['table_name']
                                    table_type = table_info.get('table_type', 'TABLE')
                                    
                                    asset_id = f"{account_domain}.{catalog_name}.{schema_name}.{table_name}"
                                    
                                    columns = []
                                    for col in table_info.get('columns', []):
                                        columns.append({
                                            "name": col.get('name', ''),
                                            "type": col.get('type', ''),
                                            "nullable": col.get('nullable', True),
                                            "description": col.get('description', ''),
                                            "tags": col.get('tags', [])
                                        })
                                    
                                    extra_data = {
                                        "source_system": "Starburst Galaxy",
                                        "columns": columns,
                                        "catalog_id": catalog_id,
                                        "catalog_type": catalog_info.get('catalog_type'),
                                        "connector_type": catalog_info.get('connector_type'),
                                        "account_domain": account_domain,
                                        "technical_metadata": {
                                            "asset_id": asset_id,
                                            "asset_type": table_type.upper(),
                                            "location": f"{account_domain}/{catalog_name}/{schema_name}/{table_name}",
                                            "format": "Starburst Table",
                                            "source_system": "Starburst Galaxy",
                                            "schema_name": schema_name,
                                            "table_name": table_name,
                                            "column_count": len(columns),
                                            "owner": "account_admin"
                                        },
                                        "business_metadata": {
                                            "description": "",
                                            "business_owner": "account_admin",
                                            "owner": "account_admin",
                                            "department": schema_name,
                                            "classification": "internal",
                                            "sensitivity_level": "low",
                                            "tags": []
                                        },
                                        "operational_metadata": {
                                            "status": "active",
                                            "owner": "account_admin",
                                            "last_modified": datetime.now().isoformat(),
                                            "last_accessed": datetime.now().isoformat(),
                                            "access_count": "N/A",
                                            "data_quality_score": 95
                                        }
                                    }
                                    
                                    asset = {
                                        "id": asset_id,
                                        "name": table_name,
                                        "type": table_type.upper(),
                                        "catalog": catalog_name,
                                        "schema": schema_name,
                                        "connector_id": connector_id,
                                        "discovered_at": datetime.now().isoformat(),
                                        "status": "active",
                                        "extra_data": extra_data
                                    }
                                    
                                    new_assets.append(asset)
                        
                        print(f" [{current_time}] Processed {len(new_assets)} assets from {len(connectors_info)} catalogs (expected ~{total_tables_discovered} tables)")
                        
                        print(f" [{current_time}] Saving {len(new_assets)} assets to MySQL...")
                        saved_count = 0
                        failed_count = 0
                        failed_assets = []
                        error_details = []
                        
                        for idx, asset in enumerate(new_assets):
                            if not asset.get('id'):
                                print(f"  [{current_time}] Skipping asset with no ID: {asset.get('name', 'Unknown')}")
                                failed_count += 1
                                failed_assets.append(f"no_id:{asset.get('name', 'Unknown')}")
                                continue
                            
                            db_asset = {
                                'id': asset.get('id'),
                                'name': asset.get('name', 'Unknown'),
                                'type': asset.get('type', 'Table'),
                                'catalog': asset.get('catalog'),
                                'schema': asset.get('schema'),
                                'connector_id': asset.get('connector_id'),
                                'discovered_at': asset.get('discovered_at', datetime.now().isoformat()),
                                'status': asset.get('status', 'active'),
                                'extra_data': asset
                            }
                            
                            if not db_asset.get('name'):
                                db_asset['name'] = db_asset['id'].split('.')[-1] if '.' in db_asset['id'] else 'Unknown'
                            
                            try:
                                success = db_helpers.save_asset(db_asset)
                                if success:
                                    saved_count += 1
                                    if saved_count % 50 == 0:
                                        print(f" [{current_time}] Saved {saved_count}/{len(new_assets)} assets so far...")
                                else:
                                    failed_count += 1
                                    failed_assets.append(db_asset.get('id', 'Unknown'))
                                    error_msg = f"save_asset returned False"
                                    error_details.append(f"{db_asset.get('id', 'Unknown')}: {error_msg}")
                                    if connector.get('type') == 'Starburst Galaxy' or failed_count <= 20:
                                        print(f"  [{current_time}] Failed to save asset {idx+1}/{len(new_assets)}: {db_asset.get('id', 'Unknown')}")
                            except Exception as save_error:
                                failed_count += 1
                                failed_assets.append(db_asset.get('id', 'Unknown'))
                                error_msg = str(save_error)
                                error_details.append(f"{db_asset.get('id', 'Unknown')}: {error_msg}")
                                if connector.get('type') == 'Starburst Galaxy' or failed_count <= 20:
                                    print(f" [{current_time}] Exception saving asset {idx+1}/{len(new_assets)} {db_asset.get('id', 'Unknown')}: {save_error}")
                                    import traceback
                                    if connector.get('type') == 'Starburst Galaxy' and failed_count <= 5:
                                        traceback.print_exc()
                        
                        print(f" [{current_time}] Saved {saved_count}/{len(new_assets)} assets ({failed_count} failed) for connector {connector_name}")
                        if failed_count > 0:
                            print(f"  [{current_time}] Failed asset count: {failed_count}")
                            if error_details and len(error_details) <= 10:
                                print(f"  [{current_time}] Error details:")
                                for err in error_details[:10]:
                                    print(f"     - {err}")
                            if failed_count <= 50:
                                print(f"  [{current_time}] Failed asset IDs: {failed_assets[:50]}")
                            else:
                                print(f"  [{current_time}] First 50 failed asset IDs: {failed_assets[:50]}")
                                print(f"  [{current_time}] ... and {failed_count - 50} more")
                        
                        print(f" [{current_time}] Reloading assets from MySQL...")
                        discovered_assets[:] = load_assets()
                        
                        connector_assets_count = len([a for a in discovered_assets if a.get('connector_id') == connector_id])
                        connector["assets_count"] = connector_assets_count
                        connector["status"] = "active"
                        connector["last_run"] = datetime.now().isoformat()  # Update last_run after successful discovery
                        print(f" [{current_time}] Starburst connector {connector_name} has {connector_assets_count} total assets")
                        print(f" [{current_time}] Total assets in memory: {len(discovered_assets)}")
                    
                    except Exception as starburst_error:
                        print(f" [{current_time}] Error discovering Starburst assets: {starburst_error}")
                        import traceback
                        traceback.print_exc()
                        connector["assets_count"] = 0
                        connector["status"] = "error"
                else:
                    print(f"  [{current_time}] Starburst connector {connector_name} missing credentials")
                    connector["assets_count"] = 0
                    connector["status"] = "error"
            
            elif connector_type == "azure blob storage" or connector_type == "azure data storage":
                connector_type_display = "Azure Data Storage" if connector_type == "azure data storage" else "Azure Blob Storage"
                print(f" [{current_time}] Checking {connector_type_display} connector: {connector_name}...")
                
                config = connector.get("config", {})
                account_name = config.get("account_name") or config.get("accountName")
                account_key = config.get("account_key") or config.get("accountKey")
                connection_string = config.get("connection_string") or config.get("connectionString")
                container_name = config.get("container_name") or config.get("containerName")
                share_name = config.get("share_name") or config.get("shareName")
                
                # Check if we should run rediscovery (every 5 minutes by default)
                last_run = connector.get("last_run")
                rediscovery_interval = config.get("rediscovery_interval_minutes", 5)  # Default 5 minutes
                
                print(f"  [{current_time}] Rediscovery interval: {rediscovery_interval} minutes, Last run: {last_run}")
                
                if last_run:
                    try:
                        last_run_dt = datetime.fromisoformat(last_run.replace('Z', '+00:00'))
                        time_since_last_run = (datetime.now() - last_run_dt.replace(tzinfo=None)).total_seconds() / 60
                        
                        print(f"  [{current_time}] Time since last run: {time_since_last_run:.2f} minutes")
                        
                        if time_since_last_run < rediscovery_interval:
                            print(f"  [{current_time}] ⏸️ Skipping rediscovery - only {time_since_last_run:.2f} minutes since last run (interval: {rediscovery_interval} min)")
                            continue
                        else:
                            print(f"  [{current_time}] ✅ Time interval met ({time_since_last_run:.2f} >= {rediscovery_interval} min), proceeding with rediscovery...")
                    except Exception as e:
                        print(f"  [{current_time}] Error parsing last_run timestamp: {e}, proceeding with rediscovery...")
                else:
                    print(f"  [{current_time}] ⚠️ No last_run timestamp found, proceeding with initial rediscovery...")
                
                if not connection_string and not (account_name and account_key):
                    print(f"  [{current_time}] {connector_type_display} connector {connector_name} missing credentials")
                    connector["assets_count"] = 0
                    connector["status"] = "error"
                    continue
                
                try:
                    if connector_type == "azure data storage":
                        from api.azure_unified import discover_azure_all_assets
                        print(f"  [{current_time}] Starting Azure Data Storage discovery (Blob, Files, Tables, Queues)...")
                        result = discover_azure_all_assets(
                            account_name=account_name,
                            account_key=account_key,
                            connection_string=connection_string,
                            container_name=container_name,
                            share_name=share_name
                        )
                        assets = result.get('all_assets', [])
                        summary = result.get('summary', {})
                        print(f"  [{current_time}] Discovered {len(assets)} total assets (Blob: {summary.get('blob_assets', 0)}, Files: {summary.get('file_assets', 0)}, Tables: {summary.get('table_assets', 0)}, Queues: {summary.get('queue_assets', 0)})")
                    else:
                        from api.azure_blob import discover_azure_blob_assets_structured
                        print(f"  [{current_time}] Starting Azure Blob Storage discovery...")
                        structured_data = discover_azure_blob_assets_structured(
                            account_name=account_name,
                            account_key=account_key,
                            connection_string=connection_string,
                            container_name=container_name
                        )
                        assets = structured_data.get('all_assets', [])
                        containers = structured_data.get('containers', [])
                        print(f"  [{current_time}] Discovered {len(assets)} assets across {len(containers)} container(s)")
                    
                    # Log discovered asset names for debugging
                    if len(assets) > 0:
                        print(f"  [{current_time}] Discovered asset names: {[a.get('name') for a in assets[:10]]}")
                        if len(assets) > 10:
                            print(f"  [{current_time}] ... and {len(assets) - 10} more")
                    
                    existing_assets = db_helpers.load_assets()
                    existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
                    
                    print(f"  [{current_time}] Existing assets in DB: {len(existing_asset_ids)}")
                    
                    new_assets = []
                    updated_count = 0
                    
                    for asset in assets:
                        asset['connector_id'] = connector_id
                        asset['discovered_at'] = datetime.now().isoformat()
                        asset['status'] = 'active'
                        
                        asset_id = asset.get('id')
                        asset_name = asset.get('name', 'Unknown')
                        
                        if asset_id:
                            if asset_id not in existing_asset_ids:
                                new_assets.append(asset)
                                existing_asset_ids.add(asset_id)
                                print(f"  [{current_time}] 🆕 NEW asset found: {asset_name} (ID: {asset_id[:50]}...)")
                            else:
                                # Update existing asset
                                try:
                                    db_helpers.save_asset(asset)
                                    updated_count += 1
                                except Exception as e:
                                    print(f"  [{current_time}] Error updating asset {asset_id}: {e}")
                        else:
                            print(f"  [{current_time}] ⚠️  Asset missing ID: {asset_name}")
                    
                    print(f"  [{current_time}] Found {len(new_assets)} new asset(s) to save")
                    
                    # Save new assets
                    saved_count = 0
                    for asset in new_assets:
                        try:
                            if db_helpers.save_asset(asset):
                                saved_count += 1
                                print(f"  [{current_time}] ✅ Saved new asset: {asset.get('name', 'Unknown')}")
                            else:
                                print(f"  [{current_time}] ❌ Failed to save asset: {asset.get('name', 'Unknown')} (save_asset returned False)")
                        except Exception as e:
                            print(f"  [{current_time}] ❌ Error saving asset {asset.get('id', 'Unknown')}: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    discovered_assets[:] = load_assets()
                    
                    connector_assets_count = len([a for a in discovered_assets if a.get('connector_id') == connector_id])
                    connector["assets_count"] = connector_assets_count
                    connector["status"] = "active"
                    connector["last_run"] = datetime.now().isoformat()
                    
                    # Save connector to database with updated last_run
                    try:
                        db_helpers.save_connector(connector)
                    except Exception as save_err:
                        print(f"  [{current_time}] Error saving connector after rediscovery: {save_err}")
                    
                    print(f"  [{current_time}] ✅ {connector_type_display} connector {connector_name}: {saved_count} new, {updated_count} updated, {connector_assets_count} total assets")
                
                except Exception as azure_error:
                    print(f"  [{current_time}] Error rediscovering {connector_type_display} assets: {azure_error}")
                    import traceback
                    traceback.print_exc()
                    connector["assets_count"] = 0
                    connector["status"] = "error"
            
            else:
                print(f"  [{current_time}] Unknown connector type: {connector_type}")
            
            save_connectors()
        
        completion_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f" [{completion_time}] Continuous API sync completed - {len(active_connectors)} connectors, {len(discovered_assets)} total assets")

    def start_scheduler():
        nonlocal scheduler_running, scheduler
        
        if scheduler_running:
            print("  Scheduler is already running")
            return
        
        print(" Starting background scheduler for continuous API fetching...")
        scheduler.add_job(
            sync_connectors,
            trigger=IntervalTrigger(seconds=1),  
            id='continuous_sync',
            name='Continuous API Sync (Always Fetching Every Second)',
            replace_existing=True
        )
        
        scheduler.start()
        scheduler_running = True
        print(" Background scheduler started - continuous API fetching EVERY SECOND")
        print(" Logs will show continuous API activity every 1 second...")

    def stop_scheduler():
        nonlocal scheduler_running, scheduler
        
        if scheduler_running:
            print(" Stopping background scheduler...")
            scheduler.shutdown(wait=False)
            scheduler_running = False
            print(" Background scheduler stopped")

    print(" S3 Event Processor DISABLED - using Airflow DAG for asset discovery")

    try:
        from services.gcs_event_processor import gcs_event_processor, set_socketio as set_gcs_socketio
        set_gcs_socketio(socketio)
        gcs_event_processor.start()
        print(" GCS Event Processor initialized and started")
    except Exception as e:
        print(f"  Failed to start GCS Event Processor: {e}")
        import traceback
        traceback.print_exc()

    try:
        start_scheduler()
        print(" Background scheduler started for continuous connector sync")
    except Exception as e:
        print(f"  Failed to start background scheduler: {e}")
        import traceback
        traceback.print_exc()

    app.config['socketio_instance'] = socketio
    
    return app, socketio

# Create app instance for gunicorn
app, socketio = create_app()

if __name__ == "__main__":
    print(" Starting Flask-SocketIO server on http://localhost:8099")
    print(" Note: For production with 20 workers, use: bash start_server.sh")
    socketio.run(
        app, 
        host="0.0.0.0", 
        port=8099, 
        debug=False, 
        allow_unsafe_werkzeug=True,
        use_reloader=False,
        log_output=False
    )
