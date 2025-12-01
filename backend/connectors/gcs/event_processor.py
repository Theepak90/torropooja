
import json as json_module
from datetime import datetime
from typing import List, Dict, Any, Optional
import db_helpers
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config
from urllib.parse import unquote

_socketio_instance = None

def set_socketio(socketio):
    global _socketio_instance
    _socketio_instance = socketio

class GCSEventProcessor:
    def __init__(self):
        self.running = False
        self.engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
        self.Session = sessionmaker(bind=self.engine)
    
    def start(self):
        if self.running:
            print(" GCS Event Processor already running")
            return
        
        self.running = True
        print(" GCS Event Processor started - ready to receive events via HTTP webhook")
        print(" Events will be received from: Pub/Sub → Eventarc → Cloud Run → Torro API")
    
    def stop(self):
        self.running = False
        print(" GCS Event Processor stopped")
    
    def process_event(self, bucket: str, object_name: str, event_type: str, 
                     connector: Dict, event_data: Optional[Dict] = None):
        try:
            print(f" Processing GCS event:")
            print(f" Bucket: {bucket}")
            print(f" Object: {object_name}")
            print(f" Event Type: {event_type}")
            print(f" Connector: {connector.get('name')}")
            
            change_type = self._determine_change_type(event_type)
            
            if change_type == 'created':
                self._handle_object_created(bucket, object_name, connector, event_type, event_data or {})
            elif change_type == 'deleted':
                self._handle_object_deleted(bucket, object_name, connector, event_type)
            elif change_type == 'updated':
                self._handle_object_updated(bucket, object_name, connector, event_type, event_data or {})
            else:
                print(f" Unknown change type: {change_type}")
        
        except Exception as e:
            print(f" Error processing GCS event: {e}")
            import traceback
            traceback.print_exc()
    
    def process_pubsub_message(self, message_data: Dict) -> None:
        try:
            if 'message' in message_data:
                message = message_data['message']
                if 'data' in message:
                    import base64
                    decoded_data = base64.b64decode(message['data']).decode('utf-8')
                    gcs_event = json_module.loads(decoded_data)
                else:
                    gcs_event = message
            elif 'data' in message_data:
                import base64
                decoded_data = base64.b64decode(message_data['data']).decode('utf-8')
                gcs_event = json_module.loads(decoded_data)
            else:
                gcs_event = message_data
            
            bucket = gcs_event.get('bucket')
            object_name = gcs_event.get('name')
            event_type = gcs_event.get('eventType') or gcs_event.get('event_type') or 'OBJECT_FINALIZE'
            
            if not event_type:
                event_type = 'OBJECT_FINALIZE'
            
            if not bucket or not object_name:
                print(f" Invalid GCS event: missing bucket or object name")
                print(f" Event data: {json_module.dumps(gcs_event, indent=2)[:500]}")
                return
            
            connectors = db_helpers.load_connectors()
            print(f" Looking for connector for bucket: {bucket}")
            print(f" Total connectors: {len(connectors)}")
            
            for c in connectors:
                c_type = c.get('type', '')
                c_enabled = c.get('enabled')
                c_buckets = self._get_configured_buckets(c)
                print(f" Connector: {c.get('name')}, Type: {c_type}, Enabled: {c_enabled}, Buckets: {c_buckets}")
            
            gcs_connector = next((
                c for c in connectors 
                if (c.get('type', '').lower() == 'google cloud storage' or c.get('type') == 'Google Cloud Storage')
                and c.get('enabled')
                and bucket in self._get_configured_buckets(c)
            ), None)
            
            if not gcs_connector:
                print(f" No matching GCS connector found for bucket: {bucket}")
                print(f" Available connectors: {[c.get('name') for c in connectors]}")
                return
            
            print(f" Found connector: {gcs_connector.get('name')}")
            
            self.process_event(bucket, object_name, event_type, gcs_connector, gcs_event)
        
        except Exception as e:
            print(f" Error processing GCS Pub/Sub message: {e}")
            import traceback
            traceback.print_exc()
    
    def _get_configured_buckets(self, connector: Dict) -> List[str]:
        config = connector.get('config', {})
        if config is None:
            return []
        if isinstance(config, str):
            try:
                config = json_module.loads(config)
            except (json_module.JSONDecodeError, ValueError):
                return []
        if isinstance(config, dict):
            return config.get('configured_buckets', [])
        return []
    
    def _determine_change_type(self, event_type: str) -> str:
        if not event_type:
            return 'created'
        event_type_upper = event_type.upper()
        
        if event_type_upper == 'OBJECT_FINALIZE':
            return 'created'
        elif event_type_upper == 'OBJECT_DELETE':
            return 'deleted'
        elif event_type_upper == 'OBJECT_METADATA_UPDATE':
            return 'updated'
        elif event_type_upper == 'OBJECT_ARCHIVE':
            return 'created'
        else:
            print(f" Unknown event type: {event_type}, treating as 'created'")
            return 'created'
    
    def _handle_object_created(self, bucket: str, object_name: str, 
                              connector: Dict, event_type: str, event_data: Dict):
        asset_id = f"gs://{bucket}/{object_name}"
        pending_id = f"pending_gcs_{bucket}_{object_name.replace('/', '_').replace(' ', '_')}_{int(datetime.now().timestamp())}"
        
        pending_id = "".join(c for c in pending_id if c.isalnum() or c in ('_', '-', ':'))[:255]
        
        asset_type = 'File'
        if object_name.endswith('/'):
            asset_type = 'Folder'
        elif object_name.lower().endswith(('.csv', '.tsv', '.json', '.parquet', '.avro', '.orc')):
            asset_type = 'Data File'
        elif object_name.lower().endswith(('.sql', '.py', '.scala', '.r')):
            asset_type = 'Script'
        elif object_name.lower().endswith(('.txt', '.log')):
            asset_type = 'Text File'
        elif object_name.lower().endswith(('.zip', '.gz', '.tar', '.bz2')):
            asset_type = 'Archive'
        
        size_bytes = event_data.get('size') or event_data.get('sizeBytes') or 0
        content_type = event_data.get('contentType') or event_data.get('content_type') or ''
        time_created = event_data.get('timeCreated') or event_data.get('time_created') or datetime.now().isoformat()
        
        asset_data = {
            "asset_id": asset_id,
            "id": asset_id,
            "name": object_name.split('/')[-1] if '/' in object_name else object_name,
            "type": asset_type,
            "catalog": bucket,
            "schema": '/'.join(object_name.split('/')[:-1]) if '/' in object_name else '',
            "connector_id": connector['id'],
            "size_bytes": size_bytes,
            "discovered_at": time_created,
            "status": "active",
            "description": f"GCS object in bucket {bucket}",
            "columns": [],
            "technical_metadata": {
                "asset_id": asset_id,
                "asset_type": asset_type,
                "location": asset_id,
                "format": content_type or "Unknown",
                "size_bytes": size_bytes,
                "source_system": "Google Cloud Storage",
                "bucket_name": bucket,
                "object_key": object_name,
                "project_id": connector.get('config', {}).get('project_id'),
                "last_modified": time_created
            },
            "operational_metadata": {
                "status": "active",
                "owner": "Unknown",
                "last_modified": time_created,
                "last_accessed": datetime.now().isoformat(),
                "access_count": "N/A",
                "data_quality_score": 95
            },
            "business_metadata": {
                "description": f"GCS object: {object_name}",
                "business_owner": "Unknown",
                "department": bucket,
                "classification": "internal",
                "sensitivity_level": "low",
                "tags": []
            }
        }
        
        self._save_pending_asset(
            pending_id=pending_id,
            name=asset_data["name"],
            asset_type=asset_type,
            catalog=bucket,
            connector_id=connector['id'],
            change_type="created",
            gcs_event_type=event_type,
            asset_id=asset_id,
            asset_data=asset_data
        )
    
    def _handle_object_deleted(self, bucket: str, object_name: str, 
                               connector: Dict, event_type: str):
        asset_id = f"gs://{bucket}/{object_name}"
        pending_id = f"pending_gcs_deleted_{bucket}_{object_name.replace('/', '_').replace(' ', '_')}_{int(datetime.now().timestamp())}"
        
        pending_id = "".join(c for c in pending_id if c.isalnum() or c in ('_', '-', ':'))[:255]
        
        asset_type = 'File'
        if object_name.endswith('/'):
            asset_type = 'Folder'
        
        self._save_pending_asset(
            pending_id=pending_id,
            name=object_name.split('/')[-1] if '/' in object_name else object_name,
            asset_type=asset_type,
            catalog=bucket,
            connector_id=connector['id'],
            change_type="deleted",
            gcs_event_type=event_type,
            asset_id=asset_id,
            asset_data=None
        )
    
    def _handle_object_updated(self, bucket: str, object_name: str, 
                               connector: Dict, event_type: str, event_data: Dict):
        self._handle_object_created(bucket, object_name, connector, event_type, event_data)
    
    def _save_pending_asset(self, pending_id: str, name: str, asset_type: str, 
                            catalog: str, connector_id: str, change_type: str,
                            gcs_event_type: str, asset_id: str, asset_data: Optional[Dict]):
        try:
            from database import PendingAsset
            
            session = self.Session()
            
            try:
                existing = session.query(PendingAsset).filter(
                    PendingAsset.asset_id == asset_id,
                    PendingAsset.status == 'pending',
                    PendingAsset.connector_id == connector_id
                ).first()
                
                if existing:
                    session.close()
                    print(f" Pending asset already exists: {asset_id}")
                    return
                
                event_type_field = f"gcs:{gcs_event_type}"
                
                pending_asset = PendingAsset(
                    id=pending_id,
                    name=name,
                    type=asset_type,
                    catalog=catalog,
                    connector_id=connector_id,
                    change_type=change_type,
                    s3_event_type=event_type_field,
                    asset_id=asset_id,
                    asset_data=asset_data,
                    status='pending',
                    created_at=datetime.utcnow()
                )
                
                session.add(pending_asset)
                session.commit()
                
                print(f"Saved pending GCS {change_type} event: {asset_id}")
                
                try:
                    global _socketio_instance
                    if _socketio_instance:
                        _socketio_instance.emit('pending_asset_created', {
                            'pending_id': pending_id,
                            'asset_id': asset_id,
                            'name': name,
                            'type': asset_type,
                            'change_type': change_type,
                            'catalog': catalog,
                            'source': 'gcs',
                            'message': f"New GCS {change_type} detected: {asset_id}"
                        }, namespace='/assets', broadcast=True)
                        print(f" Emitted socket.io notification for pending GCS asset: {asset_id}")
                except Exception as e:
                    print(f" Could not emit socket.io notification: {e}")
            
            except Exception as e:
                session.rollback()
                print(f" Error saving pending asset: {e}")
                import traceback
                traceback.print_exc()
            finally:
                session.close()
        
        except Exception as e:
            print(f" Error in _save_pending_asset: {e}")
            import traceback
            traceback.print_exc()

gcs_event_processor = GCSEventProcessor()
