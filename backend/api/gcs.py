from flask import Blueprint, request, jsonify, abort, current_app
from werkzeug.exceptions import HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Callable
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud.eventarc_v1 import EventarcClient
from google.cloud.eventarc_v1.types import Trigger, EventFilter, Destination, CloudRun
from google.oauth2 import service_account
from google.api_core import exceptions as google_exceptions
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import uuid
import db_helpers

gcs_bp = Blueprint('gcs_bp', __name__)

class GCSConnectionTest(BaseModel):
    service_account_json: str
    project_id: str
    bucket_name: Optional[str] = None
    connection_name: str

def discover_gcs_assets(service_account_json: str, project_id: str, bucket_name: Optional[str] = None, progress_callback=None) -> List[Dict[str, Any]]:
    assets = []
    
    try:
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid service account JSON format: {str(e)}")
        
        try:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
        except Exception as e:
            raise Exception(f"Failed to create credentials: {str(e)}")
        
        try:
            client = storage.Client(credentials=credentials, project=project_id)
        except Exception as e:
            raise Exception(f"Failed to create GCS client: {str(e)}")
        
        try:
            if bucket_name:
                bucket = client.bucket(bucket_name)
                bucket.reload()
                buckets = [bucket]
                if progress_callback:
                    progress_callback(f' Found bucket: {bucket_name}')
            else:
                buckets = list(client.list_buckets())
                if not buckets:
                    raise Exception("No buckets found. Please check your service account permissions.")
                if progress_callback:
                    progress_callback(f' Found {len(buckets)} bucket(s)')
        except google_exceptions.Forbidden:
            raise Exception("Access denied. Please check that your service account has 'Storage Object Viewer' or 'Storage Admin' permissions.")
        except google_exceptions.NotFound:
            if bucket_name:
                raise Exception(f"Bucket '{bucket_name}' not found in project '{project_id}'")
            else:
                raise Exception(f"No buckets found in project '{project_id}'")
        except Exception as e:
            raise Exception(f"Failed to list buckets: {str(e)}")
        
        for bucket in buckets:
            bucket_name_actual = bucket.name
            
            if progress_callback:
                progress_callback(f'Discovering objects in bucket: {bucket_name_actual}...')
            
            try:
                blobs = list(bucket.list_blobs())
                
                if progress_callback:
                    progress_callback(f'   Found {len(blobs)} object(s) in bucket {bucket_name_actual}')
                
                if not blobs:
                    if progress_callback:
                        progress_callback(f' Completed bucket {bucket_name_actual}: 0 objects (empty bucket)')
                    continue
                
                def process_blob(blob):
                    try:
                        key = blob.name
                        size = blob.size or 0
                        last_modified = blob.time_created or datetime.now()
                        storage_class = blob.storage_class or 'STANDARD'
                        
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
                            blob.reload()
                            content_type = blob.content_type or ''
                            metadata = blob.metadata or {}
                            etag = blob.etag or ''
                        except Exception:
                            content_type = ''
                            metadata = {}
                            etag = ''
                        
                        asset_id = f"gs://{bucket_name_actual}/{key}"
                        
                        asset = {
                            "id": asset_id,
                            "name": key.split('/')[-1] if '/' in key else key,
                            "type": asset_type,
                            "catalog": bucket_name_actual,
                            "schema": '/'.join(key.split('/')[:-1]) if '/' in key else '',
                            "discovered_at": datetime.now().isoformat(),
                            "status": "active",
                            "description": f"GCS object in bucket {bucket_name_actual}",
                            "size_bytes": size,
                            "columns": [],
                            "technical_metadata": {
                                "asset_id": asset_id,
                                "asset_type": asset_type,
                                "location": f"gs://{bucket_name_actual}/{key}",
                                "format": content_type or "Unknown",
                                "size_bytes": size,
                                "storage_class": storage_class,
                                "source_system": "Google Cloud Storage",
                                "bucket_name": bucket_name_actual,
                                "object_key": key,
                                "project_id": project_id,
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
                                "description": f"GCS object: {key}",
                                "business_owner": "Unknown",
                                "department": bucket_name_actual,
                                "classification": "internal",
                                "sensitivity_level": "low",
                                "tags": list(metadata.keys()) if metadata else []
                            }
                        }
                        
                        return asset
                    except Exception as e:
                        print(f"  Error processing blob {blob.name if hasattr(blob, 'name') else 'Unknown'}: {e}")
                        return None
                
                if progress_callback:
                    progress_callback(f' Processing {len(blobs)} objects in parallel (20 concurrent)...')
                
                bucket_assets = []
                processed_count = 0
                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = [executor.submit(process_blob, blob) for blob in blobs]
                    
                    for future in as_completed(futures):
                        try:
                            asset = future.result()
                            if asset:
                                bucket_assets.append(asset)
                                processed_count += 1
                                
                                if progress_callback and processed_count % 10 == 0:
                                    progress_callback(f'   Processed {processed_count}/{len(blobs)} objects in {bucket_name_actual}...')
                        except Exception as e:
                            print(f"  Error in future result: {e}")
                            continue
                
                assets.extend(bucket_assets)
                
                if progress_callback:
                    progress_callback(f' Completed bucket {bucket_name_actual}: {len(bucket_assets)} objects processed')
                    
            except google_exceptions.Forbidden:
                if progress_callback:
                    progress_callback(f'  Access denied to bucket {bucket_name_actual}')
                print(f"  Access denied to bucket {bucket_name_actual}")
                continue
            except Exception as e:
                if progress_callback:
                    progress_callback(f'  Error listing objects in bucket {bucket_name_actual}: {str(e)}')
                print(f"  Error listing objects in bucket {bucket_name_actual}: {e}")
                continue
    
    except google_exceptions.GoogleAPIError as e:
        raise Exception(f"GCS API error: {str(e)}")
    except Exception as e:
        raise Exception(f"Error discovering GCS assets: {str(e)}")
    
    return assets

@gcs_bp.route('/buckets', methods=['GET'])
def list_gcs_buckets():
    try:
        connector_id = request.args.get('connector_id')
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Google Cloud Storage'), None)
        if not connector:
            return jsonify({'error': 'GCS connector not found'}), 404
        
        config = connector.get('config', {})
        service_account_json = config.get('service_account_json') or config.get('serviceAccount')
        project_id = config.get('project_id') or config.get('projectId')
        
        if not service_account_json or not project_id:
            return jsonify({'error': 'Connector missing GCS credentials'}), 400
        
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError:
            return jsonify({'error': 'Invalid service account JSON'}), 400
        
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        client = storage.Client(credentials=credentials, project=project_id)
        buckets = [bucket.name for bucket in client.list_buckets()]
        return jsonify({'buckets': buckets}), 200
    except google_exceptions.GoogleAPIError as e:
        return jsonify({'error': f'GCS API error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def setup_gcs_event_notifications(service_account_json: str, project_id: str, 
                                   bucket_names: List[str], webhook_url: Optional[str] = None) -> Dict[str, Any]:
    try:
        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        storage_client = storage.Client(credentials=credentials, project=project_id)
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        
        topic_name = f"torro-gcs-events-{project_id}-{uuid.uuid4().hex[:8]}"
        topic_path = publisher.topic_path(project_id, topic_name)
        
        try:
            topic = publisher.create_topic(request={"name": topic_path})
            print(f" Created Pub/Sub topic: {topic_name}")
        except google_exceptions.AlreadyExists:
            print(f" Using existing Pub/Sub topic: {topic_name}")
        except Exception as e:
            print(f" Error creating Pub/Sub topic: {e}")
            return {
                'success': False,
                'error': f'Failed to create Pub/Sub topic: {str(e)}'
            }
        
        if not webhook_url:
            webhook_url = os.environ.get('GCS_WEBHOOK_URL')
            
            if not webhook_url:
                try:
                    import urllib.request
                    ngrok_response = urllib.request.urlopen('http://localhost:4040/api/tunnels', timeout=2)
                    ngrok_data = json.loads(ngrok_response.read())
                    if ngrok_data.get('tunnels'):
                        https_tunnel = next((t for t in ngrok_data['tunnels'] if t.get('proto') == 'https'), None)
                        if https_tunnel:
                            webhook_url = f"{https_tunnel['public_url']}/api/gcs/pubsub-webhook"
                            print(f" Detected ngrok HTTPS URL: {webhook_url}")
                        else:
                            http_tunnel = ngrok_data['tunnels'][0]
                            webhook_url = f"{http_tunnel['public_url']}/api/gcs/pubsub-webhook"
                            print(f" Detected ngrok HTTP URL: {webhook_url}")
                            print(f"     Note: Pub/Sub prefers HTTPS, but HTTP will work")
                except Exception as ngrok_err:
                    print(f" ngrok is NOT running!")
                    print(f"   Please start ngrok: ngrok http 8099")
                    print(f"   Or set GCS_WEBHOOK_URL environment variable")
                    return {
                        'success': False,
                        'error': 'ngrok not running. Please start ngrok (ngrok http 8099) or set GCS_WEBHOOK_URL environment variable.'
                    }
        
        if not webhook_url:
            return {
                'success': False,
                'error': 'No webhook URL available. Start ngrok or set GCS_WEBHOOK_URL.'
            }
        
        subscription_name = f"torro-gcs-webhook-{uuid.uuid4().hex[:8]}"
        subscription_path = subscriber.subscription_path(project_id, subscription_name)
        
        try:
            push_config = pubsub_v1.types.PushConfig(
                push_endpoint=webhook_url,
                attributes={
                    'x-goog-version': 'v1'
                }
            )
            
            subscription = subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "push_config": push_config,
                    "ack_deadline_seconds": 60
                }
            )
            print(f" Created Push Subscription: {subscription_name}")
            print(f"   Webhook URL: {webhook_url}")
        except google_exceptions.AlreadyExists:
            print(f" Using existing subscription: {subscription_name}")
        except Exception as e:
            print(f" Error creating subscription: {e}")
            return {
                'success': False,
                'error': f'Failed to create subscription: {str(e)}'
            }
        
        configured_buckets = []
        for bucket_name in bucket_names:
            try:
                bucket = storage_client.bucket(bucket_name)
                
                notification = bucket.notification(
                    topic_name=topic_name,
                    topic_project=project_id,
                    event_types=[
                        'OBJECT_FINALIZE',
                        'OBJECT_METADATA_UPDATE',
                        'OBJECT_DELETE',
                        'OBJECT_ARCHIVE',
                    ],
                    payload_format='JSON_API_V1'
                )
                
                notification.create()
                configured_buckets.append(bucket_name)
                print(f" Configured notifications for bucket: {bucket_name}")
                
            except google_exceptions.Conflict:
                print(f"  Notification already exists for {bucket_name}, updating...")
                try:
                    notifications = bucket.list_notifications()
                    for notif in notifications:
                        if notif.topic_name == topic_name:
                            notif.delete()
                    notification = bucket.notification(
                        topic_name=topic_name,
                        topic_project=project_id,
                        event_types=[
                            'OBJECT_FINALIZE',
                            'OBJECT_METADATA_UPDATE',
                            'OBJECT_DELETE',
                            'OBJECT_ARCHIVE',
                        ],
                        payload_format='JSON_API_V1'
                    )
                    notification.create()
                    configured_buckets.append(bucket_name)
                    print(f" Updated notifications for bucket: {bucket_name}")
                except Exception as update_err:
                    print(f"  Error updating notification for {bucket_name}: {update_err}")
                    continue
            except Exception as e:
                print(f"  Error configuring bucket {bucket_name}: {e}")
                continue
        
        result = {
            'success': True,
            'pubsub_topic_name': topic_name,
            'pubsub_topic_path': topic_path,
            'subscription_name': subscription_name,
            'subscription_path': subscription_path,
            'webhook_url': webhook_url,
            'configured_buckets': configured_buckets
        }
        
        if configured_buckets:
            print(f" Successfully configured event notifications for {len(configured_buckets)} bucket(s): {', '.join(configured_buckets)}")
        else:
            print(f"  Warning: No buckets were configured for event notifications")
            
        return result
        
    except Exception as e:
        print(f" Error setting up GCS event notifications: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

@gcs_bp.route('/setup-events', methods=['POST'])
def setup_gcs_events():
    try:
        data = request.get_json()
        connector_id = data.get('connector_id')
        
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Google Cloud Storage'), None)
        
        if not connector:
            return jsonify({'error': 'GCS connector not found'}), 404
        
        config = connector.get('config', {})
        service_account_json = config.get('service_account_json') or config.get('serviceAccount')
        project_id = config.get('project_id') or config.get('projectId')
        bucket_name = config.get('bucket_name') or config.get('bucketName')
        
        if not service_account_json or not project_id:
            return jsonify({'error': 'Connector missing GCS credentials'}), 400
        
        if bucket_name:
            bucket_names = [bucket_name]
        else:
            try:
                service_account_info = json.loads(service_account_json)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                storage_client = storage.Client(credentials=credentials, project=project_id)
                bucket_names = [bucket.name for bucket in storage_client.list_buckets()]
            except Exception as e:
                return jsonify({'error': f'Failed to list buckets: {str(e)}'}), 500
        
        result = setup_gcs_event_notifications(
            service_account_json, project_id, bucket_names
        )
        
        if result['success']:
            config['pubsub_topic_name'] = result.get('pubsub_topic_name')
            config['pubsub_topic_path'] = result.get('pubsub_topic_path')
            config['subscription_name'] = result.get('subscription_name')
            config['subscription_path'] = result.get('subscription_path')
            config['webhook_url'] = result.get('webhook_url')
            config['configured_buckets'] = result.get('configured_buckets', [])
            
            connector['config'] = config
            db_helpers.save_connector(connector)
            
            return jsonify(result), 200
        else:
            return jsonify(result), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@gcs_bp.route('/pubsub-webhook', methods=['POST', 'GET'])
def pubsub_webhook():
    try:
        if request.method == 'GET':
            challenge = request.args.get('challenge')
            if challenge:
                print(f" Pub/Sub subscription verification challenge received")
                return challenge, 200
            return '', 200
        
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data received'}), 400
        
        print(f" Received Pub/Sub webhook: {json.dumps(data, indent=2)[:500]}...")
        
        try:
            from services.gcs_event_processor import gcs_event_processor
            print(f" Calling gcs_event_processor.process_pubsub_message...")
            gcs_event_processor.process_pubsub_message(data)
            print(f" Event processor completed")
        except Exception as e:
            print(f" Error in event processor: {e}")
            import traceback
            traceback.print_exc()
        
        return '', 200
        
    except Exception as e:
        print(f" Error in Pub/Sub webhook: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@gcs_bp.route('/events', methods=['POST'])
def handle_gcs_event():
    try:
        
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No data received'}), 400
        
        bucket = data.get('bucket')
        object_name = data.get('object') or data.get('object_name')
        event_type = data.get('event_type') or data.get('eventType')
        event_data = data.get('event_data') or data
        
        if not bucket or not object_name:
            return jsonify({'error': 'Missing bucket or object name'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((
            c for c in connectors 
            if (c.get('type', '').lower() == 'google cloud storage' or c.get('type') == 'Google Cloud Storage')
            and c.get('enabled')
            and bucket in c.get('config', {}).get('configured_buckets', [])
        ), None)
        
        if not connector:
            print(f"  No matching GCS connector found for bucket: {bucket}")
            return jsonify({'error': 'No matching connector'}), 404
        
        from services.gcs_event_processor import gcs_event_processor
        gcs_event_processor.process_event(
            bucket=bucket,
            object_name=object_name,
            event_type=event_type or 'OBJECT_FINALIZE',
            connector=connector,
            event_data=event_data
        )
        
        return jsonify({'status': 'processed'}), 200
        
    except Exception as e:
        print(f" Error handling GCS event: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

def setup_eventarc_trigger(service_account_json: str, project_id: str, region: str,
                           topic_name: str, cloud_run_service: str, 
                           service_account_email: Optional[str] = None) -> Dict[str, Any]:
    try:
        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        if not service_account_email:
            service_account_email = service_account_info.get('client_email')
        
        if not service_account_email:
            return {'success': False, 'error': 'Service account email not found'}
        
        client = EventarcClient(credentials=credentials)
        parent = f"projects/{project_id}/locations/{region}"
        trigger_id = f"torro-gcs-trigger-{uuid.uuid4().hex[:8]}"
        trigger_name = f"{parent}/triggers/{trigger_id}"
        
        event_filters = [
            EventFilter(attribute="type", value="google.cloud.pubsub.topic.v1.messagePublished"),
            EventFilter(attribute="pubsubtopic", value=f"projects/{project_id}/topics/{topic_name}")
        ]
        
        destination = Destination(
            cloud_run=CloudRun(service=cloud_run_service, region=region, path="/process-gcs-event")
        )
        
        trigger = Trigger(
            name=trigger_name,
            event_filters=event_filters,
            destination=destination,
            service_account=service_account_email
        )
        
        try:
            operation = client.create_trigger(parent=parent, trigger=trigger, trigger_id=trigger_id)
            print(f" Created Eventarc trigger: {trigger_id}")
            return {
                'success': True,
                'trigger_id': trigger_id,
                'trigger_name': trigger_name,
                'cloud_run_service': cloud_run_service,
                'region': region,
                'service_account': service_account_email
            }
        except google_exceptions.AlreadyExists:
            return {
                'success': True,
                'trigger_id': trigger_id,
                'trigger_name': trigger_name,
                'message': 'Trigger already exists'
            }
        except Exception as e:
            return {'success': False, 'error': f'Failed to create Eventarc trigger: {str(e)}'}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def setup_production_gcs_events(service_account_json: str, project_id: str, region: str,
                                bucket_names: List[str], cloud_run_service: str,
                                torro_api_url: str) -> Dict[str, Any]:
    try:
        pubsub_result = setup_gcs_event_notifications(service_account_json, project_id, bucket_names)
        if not pubsub_result['success']:
            return pubsub_result
        
        topic_name = pubsub_result['pubsub_topic_name']
        eventarc_result = setup_eventarc_trigger(service_account_json, project_id, region, topic_name, cloud_run_service)
        
        if not eventarc_result['success']:
            return {
                'success': False,
                'error': f"Pub/Sub setup succeeded but Eventarc failed: {eventarc_result.get('error')}",
                'pubsub_config': pubsub_result
            }
        
        return {
            'success': True,
            'pubsub_topic_name': topic_name,
            'eventarc_trigger_id': eventarc_result.get('trigger_id'),
            'cloud_run_service': cloud_run_service,
            'region': region,
            'configured_buckets': pubsub_result.get('configured_buckets', []),
            'torro_api_url': torro_api_url
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}

@gcs_bp.route('/setup-production-events', methods=['POST'])
def setup_production_events():
    try:
        data = request.get_json()
        connector_id = data.get('connector_id')
        region = data.get('region', 'us-central1')
        cloud_run_service = data.get('cloud_run_service')
        torro_api_url = data.get('torro_api_url', 'http://localhost:8099')
        
        if not connector_id or not cloud_run_service:
            return jsonify({'error': 'connector_id and cloud_run_service are required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Google Cloud Storage'), None)
        if not connector:
            return jsonify({'error': 'GCS connector not found'}), 404
        
        config = connector.get('config', {})
        service_account_json = config.get('service_account_json') or config.get('serviceAccount')
        project_id = config.get('project_id') or config.get('projectId')
        bucket_name = config.get('bucket_name') or config.get('bucketName')
        
        if not service_account_json or not project_id:
            return jsonify({'error': 'Connector missing GCS credentials'}), 400
        
        bucket_names = [bucket_name] if bucket_name else []
        if not bucket_names:
            try:
                service_account_info = json.loads(service_account_json)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info, scopes=["https://www.googleapis.com/auth/cloud-platform"])
                storage_client = storage.Client(credentials=credentials, project=project_id)
                bucket_names = [bucket.name for bucket in storage_client.list_buckets()]
            except Exception as e:
                return jsonify({'error': f'Failed to list buckets: {str(e)}'}), 500
        
        result = setup_production_gcs_events(service_account_json, project_id, region, bucket_names, cloud_run_service, torro_api_url)
        
        if result['success']:
            config.update({
                'pubsub_topic_name': result.get('pubsub_topic_name'),
                'eventarc_trigger_id': result.get('eventarc_trigger_id'),
                'cloud_run_service': cloud_run_service,
                'region': region,
                'torro_api_url': torro_api_url,
                'configured_buckets': result.get('configured_buckets', []),
                'production_mode': True
            })
            connector['config'] = config
            db_helpers.save_connector(connector)
            return jsonify(result), 200
        else:
            return jsonify(result), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500
