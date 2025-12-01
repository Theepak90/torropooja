import boto3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
import db_helpers
import threading
from urllib.parse import unquote

_socketio_instance = None

def set_socketio(socketio):
    global _socketio_instance
    _socketio_instance = socketio

class S3EventProcessor:
    def __init__(self):
        self.running = False
        self.thread = None
    
    def start(self):
        if self.running:
            print(" S3 Event Processor already running")
            return
        
        self.running = True
        print(" S3 Event Processor started - ready to receive events via HTTP webhook")
    
    def stop(self):
        self.running = False
        print(" S3 Event Processor stopped")
    
    def _poll_sqs_loop(self):
        while self.running:
            try:
                self._process_all_s3_connectors()
                threading.Event().wait(5)
            except Exception as e:
                print(f" Error in S3 Event Processor loop: {e}")
                import traceback
                traceback.print_exc()
                threading.Event().wait(10)
    
    def _process_all_s3_connectors(self):
        connectors = db_helpers.load_connectors()
        s3_connectors = [c for c in connectors if c.get('type') == 'Amazon S3' and c.get('enabled')]
        
        if not s3_connectors:
            import time
            if not hasattr(self, '_last_no_connectors_log') or time.time() - self._last_no_connectors_log > 60:
                self._last_no_connectors_log = time.time()
            return
        
        import time
        if not hasattr(self, '_poll_count'):
            self._poll_count = 0
        self._poll_count += 1
        if self._poll_count % 10 == 0:
            print(f" Polling SQS for {len(s3_connectors)} S3 connector(s)... (poll #{self._poll_count})")
        
        for connector in s3_connectors:
            try:
                self._process_connector_events(connector)
            except Exception as e:
                print(f" Error processing events for connector {connector.get('name')}: {e}")
                import traceback
                traceback.print_exc()
    
    def _process_connector_events(self, connector: Dict[str, Any]):
        config = connector.get('config', {})
        queue_url = config.get('sqs_queue_url')
        
        if not queue_url:
            import time
            if not hasattr(self, '_last_no_queue_log') or time.time() - self._last_no_queue_log > 60:
                self._last_no_queue_log = time.time()
            return
        
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        
        if not access_key_id or not secret_access_key:
            return
        
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        try:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            if not messages:
                try:
                    queue_attrs = sqs_client.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed']
                    )
                    visible = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessages', '0'))
                    in_flight = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', '0'))
                    delayed = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesDelayed', '0'))
                    
                    import time
                    if not hasattr(self, '_last_queue_check_log'):
                        self._last_queue_check_log = {}
                    connector_name = connector.get('name')
                    last_log_time = self._last_queue_check_log.get(connector_name, 0)
                    
                    if visible > 0 or in_flight > 0 or delayed > 0 or (time.time() - last_log_time > 30):
                        if visible > 0 or in_flight > 0:
                            print(f" Queue has {visible} visible, {in_flight} in-flight messages but receive_message returned none!")
                            print(f" This might indicate a permissions issue or queue visibility timeout problem")
                        else:
                            print(f" Polling SQS for {connector_name} - queue empty (visible: {visible}, in-flight: {in_flight}, delayed: {delayed})")
                        self._last_queue_check_log[connector_name] = time.time()
                except Exception as e:
                    import time
                    if not hasattr(self, '_last_error_log'):
                        self._last_error_log = 0
                    if time.time() - self._last_error_log > 60:
                        print(f" Error checking queue status: {e}")
                        self._last_error_log = time.time()
                return
            
            print(f" Received {len(messages)} S3 event(s) for connector {connector.get('name')}")
            
            for message in messages:
                try:
                    self._process_s3_event(message, connector, sqs_client, queue_url)
                except Exception as e:
                    print(f" Error processing S3 event: {e}")
                    import traceback
                    traceback.print_exc()
                    try:
                        sqs_client.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        print(f" Deleted message after processing error to prevent stuck messages")
                    except Exception as delete_err:
                        print(f" Could not delete message after error: {delete_err}")
        
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            
            if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                print(f" SQS queue not found for connector {connector.get('name')}")
            elif error_code == 'AccessDenied':
                import time
                if not hasattr(self, '_last_permission_error_log') or time.time() - self._last_permission_error_log > 300:
                    print(f" SQS Permission Error for connector {connector.get('name')}:")
                    print(f" Missing permission: sqs:ReceiveMessage")
                    print(f" Queue: {queue_url}")
                    print(f" Error: {error_message}")
                    print(f" Add sqs:ReceiveMessage permission to your AWS IAM user/role")
                    print(f" Also ensure you have: sqs:DeleteMessage, sqs:GetQueueAttributes")
                    self._last_permission_error_log = time.time()
            else:
                print(f" SQS error: {e}")
    
    def _process_s3_event(self, message: Dict, connector: Dict, sqs_client, queue_url: str):
        try:
            body = json.loads(message['Body'])
            print(f" Processing S3 event message: {json.dumps(body, indent=2)[:500]}...")
        except json.JSONDecodeError as e:
            print(f" Error parsing SQS message body: {e}")
            print(f" Message body: {message.get('Body', '')[:200]}")
            return
        
        records = []
        if 'TopicArn' in body or 'Type' in body:
            message_str = body.get('Message', '')
            if message_str:
                try:
                    sns_message = json.loads(message_str)
                    
                    if sns_message.get('Event') == 's3:TestEvent':
                        print(f" SNS Test Event (ignoring): {sns_message.get('Bucket')}")
                        try:
                            sqs_client.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            print(f" Deleted test event message")
                        except Exception as e:
                            print(f" Could not delete test event: {e}")
                        return
                    
                    records = sns_message.get('Records', [])
                    if records:
                        print(f" SNS-wrapped event with {len(records)} record(s)")
                    else:
                        print(f" SNS message parsed but no Records found")
                        print(f" Message keys: {list(sns_message.keys())}")
                        print(f" Message preview: {str(sns_message)[:200]}")
                
                except json.JSONDecodeError as e:
                    print(f" Error parsing SNS Message field as JSON: {e}")
                    print(f" Message content (first 300 chars): {message_str[:300]}")
                    try:
                        decoded = json.loads(message_str)
                        if isinstance(decoded, str):
                            decoded = json.loads(decoded)
                        records = decoded.get('Records', [])
                        if records:
                            print(f" Parsed after double-decode: {len(records)} record(s)")
                        else:
                            print(f" Parsed but no Records found")
                    except Exception as decode_err:
                        print(f" Still failed to parse: {decode_err}")
                        records = []
                except (KeyError, TypeError) as e:
                    print(f" Error accessing Records in SNS message: {e}")
                    print(f" Message type: {type(sns_message)}")
                    if isinstance(sns_message, dict):
                        print(f" Message keys: {list(sns_message.keys())}")
                    records = []
            else:
                print(f" SNS message has no 'Message' field")
                print(f" Body keys: {list(body.keys())}")
        else:
            records = body.get('Records', [])
            print(f" Direct SQS event with {len(records)} record(s)")
        
        if not records:
            print(f" No records found in S3 event message")
            print(f" Body keys: {list(body.keys())}")
            try:
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f" Deleted message with no records")
            except Exception as e:
                print(f" Error deleting message: {e}")
            return
        
        for record in records:
            event_name = record.get('eventName', '')
            s3_data = record.get('s3', {})
            bucket_name = s3_data.get('bucket', {}).get('name', '')
            object_key = s3_data.get('object', {}).get('key', '')
            
            object_key = unquote(object_key) if object_key else ''
            
            if event_name.startswith('s3:ObjectCreated'):
                self._handle_object_created(bucket_name, object_key, connector, event_name, s3_data)
            elif event_name.startswith('s3:ObjectRemoved'):
                self._handle_object_removed(bucket_name, object_key, connector, event_name)
            elif 'BucketCreated' in event_name:
                self._handle_bucket_created(bucket_name, connector, event_name)
        
        try:
            sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        except Exception as e:
            print(f" Error deleting SQS message: {e}")
    
    def _handle_object_created(self, bucket_name: str, object_key: str, connector: Dict, 
                               event_name: str, s3_data: Dict):
        asset_id = f"s3://{bucket_name}/{object_key}"
        pending_id = f"pending_{bucket_name}_{object_key.replace('/', '_').replace(' ', '_')}_{int(datetime.now().timestamp())}"
        
        pending_id = "".join(c for c in pending_id if c.isalnum() or c in ('_', '-', ':'))[:255]
        
        asset_type = 'File'
        if object_key.endswith('/'):
            asset_type = 'Folder'
        elif object_key.lower().endswith(('.csv', '.tsv', '.json', '.parquet', '.avro', '.orc')):
            asset_type = 'Data File'
        
        asset_data = {
            "asset_id": asset_id,
            "name": object_key.split('/')[-1] if '/' in object_key else object_key,
            "type": asset_type,
            "catalog": bucket_name,
            "schema": '/'.join(object_key.split('/')[:-1]) if '/' in object_key else '',
            "connector_id": connector['id'],
            "technical_metadata": {
                "asset_type": asset_type,
                "location": asset_id,
                "source_system": "Amazon S3",
                "bucket_name": bucket_name,
                "object_key": object_key,
            }
        }
        
        self._save_pending_asset(
            pending_id=pending_id,
            name=asset_data["name"],
            asset_type=asset_type,
            catalog=bucket_name,
            connector_id=connector['id'],
            change_type="created",
            s3_event_type=event_name,
            asset_id=asset_id,
            asset_data=asset_data
        )
    
    def _handle_object_removed(self, bucket_name: str, object_key: str, connector: Dict, event_name: str):
        asset_id = f"s3://{bucket_name}/{object_key}"
        pending_id = f"pending_deleted_{bucket_name}_{object_key.replace('/', '_').replace(' ', '_')}_{int(datetime.now().timestamp())}"
        
        pending_id = "".join(c for c in pending_id if c.isalnum() or c in ('_', '-', ':'))[:255]
        
        asset_type = 'File'
        if object_key.endswith('/'):
            asset_type = 'Folder'
        
        self._save_pending_asset(
            pending_id=pending_id,
            name=object_key.split('/')[-1] if '/' in object_key else object_key,
            asset_type=asset_type,
            catalog=bucket_name,
            connector_id=connector['id'],
            change_type="deleted",
            s3_event_type=event_name,
            asset_id=asset_id,
            asset_data=None
        )
    
    def _handle_s3_event_direct(self, bucket_name: str, object_key: str, event_name: str, 
                                connector: Dict, change_type: str, s3_data: Optional[Dict] = None):
        print(f" _handle_s3_event_direct called:")
        print(f" Bucket: {bucket_name}")
        print(f" Object Key: {object_key}")
        print(f" Event Name: {event_name}")
        print(f" Change Type: {change_type}")
        
        if not change_type or change_type == 'unknown':
            if event_name.startswith('s3:ObjectCreated') or event_name.startswith('ObjectCreated'):
                change_type = 'created'
            elif event_name.startswith('s3:ObjectRemoved') or event_name.startswith('ObjectRemoved'):
                change_type = 'deleted'
            elif event_name.startswith('s3:ObjectRestore'):
                if 'Delete' in event_name:
                    change_type = 'deleted'
                else:
                    change_type = 'created'
            elif event_name.startswith('s3:ObjectTagging'):
                if 'Delete' in event_name:
                    change_type = 'deleted'
                else:
                    change_type = 'created'
            elif event_name.startswith('s3:LifecycleExpiration'):
                change_type = 'deleted'
            elif event_name.startswith('s3:ObjectAcl') or event_name.startswith('s3:ObjectLock'):
                change_type = 'created'
            elif event_name.startswith('s3:Replication') or event_name.startswith('s3:LifecycleTransition') or \
                 event_name.startswith('s3:IntelligentTiering') or 'ReducedRedundancyLostObject' in event_name:
                change_type = 'created'
            else:
                change_type = 'created'
                print(f" Unknown event type: {event_name}, treating as 'created'")
        
        if change_type == 'created':
            self._handle_object_created(bucket_name, object_key, connector, event_name, s3_data or {})
        elif change_type == 'deleted':
            self._handle_object_removed(bucket_name, object_key, connector, event_name)
        else:
            print(f" Unknown change_type: {change_type}, defaulting to created")
            self._handle_object_created(bucket_name, object_key, connector, event_name, s3_data or {})
    
    def _handle_bucket_created(self, bucket_name: str, connector: Dict, event_name: str):
        asset_id = f"s3://{bucket_name}/"
        pending_id = f"pending_bucket_{bucket_name}_{int(datetime.now().timestamp())}"
        
        pending_id = "".join(c for c in pending_id if c.isalnum() or c in ('_', '-', ':'))[:255]
        
        asset_data = {
            "asset_id": asset_id,
            "name": bucket_name,
            "type": "Bucket",
            "catalog": bucket_name,
            "schema": "",
            "connector_id": connector['id'],
            "technical_metadata": {
                "asset_type": "Bucket",
                "location": asset_id,
                "source_system": "Amazon S3",
                "bucket_name": bucket_name,
            }
        }
        
        self._save_pending_asset(
            pending_id=pending_id,
            name=bucket_name,
            asset_type="Bucket",
            catalog=bucket_name,
            connector_id=connector['id'],
            change_type="created",
            s3_event_type=event_name,
            asset_id=asset_id,
            asset_data=asset_data
        )
    
    def _save_pending_asset(self, pending_id: str, name: str, asset_type: str, catalog: str,
                            connector_id: str, change_type: str, s3_event_type: str,
                            asset_id: str, asset_data: Optional[Dict]):
        try:
            from database import PendingAsset
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from config import Config
            
            engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            try:
                existing = session.query(PendingAsset).filter(
                    PendingAsset.asset_id == asset_id,
                    PendingAsset.status == 'pending'
                ).first()
                
                if existing:
                    session.close()
                    return
                
                pending_asset = PendingAsset(
                    id=pending_id,
                    name=name,
                    type=asset_type,
                    catalog=catalog,
                    connector_id=connector_id,
                    change_type=change_type,
                    s3_event_type=s3_event_type,
                    asset_id=asset_id,
                    asset_data=asset_data,
                    status='pending',
                    created_at=datetime.utcnow()
                )
                
                session.add(pending_asset)
                session.commit()
                
                print(f"Saved pending {change_type} event: {asset_id}")
                
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
                            'message': f"New S3 {change_type} detected: {asset_id}"
                        }, namespace='/assets', broadcast=True)
                        print(f" Emitted socket.io notification for pending asset: {asset_id}")
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

s3_event_processor = S3EventProcessor()
