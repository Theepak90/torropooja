import boto3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
import db_helpers
import threading
from urllib.parse import unquote
from database import PendingAsset
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config

class S3EventBridgeProcessor:
    
    def __init__(self):
        self.running = False
        self.thread = None
        self.engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
        self.Session = sessionmaker(bind=self.engine)
        
    def start(self):
        if self.running:
            print("  S3 EventBridge Processor already running")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._poll_eventbridge_loop, daemon=True)
        self.thread.start()
        print(" S3 EventBridge Processor started - polling EventBridge for real-time changes")
        
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print(" S3 EventBridge Processor stopped")
        
    def _poll_eventbridge_loop(self):
        while self.running:
            try:
                self._process_all_s3_connectors()
                threading.Event().wait(10)
            except Exception as e:
                print(f" Error in EventBridge Processor loop: {e}")
                import traceback
                traceback.print_exc()
                threading.Event().wait(20)
                
    def _process_all_s3_connectors(self):
        connectors = db_helpers.load_connectors()
        s3_connectors = [c for c in connectors if c.get('type') == 'Amazon S3' and c.get('enabled')]
        
        if not s3_connectors:
            return
        
        for connector in s3_connectors:
            try:
                self._process_connector_events(connector)
            except Exception as e:
                print(f" Error processing EventBridge events for connector {connector.get('name')}: {e}")
                import traceback
                traceback.print_exc()
                
    def _process_connector_events(self, connector: Dict[str, Any]):
        config = connector.get('config', {})
        event_bus_name = config.get('eventbridge_bus_name', 'default')
        
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        
        if not access_key_id or not secret_access_key:
            return
            
        events_client = boto3.client(
            'events',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        try:
            end_time = datetime.utcnow()
            start_time = datetime.utcnow().replace(second=0, microsecond=0)

            pass
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ResourceNotFoundException':
                print(f"  EventBridge bus not found: {event_bus_name}")
            else:
                print(f" EventBridge error: {e}")

s3_eventbridge_processor = S3EventBridgeProcessor()

