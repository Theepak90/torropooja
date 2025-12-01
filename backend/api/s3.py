from flask import Blueprint, request, jsonify, abort, current_app
from werkzeug.exceptions import HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime
import json
import os
import requests
import db_helpers

s3_bp = Blueprint('s3_bp', __name__)

class S3ConnectionTest(BaseModel):
    access_key_id: str
    secret_access_key: str
    region: str
    bucket_name: Optional[str] = None
    connection_name: str

def discover_s3_assets(access_key_id: str, secret_access_key: str, region: str, bucket_name: Optional[str] = None) -> List[Dict[str, Any]]:
    assets = []
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        if bucket_name:
            buckets = [{'Name': bucket_name}]
        else:
            response = s3_client.list_buckets()
            buckets = response.get('Buckets', [])
        
        for bucket_info in buckets:
            bucket_name_actual = bucket_info['Name']
            
            try:
                paginator = s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name_actual)
                
                for page in pages:
                    if 'Contents' not in page:
                        continue
                    
                    for obj in page['Contents']:
                        key = obj['Key']
                        size = obj.get('Size', 0)
                        last_modified = obj.get('LastModified', datetime.now())
                        storage_class = obj.get('StorageClass', 'STANDARD')
                        
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
                            metadata_response = s3_client.head_object(Bucket=bucket_name_actual, Key=key)
                            content_type = metadata_response.get('ContentType', '')
                            metadata = metadata_response.get('Metadata', {})
                            etag = metadata_response.get('ETag', '').strip('"')
                        except ClientError:
                            content_type = ''
                            metadata = {}
                            etag = ''
                        
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
                            "columns": [],
                            "technical_metadata": {
                                "asset_id": asset_id,
                                "asset_type": asset_type,
                                "location": f"s3://{bucket_name_actual}/{key}",
                                "format": content_type or "Unknown",
                                "size_bytes": size,
                                "storage_class": storage_class,
                                "source_system": "Amazon S3",
                                "bucket_name": bucket_name_actual,
                                "object_key": key,
                                "region": region,
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
                                "description": f"S3 object: {key}",
                                "business_owner": "Unknown",
                                "department": bucket_name_actual,
                                "classification": "internal",
                                "sensitivity_level": "low",
                                "tags": list(metadata.keys()) if metadata else []
                            }
                        }
                        
                        assets.append(asset)
                        
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'AccessDenied':
                    print(f"  Access denied to bucket {bucket_name_actual}: {e}")
                    continue
                else:
                    print(f"  Error listing objects in bucket {bucket_name_actual}: {e}")
                    continue
    
    except NoCredentialsError:
        raise Exception("AWS credentials not found or invalid")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'InvalidAccessKeyId':
            raise Exception("Invalid AWS Access Key ID")
        elif error_code == 'SignatureDoesNotMatch':
            raise Exception("Invalid AWS Secret Access Key")
        else:
            raise Exception(f"AWS API error: {str(e)}")
    except Exception as e:
        raise Exception(f"Error discovering S3 assets: {str(e)}")
    
    return assets

def trigger_airflow_dag(dag_id: str = 's3_asset_discovery') -> Dict[str, Any]:
    try:
        airflow_url = os.environ.get('AIRFLOW_URL', 'http://localhost:8080')
        airflow_username = os.environ.get('AIRFLOW_USERNAME', 'airflow')
        airflow_password = os.environ.get('AIRFLOW_PASSWORD', 'airflow')
        
        trigger_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        
        from datetime import datetime
        payload = {
            "dag_run_id": f"manual_trigger_{int(datetime.now().timestamp())}",
            "conf": {},
            "note": "Triggered automatically when S3 connector was created/enabled"
        }
        
        response = requests.post(
            trigger_url,
            json=payload,
            auth=(airflow_username, airflow_password),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            print(f"Successfully triggered Airflow DAG: {dag_id}")
            return {
                'success': True,
                'message': f'Airflow DAG {dag_id} triggered successfully',
                'dag_run_id': response.json().get('dag_run_id')
            }
        else:
            print(f"Failed to trigger Airflow DAG: HTTP {response.status_code} - {response.text}")
            return {
                'success': False,
                'message': f'Failed to trigger DAG: HTTP {response.status_code}',
                'error': response.text
            }
            
    except requests.exceptions.ConnectionError:
        airflow_url = os.environ.get('AIRFLOW_URL', 'http://localhost:8080')
        print(f"Could not connect to Airflow at {airflow_url}. DAG will run on schedule.")
        return {
            'success': False,
            'message': 'Airflow not accessible, DAG will run on schedule',
            'error': 'Connection refused'
        }
    except Exception as e:
        print(f"Error triggering Airflow DAG: {str(e)}")
        return {
            'success': False,
            'message': f'Error triggering DAG: {str(e)}',
            'error': str(e)
        }

def setup_s3_event_notifications(access_key_id: str, secret_access_key: str, 
                                 region: str, bucket_names: List[str]) -> Dict[str, Any]:
    import uuid
    from botocore.exceptions import ClientError
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        sns_client = boto3.client(
            'sns',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        sqs_client = boto3.client(
            'sqs',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        topic_name = f"torro-s3-events-{uuid.uuid4().hex[:8]}"
        try:
            topic_response = sns_client.create_topic(Name=topic_name)
            topic_arn = topic_response['TopicArn']
            print(f" Created SNS topic: {topic_name}")
        except ClientError as e:
            print(f"  Could not create SNS topic: {e}")
            return {
                'success': False,
                'error': f'Failed to create SNS topic: {str(e)}'
            }
        
        try:
            sns_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "SNS:Publish",
                    "Resource": topic_arn,
                    "Condition": {
                        "StringLike": {
                            "aws:SourceArn": f"arn:aws:s3:*:*:*"
                        }
                    }
                }]
            }
            
            sns_client.set_topic_attributes(
                TopicArn=topic_arn,
                AttributeName='Policy',
                AttributeValue=json.dumps(sns_policy)
            )
            print(f" Set SNS topic policy to allow S3")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if 'AuthorizationError' in error_code or 'AccessDenied' in error_code:
                print(f"  Cannot set SNS topic policy (missing permissions)")
                print(f"   Error: {e}")
                return {
                    'success': False,
                    'error': f'Missing SNS permissions: {str(e)}'
                }
            else:
                raise
        
        queue_url = None
        queue_arn = None
        subscription_arn = None
        webhook_url = None
        
        webhook_url = os.environ.get('SNS_WEBHOOK_URL')
        
        if not webhook_url:
            try:
                import urllib.request
                ngrok_response = urllib.request.urlopen('http://localhost:4040/api/tunnels', timeout=2)
                ngrok_data = json.loads(ngrok_response.read())
                if ngrok_data.get('tunnels'):
                    https_tunnel = next((t for t in ngrok_data['tunnels'] if t.get('proto') == 'https'), None)
                    if https_tunnel:
                        webhook_url = f"{https_tunnel['public_url']}/api/s3/sns-webhook"
                        print(f" Detected ngrok HTTPS URL: {webhook_url}")
                    else:
                        http_tunnel = ngrok_data['tunnels'][0]
                        webhook_url = f"{http_tunnel['public_url']}/api/s3/sns-webhook"
                        print(f" Detected ngrok HTTP URL: {webhook_url}")
                        print(f"     Note: SNS prefers HTTPS, but HTTP will work")
            except Exception as ngrok_err:
                print(f" ngrok is NOT running!")
                print(f"   Please start ngrok: ngrok http 8099")
                print(f"   Or set SNS_WEBHOOK_URL environment variable")
                return {
                    'success': False,
                    'error': f'ngrok not running. Please start ngrok (ngrok http 8099) or set SNS_WEBHOOK_URL environment variable.'
                }
        
        if not webhook_url:
            return {
                'success': False,
                'error': 'No webhook URL available. Start ngrok or set SNS_WEBHOOK_URL.'
            }
        
        protocol = 'https' if webhook_url.startswith('https://') else 'http'
        
        try:
            print(f" Subscribing HTTP endpoint to SNS topic...")
            print(f"   Webhook URL: {webhook_url}")
            print(f"   Protocol: {protocol}")
            
            subscription = sns_client.subscribe(
                TopicArn=topic_arn,
                Protocol=protocol,
                Endpoint=webhook_url
            )
            subscription_arn = subscription['SubscriptionArn']
            print(f" Subscribed HTTP endpoint to SNS topic")
            print(f"   Subscription ARN: {subscription_arn}")
            print(f"     SNS will send a confirmation request to the webhook")
            print(f"   The webhook endpoint will auto-confirm the subscription")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f" Error subscribing HTTP endpoint:")
            print(f"   Error Code: {error_code}")
            print(f"   Error Message: {error_message}")
            
            if 'InvalidParameter' in error_code or 'Not authorized' in error_message:
                print(f"    SNS requires a public HTTPS URL")
                print(f"   Make sure ngrok is running and providing HTTPS: ngrok http 8099")
            
            return {
                'success': False,
                'error': f'Failed to subscribe HTTP endpoint: {str(e)}'
            }
        
        if not topic_arn:
            return {
                'success': False,
                'error': 'SNS topic ARN not set'
            }
        
        notification_config = {
            'TopicConfigurations': [
                {
                    'Id': f'torro-events-{uuid.uuid4().hex[:8]}',
                    'TopicArn': topic_arn,
                    'Events': [
                        's3:ObjectCreated:*',
                        's3:ObjectRemoved:*',
                    ]
                }
            ]
        }
        
        print(f" Configuring notifications for {len(bucket_names)} bucket(s): {bucket_names}")
        configured_buckets = []
        for bucket_name in bucket_names:
            try:
                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                except ClientError as e:
                    print(f"  Cannot access bucket {bucket_name}: {e}")
                    continue
                
                s3_client.put_bucket_notification_configuration(
                    Bucket=bucket_name,
                    NotificationConfiguration=notification_config
                )
                
                try:
                    verify_response = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
                    topic_configs = verify_response.get('TopicConfigurations', [])
                    queue_configs = verify_response.get('QueueConfigurations', [])
                    if topic_configs or queue_configs:
                        print(f" Verified notifications configured for bucket {bucket_name}")
                        configured_buckets.append(bucket_name)
                    else:
                        print(f"  Notifications not found in bucket {bucket_name} after configuration")
                except ClientError as e:
                    print(f"  Could not verify notifications for bucket {bucket_name}: {e}")
                    configured_buckets.append(bucket_name)
                    
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                error_msg = str(e)
                print(f"  Failed to configure notifications for bucket {bucket_name}: {error_code} - {error_msg}")
                if 'AccessDenied' in error_code:
                    print(f"    Make sure your AWS user has s3:PutBucketNotification permission")
                elif 'InvalidArgument' in error_code:
                    print(f"    Check if the queue ARN is correct and accessible")
                
        result = {
            'success': True,
            'sns_topic_arn': topic_arn,
            'sns_topic_name': topic_name,
            'subscription_arn': subscription_arn if subscription_arn else None,
            'webhook_url': webhook_url if webhook_url else None,
            'sqs_queue_url': None,
            'sqs_queue_arn': None,
            'configured_buckets': configured_buckets
        }
        
        if configured_buckets:
            print(f" Successfully configured event notifications for {len(configured_buckets)} bucket(s): {', '.join(configured_buckets)}")
        else:
            print(f"  Warning: No buckets were configured for event notifications")
            
        return result
        
    except Exception as e:
        print(f" Error setting up S3 event notifications: {e}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': str(e)
        }

@s3_bp.route('/setup-events', methods=['POST'])
def setup_s3_events_endpoint():
    try:
        data = request.json
        connector_id = data.get('connector_id')
        
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
            
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Amazon S3'), None)
        
        if not connector:
            return jsonify({'error': 'S3 connector not found'}), 404
            
        config = connector.get('config', {})
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        
        if not access_key_id or not secret_access_key:
            return jsonify({'error': 'Connector missing AWS credentials'}), 400
        
        bucket_name = config.get('bucketName') or config.get('bucket_name')
        if bucket_name:
            bucket_names = [bucket_name]
        else:
            s3_client = boto3.client('s3', 
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name=region)
            response = s3_client.list_buckets()
            bucket_names = [b['Name'] for b in response.get('Buckets', [])]
            
        result = setup_s3_event_notifications(
            access_key_id, secret_access_key, region, bucket_names
        )
        
        if result['success']:
            config['sns_topic_arn'] = result.get('sns_topic_arn')
            config['sns_topic_name'] = result.get('sns_topic_name')
            config['sns_subscription_arn'] = result.get('subscription_arn')
            config['webhook_url'] = result.get('webhook_url')
            connector['config'] = config
            db_helpers.save_connector(connector)
            
            return jsonify({
                'success': True,
                'message': f'Real-time monitoring enabled for {len(result["configured_buckets"])} bucket(s)',
                'sns_topic_arn': result.get('sns_topic_arn'),
                'webhook_url': result.get('webhook_url'),
                'configured_buckets': result['configured_buckets']
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', 'Unknown error')
            }), 500
            
    except Exception as e:
        print(f"Error setting up S3 events: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/verify-notifications', methods=['GET'])
def verify_bucket_notifications():
    try:
        connector_id = request.args.get('connector_id')
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Amazon S3'), None)
        if not connector:
            return jsonify({'error': 'S3 connector not found'}), 404
        
        config = connector.get('config', {})
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        sns_topic_arn = config.get('sns_topic_arn')
        
        if not access_key_id or not secret_access_key:
            return jsonify({'error': 'Connector missing AWS credentials'}), 400
        
        if not sns_topic_arn:
            return jsonify({'error': 'Event monitoring not configured for this connector'}), 400
        
        s3_client = boto3.client('s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region)
        
        response = s3_client.list_buckets()
        all_buckets = [b['Name'] for b in response.get('Buckets', [])]
        
        bucket_status = []
        for bucket_name in all_buckets:
            try:
                notif_config = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
                topic_configs = notif_config.get('TopicConfigurations', [])
                
                has_our_topic = False
                for tc in topic_configs:
                    if tc.get('TopicArn') == sns_topic_arn:
                        has_our_topic = True
                        break
                
                bucket_status.append({
                    'bucket_name': bucket_name,
                    'notifications_configured': len(topic_configs) > 0,
                    'our_topic_configured': has_our_topic,
                    'topic_configs_count': len(topic_configs)
                })
            except ClientError as e:
                bucket_status.append({
                    'bucket_name': bucket_name,
                    'error': str(e),
                    'notifications_configured': False
                })
        
        return jsonify({
            'connector_id': connector_id,
            'connector_name': connector.get('name'),
            'sns_topic_arn': sns_topic_arn,
            'webhook_url': config.get('webhook_url'),
            'total_buckets': len(all_buckets),
            'bucket_status': bucket_status
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/test-notifications', methods=['POST'])
def test_s3_notifications():
    try:
        connectors = db_helpers.load_connectors()
        s3_connector = next((c for c in connectors if c.get('type') == 'Amazon S3' and c.get('enabled')), None)
        
        if not s3_connector:
            return jsonify({'error': 'No enabled S3 connector found'}), 404
        
        config = s3_connector.get('config', {})
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        bucket_name = config.get('bucketName') or config.get('bucket_name')
        sns_topic_arn = config.get('sns_topic_arn')
        sqs_queue_url = config.get('sqs_queue_url')
        
        if not access_key_id or not secret_access_key:
            return jsonify({'error': 'Missing AWS credentials'}), 400
        
        s3_client = boto3.client('s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region)
        
        sqs_client = boto3.client('sqs',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region)
        
        result = {
            'connector_name': s3_connector.get('name'),
            'sns_topic_arn': sns_topic_arn,
            'sqs_queue_url': sqs_queue_url,
            'buckets_checked': [],
            'issues': []
        }
        
        if bucket_name:
            buckets_to_check = [bucket_name]
        else:
            buckets_response = s3_client.list_buckets()
            buckets_to_check = [b['Name'] for b in buckets_response.get('Buckets', [])]
        
        for bucket in buckets_to_check:
            bucket_info = {'bucket': bucket, 'notifications_configured': False, 'topic_arn': None, 'events': []}
            
            try:
                notif_config = s3_client.get_bucket_notification_configuration(Bucket=bucket)
                
                for topic_config in notif_config.get('TopicConfigurations', []):
                    topic_arn = topic_config.get('TopicArn', '')
                    events = topic_config.get('Events', [])
                    bucket_info['notifications_configured'] = True
                    bucket_info['topic_arn'] = topic_arn
                    bucket_info['events'] = events
                    
                    if topic_arn == sns_topic_arn:
                        bucket_info['matches_connector'] = True
                    else:
                        bucket_info['matches_connector'] = False
                        result['issues'].append(f"Bucket {bucket} has topic {topic_arn} but connector expects {sns_topic_arn}")
                
                if not bucket_info['notifications_configured']:
                    result['issues'].append(f"Bucket {bucket} has no topic notifications configured")
                    
            except Exception as e:
                bucket_info['error'] = str(e)
                result['issues'].append(f"Error checking bucket {bucket}: {str(e)}")
            
            result['buckets_checked'].append(bucket_info)
        
        if sqs_queue_url:
            try:
                queue_attrs = sqs_client.get_queue_attributes(
                    QueueUrl=sqs_queue_url,
                    AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed', 'VisibilityTimeout']
                )
                result['sqs_status'] = {
                    'visible_messages': int(queue_attrs['Attributes'].get('ApproximateNumberOfMessages', '0')),
                    'in_flight': int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', '0')),
                    'delayed': int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesDelayed', '0')),
                    'visibility_timeout': int(queue_attrs['Attributes'].get('VisibilityTimeout', '0'))
                }
            except Exception as e:
                result['sqs_status'] = {'error': str(e)}
                result['issues'].append(f"Error checking SQS queue: {str(e)}")
        else:
            result['issues'].append("No SQS queue URL configured")
        
        if sns_topic_arn:
            try:
                sns_client = boto3.client('sns',
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                    region_name=region)
                
                subscriptions = sns_client.list_subscriptions_by_topic(TopicArn=sns_topic_arn)
                result['sns_subscriptions'] = []
                for sub in subscriptions.get('Subscriptions', []):
                    result['sns_subscriptions'].append({
                        'protocol': sub.get('Protocol'),
                        'endpoint': sub.get('Endpoint'),
                        'subscription_arn': sub.get('SubscriptionArn'),
                        'confirmed': 'PendingConfirmation' not in sub.get('SubscriptionArn', '')
                    })
                    if 'PendingConfirmation' in sub.get('SubscriptionArn', ''):
                        result['issues'].append(f"SNS subscription {sub.get('Endpoint')} is pending confirmation")
            except Exception as e:
                result['sns_subscriptions'] = {'error': str(e)}
                result['issues'].append(f"Error checking SNS subscriptions: {str(e)}")
        
        if not result['issues']:
            result['status'] = 'ok'
            result['message'] = 'S3 notifications are properly configured'
        else:
            result['status'] = 'issues_found'
            result['message'] = f"Found {len(result['issues'])} issue(s)"
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/buckets', methods=['GET'])
def list_s3_buckets():
    try:
        connector_id = request.args.get('connector_id')
        if not connector_id:
            return jsonify({'error': 'connector_id is required'}), 400
        
        connectors = db_helpers.load_connectors()
        connector = next((c for c in connectors if c['id'] == connector_id and c.get('type') == 'Amazon S3'), None)
        if not connector:
            return jsonify({'error': 'S3 connector not found'}), 404
        
        config = connector.get('config', {})
        access_key_id = config.get('access_key_id') or config.get('accessKeyId')
        secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
        region = config.get('region', 'us-east-1')
        
        if not access_key_id or not secret_access_key:
            return jsonify({'error': 'Connector missing AWS credentials'}), 400
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        return jsonify({'buckets': buckets}), 200
    except ClientError as e:
        return jsonify({'error': f'AWS error: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/accept-asset', methods=['POST'])
def accept_pending_asset():
    from database import PendingAsset, Asset
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from config import Config
    
    engine = None
    db_session = None
    
    try:
        data = request.json
        pending_id = data.get('pending_id')
        
        if not pending_id:
            return jsonify({'error': 'pending_id is required'}), 400
        
        engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        
        pending_asset = db_session.query(PendingAsset).filter(
            PendingAsset.id == pending_id,
            PendingAsset.status == 'pending'
        ).first()
        
        if not pending_asset:
            db_session.close()
            db_session = None
            return jsonify({'error': 'Pending asset not found'}), 404
        
        if pending_asset.change_type == 'deleted':
            pending_asset.status = 'accepted'
            pending_asset.processed_at = datetime.utcnow()
            db_session.commit()
            
            asset_deleted = False
            try:
                existing_asset = db_session.query(Asset).filter(
                    Asset.id == pending_asset.asset_id
                ).first()
                if existing_asset:
                    db_session.delete(existing_asset)
                    db_session.commit()
                    asset_deleted = True
                    print(f" Deleted asset {pending_asset.asset_id} from database")
            except Exception as e:
                print(f"Warning: Could not delete asset {pending_asset.asset_id} from database: {e}")
            
            discovered_assets = db_helpers.load_assets()
            assets_count = len(discovered_assets)
            
            db_session.close()
            db_session = None
            return jsonify({
                'success': True, 
                'message': 'Asset removed from inventory',
                'assets_count': assets_count
            }), 200
        
        saved_asset_id = None
        if pending_asset.asset_data:
            asset_data = pending_asset.asset_data.copy()
            
            if 'id' not in asset_data or not asset_data['id']:
                asset_data['id'] = pending_asset.asset_id
            
            asset_data['discovered_at'] = datetime.utcnow().isoformat() + 'Z'
            asset_data['status'] = 'active'
            
            if 'connector_id' not in asset_data:
                asset_data['connector_id'] = pending_asset.connector_id
            
            if 'name' not in asset_data:
                asset_data['name'] = pending_asset.name
            
            if 'type' not in asset_data:
                asset_data['type'] = pending_asset.type
            
            if 'catalog' not in asset_data:
                asset_data['catalog'] = pending_asset.catalog
            
            saved_asset_id = asset_data.get('id')
            
            try:
                existing_asset = db_session.query(Asset).filter(Asset.id == saved_asset_id).first()
                
                if existing_asset:
                    current_time = datetime.utcnow()
                    existing_asset.discovered_at = current_time
                    existing_asset.sort_order = int(current_time.timestamp() * 1000)
                    
                    for key, value in asset_data.items():
                        if key == 'discovered_at':
                            continue
                        if key == 'schema':
                            setattr(existing_asset, 'schema_name', value)
                        elif key == 'metadata' or key == 'extra_data':
                            if isinstance(value, dict):
                                existing_asset.extra_data = value
                        elif key not in ['id']:
                            if hasattr(existing_asset, key):
                                setattr(existing_asset, key, value)
                    existing_asset.updated_at = current_time
                    existing_asset.status = 'active'
                else:
                    from database import Asset as AssetModel
                    
                    current_time = datetime.utcnow()
                    new_asset_data = {
                        'id': saved_asset_id,
                        'name': asset_data.get('name'),
                        'type': asset_data.get('type'),
                        'catalog': asset_data.get('catalog'),
                        'schema_name': asset_data.get('schema', ''),
                        'connector_id': asset_data.get('connector_id'),
                        'status': asset_data.get('status', 'active'),
                        'sort_order': int(current_time.timestamp() * 1000),
                        'extra_data': asset_data
                    }
                    
                    if 'discovered_at' in asset_data:
                        discovered_at_str = asset_data['discovered_at']
                        try:
                            if discovered_at_str.endswith('Z'):
                                discovered_at_str = discovered_at_str[:-1] + '+00:00'
                            new_asset_data['discovered_at'] = datetime.fromisoformat(discovered_at_str)
                        except Exception as e:
                            print(f"  Error parsing discovered_at '{discovered_at_str}': {e}, using current time")
                            new_asset_data['discovered_at'] = current_time
                    else:
                        new_asset_data['discovered_at'] = current_time
                    
                    new_asset = AssetModel(**new_asset_data)
                    db_session.add(new_asset)
                    print(f" Created new asset {saved_asset_id}")
                
                db_session.commit()
                print(f" Successfully saved asset {saved_asset_id} to database")
                
            except Exception as save_error:
                db_session.rollback()
                print(f" Error saving asset {saved_asset_id}: {save_error}")
                import traceback
                traceback.print_exc()
                db_session.close()
                return jsonify({
                    'success': False,
                    'error': f'Failed to save asset to database: {str(save_error)}'
                }), 500
        
        pending_asset.status = 'accepted'
        pending_asset.processed_at = datetime.utcnow()
        db_session.commit()
        db_session.close()
        
        discovered_assets = db_helpers.load_assets()
        assets_count = len(discovered_assets)
        print(f" Reloaded {assets_count} assets from database after adding new asset")
        
        if saved_asset_id:
            saved_asset = next((a for a in discovered_assets if a.get('id') == saved_asset_id), None)
            if saved_asset:
                print(f"Verified saved asset in discovered_assets: {saved_asset.get('name')} (connector: {saved_asset.get('connector_id')})")
                print(f"   Discovered at: {saved_asset.get('discovered_at')}")
                print(f"   Sort order: {saved_asset.get('sort_order')}")
            else:
                print(f"WARNING: Saved asset {saved_asset_id} not found in discovered_assets list!")
        
        try:
            current_app.config['discovered_assets'] = discovered_assets
            print(f" Updated app.config['discovered_assets'] with {assets_count} assets")
            
        except Exception as e:
            print(f"  Could not update app.config: {e}")
        
        return jsonify({
            'success': True, 
            'message': 'Asset accepted and added to inventory',
            'assets_count': assets_count,
            'asset_id': saved_asset_id
        }), 200
    except Exception as e:
        if 'db_session' in locals():
            try:
                db_session.rollback()
                db_session.close()
            except:
                pass
        print(f"Error accepting pending asset: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/airflow-notification', methods=['POST'])
def airflow_notification():
    try:
        data = request.json
        assets = data.get('assets', [])
        connector_name = data.get('connector_name', 'S3')
        connector_id = data.get('connector_id', '')
        
        if not assets:
            return jsonify({'status': 'ok', 'message': 'No assets to notify'}), 200
        
        try:
            from flask import current_app
            from database import PendingAsset
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            from config import Config
            
            socketio = None
            if hasattr(current_app, 'extensions') and 'socketio' in current_app.extensions:
                socketio = current_app.extensions['socketio']
            
            engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            saved_count = 0
            emitted_count = 0
            
            try:
                for asset in assets:
                    asset_id = asset.get('asset_id') or asset.get('id', '')
                    name = asset.get('name', 'Unknown')
                    asset_type = asset.get('type', 'File')
                    catalog = asset.get('catalog', '')
                    
                    if not asset_id:
                        continue
                    
                    pending_id = f"airflow_{connector_id}_{asset_id}_{int(datetime.now().timestamp())}"
                    
                    existing = session.query(PendingAsset).filter(
                        PendingAsset.asset_id == asset_id,
                        PendingAsset.status == 'pending'
                    ).first()
                    
                    if existing:
                        continue
                    
                    pending_asset = PendingAsset(
                        id=pending_id,
                        name=name,
                        type=asset_type,
                        catalog=catalog,
                        connector_id=connector_id,
                        change_type='created',
                        s3_event_type='airflow:discovered',
                        asset_id=asset_id,
                        asset_data=asset,
                        status='pending',
                        created_at=datetime.utcnow()
                    )
                    
                    session.add(pending_asset)
                    saved_count += 1
                    
                    if socketio:
                        socketio.emit('pending_asset_created', {
                            'pending_id': pending_id,
                            'asset_id': asset_id,
                            'name': name,
                            'type': asset_type,
                            'change_type': 'created',
                            'catalog': catalog,
                            'source': 'airflow',
                            'connector_id': connector_id,
                            'connector_name': connector_name,
                            'message': f"Airflow discovered new {connector_name} asset: {name}"
                        }, namespace='/assets')
                        emitted_count += 1
                
                session.commit()
                print(f" ✓ Saved {saved_count} pending assets and emitted {emitted_count} notifications from Airflow")
                
                return jsonify({
                    'status': 'success',
                    'pending_assets_saved': saved_count,
                    'notifications_sent': emitted_count,
                    'message': f'Saved {saved_count} pending asset(s) and sent {emitted_count} notification(s)'
                }), 200
                
            except Exception as db_error:
                session.rollback()
                print(f"  Error saving pending assets: {db_error}")
                import traceback
                traceback.print_exc()
                raise
            finally:
                session.close()
                
        except Exception as e:
            print(f"  Error in notification process: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'status': 'partial',
                'message': f'Notifications failed: {str(e)}'
            }), 200
            
    except Exception as e:
        print(f"Error in airflow notification endpoint: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@s3_bp.route('/trigger-discovery', methods=['POST'])
def trigger_discovery():
    try:
        from datetime import datetime
        import requests
        
        print("=" * 60)
        print("Manual S3 Asset Discovery Triggered (from bell click)")
        print(f"Time: {datetime.now().isoformat()}")
        print("=" * 60)
        
        connectors = db_helpers.load_connectors()
        print(f"Found {len(connectors)} total connectors")
        
        s3_connectors = [
            c for c in connectors 
            if c.get('type') == 'Amazon S3' and c.get('enabled', False)
        ]
        
        if not s3_connectors:
            return jsonify({
                'status': 'error',
                'message': 'No enabled S3 connectors found'
            }), 400
        
        print(f"Processing {len(s3_connectors)} enabled S3 connector(s)")
        
        existing_assets = db_helpers.load_assets()
        existing_asset_ids = {asset.get('id') for asset in existing_assets if asset.get('id')}
        print(f"Found {len(existing_asset_ids)} existing assets in database")
        
        total_new = 0
        all_new_assets = []
        total_discovered = 0
        
        for connector in s3_connectors:
            connector_name = connector.get('name', 'Unknown')
            connector_id = connector.get('id', 'Unknown')
            config = connector.get('config', {})
            
            print(f"\nProcessing: {connector_name}")
            
            access_key_id = config.get('access_key_id') or config.get('accessKeyId')
            secret_access_key = config.get('secret_access_key') or config.get('secretAccessKey')
            region = config.get('region', 'us-east-1')
            bucket_name = config.get('bucketName') or config.get('bucket_name')
            
            if not access_key_id or not secret_access_key:
                print("Missing AWS credentials")
                continue
            
            try:
                print(f"Discovering assets from S3...")
                discovered_assets = discover_s3_assets(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    region=region,
                    bucket_name=bucket_name
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
        
        if all_new_assets:
            print(f"Sending notifications for {len(all_new_assets)} new asset(s)...")
            try:
                backend_url = os.environ.get('BACKEND_URL', 'http://localhost:8099')
                
                response = requests.post(
                    f"{backend_url}/api/s3/airflow-notification",
                    json={
                        'assets': all_new_assets,
                        'connector_name': 'Amazon S3',
                        'connector_id': connector_id if s3_connectors else 'manual_trigger'
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    print(f"Notifications sent successfully!")
                else:
                    print(f"Notification failed: {response.status_code}")
            except Exception as e:
                print(f"Error sending notifications: {e}")
        
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

