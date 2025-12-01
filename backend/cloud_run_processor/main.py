
from flask import Flask, request, jsonify
import requests
import os
import json
import base64
from datetime import datetime

app = Flask(__name__)

TORRO_API_URL = os.environ.get('TORRO_API_URL', 'http://localhost:8099')
TORRO_API_KEY = os.environ.get('TORRO_API_KEY', '')
SERVICE_NAME = os.environ.get('SERVICE_NAME', 'torro-gcs-processor')
REGION = os.environ.get('REGION', 'us-central1')

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': SERVICE_NAME,
        'timestamp': datetime.utcnow().isoformat()
    }), 200

@app.route('/process-gcs-event', methods=['POST'])
def process_event():
    try:
        event_data = request.get_json()
        
        if not event_data:
            print("  No event data received")
            return jsonify({'error': 'No data received'}), 400
        
        print(f" Received CloudEvent: {json.dumps(event_data, indent=2)[:500]}...")
        
        message_data = event_data.get('message', {})
        
        if not message_data:
            print("  No message in CloudEvent")
            return jsonify({'error': 'Invalid event format'}), 400
        
        encoded_data = message_data.get('data', '')
        if not encoded_data:
            print("  No data in message")
            return jsonify({'error': 'No data in message'}), 400
        
        try:
            decoded_data = base64.b64decode(encoded_data).decode('utf-8')
            gcs_event = json.loads(decoded_data)
        except Exception as decode_error:
            print(f" Error decoding message: {decode_error}")
            return jsonify({'error': f'Failed to decode message: {str(decode_error)}'}), 400
        
        bucket = gcs_event.get('bucket')
        object_name = gcs_event.get('name')
        event_type = gcs_event.get('eventType')
        
        if not bucket or not object_name:
            print(f"  Missing bucket or object name in event")
            return jsonify({'error': 'Missing bucket or object name'}), 400
        
        print(f" Processing GCS event:")
        print(f"   Bucket: {bucket}")
        print(f"   Object: {object_name}")
        print(f"   Event Type: {event_type}")
        
        torro_payload = {
            'bucket': bucket,
            'object': object_name,
            'object_name': object_name,
            'event_type': event_type,
            'eventType': event_type,
            'event_data': gcs_event,
            'source': 'eventarc',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        if TORRO_API_KEY:
            headers['Authorization'] = f'Bearer {TORRO_API_KEY}'
        
        try:
            response = requests.post(
                f"{TORRO_API_URL}/api/gcs/events",
                json=torro_payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                print(f" Successfully forwarded event to Torro API")
                return jsonify({
                    'status': 'processed',
                    'bucket': bucket,
                    'object': object_name,
                    'event_type': event_type
                }), 200
            else:
                print(f" Torro API returned error: {response.status_code} - {response.text}")
                return jsonify({
                    'error': f'Torro API error: {response.status_code}',
                    'details': response.text
                }), response.status_code
                
        except requests.exceptions.RequestException as req_error:
            print(f" Error calling Torro API: {req_error}")
            return jsonify({
                'error': f'Failed to forward event: {str(req_error)}'
            }), 500
        
    except Exception as e:
        print(f" Error processing event: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/', methods=['GET'])
def root():
    return jsonify({
        'service': SERVICE_NAME,
        'status': 'running',
        'endpoints': {
            'health': '/health',
            'process_event': '/process-gcs-event'
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)

