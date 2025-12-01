#!/bin/bash

# Start Flask-SocketIO server with gunicorn and 20 workers
# This script uses gunicorn with eventlet workers for Flask-SocketIO support

echo "Starting Torro backend server with 20 workers..."

# Check if gunicorn is installed
if ! command -v gunicorn &> /dev/null; then
    echo "Installing gunicorn and eventlet..."
    pip install gunicorn eventlet
fi

# Start server with 20 workers using eventlet
cd "$(dirname "$0")"
gunicorn \
    --bind 0.0.0.0:8099 \
    --workers 20 \
    --worker-class eventlet \
    --worker-connections 1000 \
    --timeout 300 \
    --keep-alive 2 \
    --log-level info \
    --access-logfile - \
    --error-logfile - \
    "main:app"

