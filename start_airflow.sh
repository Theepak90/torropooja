#!/bin/bash
# Start Airflow webserver and scheduler for automatic S3 asset discovery

set -e

# Set Airflow home
export AIRFLOW_HOME=$HOME/airflow

# Check if Airflow is initialized
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo "⚠️  Airflow database not initialized. Run setup first."
    exit 1
fi

echo "=========================================="
echo "Starting Airflow for S3 Asset Discovery"
echo "=========================================="
echo ""
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""

# Start scheduler in background
echo "🚀 Starting Airflow Scheduler..."
airflow scheduler > $AIRFLOW_HOME/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo "   Scheduler PID: $SCHEDULER_PID"
echo ""

# Wait a moment for scheduler to start
sleep 3

# Start webserver in background
echo "🌐 Starting Airflow Webserver..."
airflow webserver -p 8080 > $AIRFLOW_HOME/webserver.log 2>&1 &
WEBSERVER_PID=$!
echo "   Webserver PID: $WEBSERVER_PID"
echo ""

# Wait a moment for webserver to start
sleep 3

echo "=========================================="
echo "✅ Airflow Started!"
echo "=========================================="
echo ""
echo "📊 Access Airflow UI:"
echo "   URL: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "📁 DAGs folder: $AIRFLOW_HOME/dags"
echo ""
echo "🔍 Find the 's3_asset_discovery' DAG and toggle it ON"
echo ""
echo "📝 Logs:"
echo "   Scheduler: $AIRFLOW_HOME/scheduler.log"
echo "   Webserver: $AIRFLOW_HOME/webserver.log"
echo ""
echo "🛑 To stop Airflow:"
echo "   kill $SCHEDULER_PID $WEBSERVER_PID"
echo ""

# Save PIDs to file for easy stopping
echo "$SCHEDULER_PID $WEBSERVER_PID" > $AIRFLOW_HOME/airflow.pids


