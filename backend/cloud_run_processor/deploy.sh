#!/bin/bash

# Production Deployment Script for GCS Event Processor
# Usage: ./deploy.sh PROJECT_ID REGION SERVICE_NAME TORRO_API_URL

set -e

PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"us-central1"}
SERVICE_NAME=${3:-"torro-gcs-processor"}
TORRO_API_URL=${4:-"https://your-torro-api.com"}

echo "🚀 Deploying GCS Event Processor to Cloud Run"
echo "   Project: $PROJECT_ID"
echo "   Region: $REGION"
echo "   Service: $SERVICE_NAME"
echo "   Torro API: $TORRO_API_URL"

# Enable required APIs
echo "📦 Enabling required APIs..."
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com \
  pubsub.googleapis.com \
  storage-api.googleapis.com \
  --project=$PROJECT_ID

# Build and deploy
echo "🔨 Building Docker image..."
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME --project=$PROJECT_ID

# Get service account email (or use provided)
SERVICE_ACCOUNT_EMAIL=$(gcloud iam service-accounts list --project=$PROJECT_ID --filter="displayName:Compute Engine default service account" --format="value(email)" | head -1)

if [ -z "$SERVICE_ACCOUNT_EMAIL" ]; then
    echo "⚠️  No service account found, using default"
    SERVICE_ACCOUNT_EMAIL="${PROJECT_ID}@appspot.gserviceaccount.com"
fi

echo "📤 Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image gcr.io/$PROJECT_ID/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars "TORRO_API_URL=$TORRO_API_URL" \
  --service-account $SERVICE_ACCOUNT_EMAIL \
  --memory 512Mi \
  --cpu 1 \
  --timeout 300 \
  --max-instances 10 \
  --min-instances 0 \
  --project=$PROJECT_ID

# Get service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --project=$PROJECT_ID --format="value(status.url)")

echo "✅ Deployment complete!"
echo "   Service URL: $SERVICE_URL"
echo ""
echo "📋 Next steps:"
echo "   1. Test health endpoint: curl $SERVICE_URL/health"
echo "   2. Set up Eventarc trigger via API or manually"
echo "   3. Configure GCS bucket notifications"

