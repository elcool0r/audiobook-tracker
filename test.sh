#!/bin/bash
# Test runner script for local development

set -e

echo "🚀 Running Audiobook Tracker Tests"
echo "==================================="

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "🐍 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Check if MongoDB is running
if ! pgrep -f mongod > /dev/null && ! docker ps | grep -q mongo; then
    echo "⚠️  MongoDB not running. Starting MongoDB container..."
    docker run -d --name test-mongo -p 27017:27017 mongo:7
    sleep 5
fi

# Set test environment variables
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="test_audiobook_tracker"
export SECRET_KEY="test_secret_key_local"

echo "📦 Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "🧪 Running unit tests..."
unset MONGO_URI  # Use mongomock for unit tests
PYTHONWARNINGS="ignore:pkg_resources is deprecated as an API:DeprecationWarning,ignore:'crypt' is deprecated and slated for removal in Python 3.13:DeprecationWarning" python -m pytest tests/operations -v

echo "🔗 Checking integration test requirements..."
# Check if MongoDB is accessible from localhost
if python -c "
import socket
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(('127.0.0.1', 27017))
    sock.close()
    exit(0 if result == 0 else 1)
except:
    exit(1)
"; then
    echo "✅ MongoDB accessible, running integration tests..."
    export MONGO_URI="mongodb://localhost:27017"  # Use real MongoDB for integration tests
    python -m pytest tests/integration tracker/test_release_sweep.py -v
else
    echo "⚠️  MongoDB not accessible from localhost, skipping integration tests"
    echo "💡 To run integration tests locally, uncomment the MongoDB port in docker-compose.yml"
fi

echo "🐳 Testing Docker build..."
docker build -t test-audiobook-tracker .
echo "✅ Docker build successful"

echo "🎉 All tests passed!"

# Cleanup
if docker ps -a --format 'table {{.Names}}' | grep -q test-mongo; then
    echo "🧹 Cleaning up test MongoDB container..."
    docker stop test-mongo
    docker rm test-mongo
fi

# Deactivate virtual environment
deactivate