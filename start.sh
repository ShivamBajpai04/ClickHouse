#!/bin/bash

# Function to handle cleanup on exit
cleanup() {
  echo "Stopping services..."
  kill $FRONTEND_PID $BACKEND_PID 2>/dev/null
  exit 0
}

# Set up trap to handle Ctrl+C
trap cleanup INT

echo "Starting ClickHouse Data Ingestion Tool..."

# Start backend service
echo "Starting backend server..."
cd backend
npm run dev &
BACKEND_PID=$!
cd ..

# Wait a bit for backend to initialize
sleep 2

# Start frontend service
echo "Starting frontend server..."
cd frontend
npm run dev &
FRONTEND_PID=$!
cd ..

echo "Services started!"
echo "Backend running at http://localhost:3001"
echo "Frontend running at http://localhost:3000"
echo "Press Ctrl+C to stop all services"

# Wait for Ctrl+C
wait $FRONTEND_PID $BACKEND_PID 