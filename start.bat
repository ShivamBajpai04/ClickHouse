@echo off
echo Starting ClickHouse Data Ingestion Tool...

:: Start backend server
echo Starting backend server...
start cmd /k "cd backend && pnpm dev"

:: Wait a bit for backend to initialize
timeout /t 5 /nobreak > nul

:: Start frontend server
echo Starting frontend server...
start cmd /k "cd frontend && pnpm dev"

echo Services started!
echo Backend running at http://localhost:3001
echo Frontend running at http://localhost:3000
echo Close the terminal windows to stop the services 