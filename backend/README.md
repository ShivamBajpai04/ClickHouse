# ClickHouse Data Ingestion Backend

This is a modular backend service for a data ingestion tool supporting ClickHouse databases and flat files.

## Project Structure

```
backend/
├── src/                  # Source directory
│   ├── config/           # Configuration files
│   │   ├── database.js   # Database configuration
│   │   └── server.js     # Server configuration
│   ├── controllers/      # Request handlers
│   │   └── clickhouseController.js  # Controllers for ClickHouse operations
│   ├── routes/           # API routes
│   │   └── clickhouseRoutes.js      # Routes for ClickHouse operations
│   ├── services/         # Business logic
│   │   └── clickhouseService.js     # Services for ClickHouse operations
│   ├── utils/            # Helper functions
│   │   └── helpers.js    # Utility functions
│   ├── middleware/       # Custom middleware
│   └── server.js         # Main server file
├── uploads/              # Uploaded files directory
├── exports/              # Exported files directory
├── package.json          # Package configuration
└── README.md             # Project documentation
```

## Features

- Connect to ClickHouse databases
- Export data from ClickHouse to CSV
- Import data from CSV to ClickHouse
- Browse tables and columns
- Preview data
- File upload/download capabilities

## API Endpoints

- `POST /connect` - Connect to a ClickHouse database
- `GET /tables` - Get all tables from the database
- `GET /columns/:table` - Get columns for a specific table
- `POST /preview` - Preview data from a table
- `POST /upload` - Upload a file
- `POST /ingest` - Perform data ingestion
- `GET /download/:filename` - Download an exported file

## Getting Started

1. Install dependencies:

   ```
   npm install
   ```

2. Start the development server:

   ```
   npm run dev
   ```

3. For production:
   ```
   npm start
   ```

## ClickHouse Client

This project uses the official ClickHouse Node.js client (`@clickhouse/client`) for database operations.
