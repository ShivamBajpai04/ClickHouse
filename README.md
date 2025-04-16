# ClickHouse Data Ingestion Tool

A full-stack application for bi-directional data transfer between ClickHouse databases and flat files (CSV).

## Project Overview

This tool provides a user-friendly interface for:

- Connecting to ClickHouse databases
- Exporting data from ClickHouse to CSV files
- Importing data from CSV files to ClickHouse
- Browsing and previewing database tables and columns

## Project Structure

```
.
├── backend/                # Node.js/Express backend
│   ├── src/                # Source code
│   │   ├── config/         # Configuration files
│   │   ├── controllers/    # Request handlers
│   │   ├── routes/         # API route definitions
│   │   ├── services/       # Business logic
│   │   ├── utils/          # Helper functions
│   │   └── server.js       # Main server file
│   ├── uploads/            # Uploaded files directory
│   ├── exports/            # Exported files directory
│   └── package.json        # Backend dependencies
│
├── frontend/               # React/Next.js frontend
│   ├── src/                # Source code
│   │   ├── app/            # Next.js app directory
│   │   ├── components/     # UI components
│   │   └── lib/            # Utilities and API services
│   └── package.json        # Frontend dependencies
│
├── package.json            # Root package.json with convenience scripts
├── start.sh                # Unix startup script
├── start.bat               # Windows startup script
└── .gitignore              # Git ignore file
```

## Features

- **Database Connectivity**: Connect to any ClickHouse database instance
- **Data Export**: Export data from ClickHouse tables to CSV files
- **Data Import**: Import data from CSV files to ClickHouse tables
- **Table/Column Selection**: Browse and select specific tables and columns
- **Data Preview**: Preview data before import/export
- **File Management**: Upload and download CSV files

## Technologies Used

### Backend

- Node.js with Express
- ClickHouse Node.js client
- CSV parsing and writing utilities
- File upload handling with Multer

### Frontend

- React.js with Next.js
- Modern UI components
- HTTP client for API communication

## Performance Considerations

- Uses streams for large file operations when possible
- Provides support for different delimiters in CSV files
- Implements error handling and connection management

## Getting Started

### Prerequisites

- Node.js (v16 or later)
- ClickHouse database instance
- npm or pnpm package manager

### Installation

#### Method 1: Using the root package.json (recommended)

1. Clone the repository:

   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install all dependencies:
   ```
   npm run install:all
   ```

#### Method 2: Installing individual packages

1. Clone the repository:

   ```
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install backend dependencies:

   ```
   cd backend
   npm install
   cd ..
   ```

3. Install frontend dependencies:
   ```
   cd frontend
   npm install
   cd ..
   ```

### Running the Application

#### Method 1: Using startup scripts

**On Unix/Linux/Mac:**

```
chmod +x start.sh
./start.sh
```

**On Windows:**

```
start.bat
```

#### Method 2: Using npm scripts

```
npm run dev
```

#### Method 3: Running services individually

1. Start the backend server:

   ```
   cd backend
   npm run dev
   ```

2. Start the frontend development server:

   ```
   cd frontend
   npm run dev
   ```

3. Open your browser and navigate to http://localhost:3000

## Usage

1. **Connect to ClickHouse**:

   - Enter your ClickHouse connection details
   - Click "Connect" to establish a connection

2. **Export Data**:

   - Select "ClickHouse to CSV" as the operation
   - Choose tables and columns to export
   - Specify a filename for the exported CSV
   - Click "Export" to start the process

3. **Import Data**:
   - Select "CSV to ClickHouse" as the operation
   - Upload a CSV file
   - Specify a target table name
   - Click "Import" to start the process

## Additional Information

For more details on the individual components:

- See [backend/README.md](backend/README.md) for backend documentation
- For ClickHouse client optimization tips, refer to the [official documentation](https://clickhouse.com/docs/integrations/javascript)

## Performance Optimization

Based on ClickHouse documentation:

- For large datasets, consider enabling compression via `ClickHouseClientConfigOptions.compression` to reduce network traffic
- Use streams for large inserts and selects to reduce application memory consumption
- Consider using async inserts for event listeners and similar use cases

## License

This project is licensed under the ISC License.
