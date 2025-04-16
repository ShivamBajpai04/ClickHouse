import express from "express";
import cors from "cors";
import { createClient } from "@clickhouse/client";
import fs from "fs";
import csv from "csv-parser";
import { createObjectCsvWriter } from "csv-writer";
import multer from "multer";
import path from "path";
import { fileURLToPath } from "url";

// ES Module equivalent for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3001;

app.use(cors());
app.use(express.json());

// Setup file upload
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(__dirname, "uploads");
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

function startsWith(str, prefix) {
  return str.slice(0, prefix.length) === prefix;
}

const upload = multer({ storage });

let client;
let connectionConfig; // Store connection config for potential reconnection

// Connect to ClickHouse
app.post("/connect", async (req, res) => {
  const { host, port, database, username, password } = req.body;
  console.log("Connection request received:", {
    host,
    port,
    database,
    username,
  });
  let cleanHost = host.trim();
  if (startsWith(cleanHost, "https://") || startsWith(cleanHost, "http://")) {
    cleanHost = cleanHost.replace("https://", "").replace("http://", "");
  }

  try {
    // Store connection config for potential reconnection
    connectionConfig = {
      url: `https://${cleanHost}:${port}`,
      username,
      password,
      database,
    };

    client = createClient(connectionConfig);

    // Test connection
    const result = await client.query({
      query: "SELECT 1",
      format: "JSONEachRow",
    });

    const data = await result.json();
    console.log("Connection successful:", data);
    res.json({ success: true, message: "Connected to ClickHouse" });
  } catch (err) {
    console.error("Connection error:", err.message);
    client = null; // Clear the client on connection failure
    res.status(500).json({
      success: false,
      message: "Failed to connect",
      error: err.message,
    });
  }
});

// Ensure connection is valid or attempt to reconnect
async function ensureConnection() {
  if (!client && connectionConfig) {
    try {
      console.log("Attempting to reconnect to ClickHouse...");
      client = createClient(connectionConfig);
      await client
        .query({
          query: "SELECT 1",
          format: "JSONEachRow",
        })
        .json();
      console.log("Reconnection successful");
      return true;
    } catch (err) {
      console.error("Reconnection failed:", err.message);
      client = null;
      return false;
    }
  }
  return !!client; // Return true if client exists, false otherwise
}

// Get actual tables from ClickHouse
app.get("/tables", async (req, res) => {
  if (!(await ensureConnection())) {
    return res.status(400).json({ error: "Not connected to ClickHouse" });
  }

  try {
    const result = await client.query({
      query: "SHOW TABLES",
      format: "JSONEachRow",
    });

    const tables = await result.json();
    const tableNames = tables.map((table) => table.name);
    res.json(tableNames);
  } catch (err) {
    console.error("Error fetching tables:", err.message);
    res.status(500).json({ error: "Failed to fetch tables" });
  }
});

// Get actual columns for a table
app.get("/columns/:table", async (req, res) => {
  const { table } = req.params;

  if (!(await ensureConnection())) {
    return res.status(400).json({ error: "Not connected to ClickHouse" });
  }

  try {
    const result = await client.query({
      query: `DESCRIBE TABLE ${table}`,
      format: "JSONEachRow",
    });

    const columnsData = await result.json();
    const columnNames = columnsData.map((col) => col.name);
    res.json(columnNames);
  } catch (err) {
    console.error(`Error fetching columns for ${table}:`, err.message);
    res.status(500).json({ error: `Failed to fetch columns for ${table}` });
  }
});

// Preview Data (first few rows from any table)
app.post("/preview", async (req, res) => {
  const { table, columns } = req.body;

  if (!(await ensureConnection())) {
    return res.status(400).json({ error: "Not connected to ClickHouse" });
  }

  try {
    const columnsStr = columns.length > 0 ? columns.join(", ") : "*";
    const result = await client.query({
      query: `SELECT ${columnsStr} FROM ${table} LIMIT 100`,
      format: "JSONEachRow",
    });

    const rows = await result.json();
    res.json(rows);
  } catch (err) {
    console.error("Preview error:", err.message);
    res.status(500).json({ error: "Failed to fetch preview" });
  }
});

// Handle file uploads
app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  const fileInfo = {
    filename: req.file.filename,
    originalName: req.file.originalname,
    path: req.file.path,
    size: req.file.size,
  };

  res.json({ success: true, file: fileInfo });
});

// Process data ingestion from ClickHouse to flat file
async function clickhouseToFlatFile(
  tables,
  columns,
  targetFile,
  delimiter = ","
) {
  // Check if client is available
  if (!(await ensureConnection())) {
    throw new Error("Not connected to ClickHouse. Please connect first.");
  }

  let results = [];
  let recordCount = 0;

  for (const table of tables) {
    const tableColumns = columns[table] || [];
    const columnsStr = tableColumns.length > 0 ? tableColumns.join(", ") : "*";

    try {
      const result = await client.query({
        query: `SELECT ${columnsStr} FROM ${table}`,
        format: "JSONEachRow",
      });

      const rows = await result.json();
      results = results.concat(rows);
      recordCount += rows.length;
    } catch (err) {
      console.error(`Error exporting from ${table}:`, err);
      throw err;
    }
  }

  // Handle empty results
  if (results.length === 0) {
    return { recordCount: 0, message: "No records found to export" };
  }

  // Write to CSV
  const dirPath = path.join(__dirname, "exports");
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
  }

  // Ensure we have a valid filename (remove any path characters)
  const safeFileName = path.basename(targetFile || "export.csv");
  const filePath = path.join(dirPath, safeFileName);

  const header = Object.keys(results[0]).map((key) => ({
    id: key,
    title: key,
  }));
  const csvWriter = createObjectCsvWriter({
    path: filePath,
    header: header,
  });

  await csvWriter.writeRecords(results);

  return {
    recordCount,
    filePath: safeFileName, // Return just the filename, not the full path
    message: `Successfully exported ${recordCount} records to ${safeFileName}`,
  };
}

// Process data ingestion from flat file to ClickHouse
async function flatFileToClickhouse(filePath, targetTable, delimiter = ",") {
  return new Promise(async (resolve, reject) => {
    // Check for ClickHouse connection
    if (!(await ensureConnection())) {
      reject(new Error("Not connected to ClickHouse. Please connect first."));
      return;
    }

    const rows = [];

    fs.createReadStream(filePath)
      .pipe(csv({ separator: delimiter }))
      .on("data", (data) => rows.push(data))
      .on("end", async () => {
        if (rows.length === 0) {
          resolve({ recordCount: 0, message: "No records found in file" });
          return;
        }

        try {
          // Create table if it doesn't exist
          const columns = Object.keys(rows[0]);
          const columnDefs = columns.map((col) => `${col} String`).join(", ");

          await client.exec({
            query: `
              CREATE TABLE IF NOT EXISTS ${targetTable} (
                ${columnDefs}
              ) ENGINE = MergeTree()
              ORDER BY tuple()
            `,
          });

          // Insert data
          for (const row of rows) {
            const columnNames = Object.keys(row).join(", ");
            const values = Object.values(row)
              .map((val) => `'${val.replace(/'/g, "''")}'`)
              .join(", ");

            await client.exec({
              query: `INSERT INTO ${targetTable} (${columnNames}) VALUES (${values})`,
            });
          }

          resolve({
            recordCount: rows.length,
            message: `Successfully imported ${rows.length} records to ${targetTable}`,
          });
        } catch (err) {
          console.error("Import error:", err);
          reject(err);
        }
      })
      .on("error", (err) => {
        reject(err);
      });
  });
}

// Perform data ingestion
app.post("/ingest", async (req, res) => {
  const {
    source,
    target,
    tables,
    columns,
    filePath,
    targetFile,
    targetTable,
    delimiter,
  } = req.body;

  try {
    // Check if client is available for ClickHouse operations
    if (
      (source === "clickhouse" || target === "clickhouse") &&
      !(await ensureConnection())
    ) {
      return res.status(400).json({
        error: "Not connected to ClickHouse",
        message:
          "Please connect to ClickHouse before performing data operations",
      });
    }

    let result;

    if (source === "clickhouse" && target === "flatfile") {
      // ClickHouse to flat file
      result = await clickhouseToFlatFile(
        tables,
        columns,
        targetFile,
        delimiter
      );
    } else if (source === "flatfile" && target === "clickhouse") {
      // Flat file to ClickHouse
      if (!filePath) {
        return res.status(400).json({ error: "No file path provided" });
      }
      if (!targetTable) {
        return res.status(400).json({ error: "No target table specified" });
      }

      result = await flatFileToClickhouse(filePath, targetTable, delimiter);
    } else {
      return res
        .status(400)
        .json({ error: "Invalid source or target combination" });
    }

    res.json({
      success: true,
      records: result.recordCount,
      message: result.message,
      filePath: result.filePath || null,
    });
  } catch (err) {
    console.error("Ingestion error:", err.message);
    res.status(500).json({
      error: "Failed to perform data ingestion",
      message: err.message,
    });
  }
});

// Download exported file
app.get("/download/:filename", (req, res) => {
  const { filename } = req.params;
  // Make sure we only use the basename (no path manipulation)
  const safeFilename = path.basename(filename);
  const filePath = path.join(__dirname, "exports", safeFilename);

  console.log(
    `Download requested for: ${safeFilename}, looking at: ${filePath}`
  );

  if (!fs.existsSync(filePath)) {
    console.error(`File not found: ${filePath}`);
    return res.status(404).json({ error: "File not found" });
  }

  res.download(filePath);
});

app.listen(PORT, () => {
  console.log(`🚀 Server listening on http://localhost:${PORT}`);
});
