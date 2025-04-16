import { ensureConnection } from "../config/database.js";
import { getExportsDirectory, getUploadsDirectory } from "../config/server.js";
import { getSafeFileName, createCsvHeader } from "../utils/helpers.js";
import { createObjectCsvWriter } from "csv-writer";
import fs from "fs";
import csv from "csv-parser";
import path from "path";

// Get all tables from ClickHouse
export const getTables = async (client) => {
  try {
    const result = await client.query({
      query: "SHOW TABLES",
      format: "JSONEachRow",
    });

    const tables = await result.json();
    return tables.map((table) => table.name);
  } catch (err) {
    console.error("Error fetching tables:", err.message);
    throw new Error("Failed to fetch tables");
  }
};

// Get all columns for a specific table
export const getColumns = async (client, table) => {
  try {
    const result = await client.query({
      query: `DESCRIBE TABLE ${table}`,
      format: "JSONEachRow",
    });

    const columnsData = await result.json();
    return columnsData.map((col) => col.name);
  } catch (err) {
    console.error(`Error fetching columns for ${table}:`, err.message);
    throw new Error(`Failed to fetch columns for ${table}`);
  }
};

// Preview data from a table
export const previewData = async (client, table, columns) => {
  try {
    const columnsStr = columns.length > 0 ? columns.join(", ") : "*";
    const result = await client.query({
      query: `SELECT ${columnsStr} FROM ${table} LIMIT 100`,
      format: "JSONEachRow",
    });

    return await result.json();
  } catch (err) {
    console.error("Preview error:", err.message);
    throw new Error("Failed to fetch preview");
  }
};

// Process data ingestion from ClickHouse to flat file
export const clickhouseToFlatFile = async (
  client,
  tables,
  columns,
  targetFile,
  delimiter = ","
) => {
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
  const dirPath = getExportsDirectory();

  // Ensure we have a valid filename
  const safeFileName = getSafeFileName(targetFile);
  const filePath = path.join(dirPath, safeFileName);

  const header = createCsvHeader(results[0]);
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
};

// Process data ingestion from flat file to ClickHouse
export const flatFileToClickhouse = async (
  client,
  filePath,
  targetTable,
  delimiter = ","
) => {
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
};
