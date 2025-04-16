"use client";

import { useState, useEffect } from "react";
import { SourceTargetSelector } from "@/components/data-ingestion/SourceTargetSelector";
import { ClickHouseConfig } from "@/components/data-ingestion/ClickHouseConfig";
import { FileUpload } from "@/components/data-ingestion/FileUpload";
import { TableColumnSelector } from "@/components/data-ingestion/TableColumnSelector";
import { DataPreview } from "@/components/data-ingestion/DataPreview";
import { StatusDisplay } from "@/components/data-ingestion/StatusDisplay";
import TableConfiguration from "@/components/data-ingestion/TableConfig";
import { apiService } from "@/lib/api";
import { toast } from "sonner";

export default function DataIngestionTool() {
  const [source, setSource] = useState("");
  const [target, setTarget] = useState("");
  const [selectedTables, setSelectedTables] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<
    Record<string, string[]>
  >({});
  const [status, setStatus] = useState("Ready");
  const [progress, setProgress] = useState(0);
  const [recordsIngested, setRecordsIngested] = useState(0);
  const [previewData, setPreviewData] = useState<any[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isClickHouseConnected, setIsClickHouseConnected] = useState(false);
  const [tableData, setTableData] = useState<any>({
    headers: [],
    rows: [],
  });
  const [clickhouseTables, setClickhouseTables] = useState<string[]>([]);
  const [uploadedFileInfo, setUploadedFileInfo] = useState<any>(null);
  const [showTableConfig, setShowTableConfig] = useState(false);
  const [clickhouseColumns, setClickhouseColumns] = useState<
    Record<string, string[]>
  >({});
  const [showTableSelector, setShowTableSelector] = useState(false);

  // Mock data for demonstration
  const mockTables = ["users", "orders", "products", "transactions"];
  const mockColumns = {
    users: ["id", "name", "email", "created_at"],
    orders: ["id", "user_id", "total", "status", "created_at"],
    products: ["id", "name", "price", "stock", "category"],
    transactions: ["id", "order_id", "amount", "status", "created_at"],
  };

  // Fetch tables from ClickHouse when connected
  useEffect(() => {
    if (isClickHouseConnected) {
      fetchClickHouseTables();
    }
  }, [isClickHouseConnected]);

  const fetchClickHouseTables = async () => {
    try {
      const tables = await apiService.getTables();
      setClickhouseTables(tables);
      setShowTableSelector(source === "clickhouse" && target === "flatfile");
    } catch (error) {
      console.error("Failed to fetch tables:", error);
      toast.error("Failed to fetch tables from ClickHouse");
    }
  };

  const fetchClickHouseColumns = async (table: string) => {
    if (clickhouseColumns[table]) return; // Skip if already fetched

    try {
      const columns = await apiService.getColumns(table);
      setClickhouseColumns((prev) => ({
        ...prev,
        [table]: columns,
      }));
    } catch (error) {
      console.error(`Failed to fetch columns for ${table}:`, error);
      toast.error(`Failed to fetch columns for table ${table}`);
    }
  };

  const handleSourceChange = (value: string) => {
    setSource(value);
    setTarget(value === "clickhouse" ? "flatfile" : "clickhouse");
    // Reset states
    setShowTableConfig(false);
    setShowTableSelector(false);
    setTableData({ headers: [], rows: [] });
    setSelectedTables([]);
    setSelectedColumns({});
    setPreviewData([]);
  };

  const handleTargetChange = (value: string) => {
    setTarget(value);
    setSource(value === "clickhouse" ? "flatfile" : "clickhouse");
    // Reset states
    setShowTableConfig(false);
    setShowTableSelector(false);
    setTableData({ headers: [], rows: [] });
    setSelectedTables([]);
    setSelectedColumns({});
    setPreviewData([]);
  };

  const handleTableSelect = (table: string) => {
    if (selectedTables.includes(table)) {
      setSelectedTables(selectedTables.filter((t) => t !== table));
      const newSelectedColumns = { ...selectedColumns };
      delete newSelectedColumns[table];
      setSelectedColumns(newSelectedColumns);
    } else {
      setSelectedTables([...selectedTables, table]);
      setSelectedColumns({
        ...selectedColumns,
        [table]: [],
      });

      // Fetch columns for this table if it's not already fetched
      if (isClickHouseConnected && !clickhouseColumns[table]) {
        fetchClickHouseColumns(table);
      }
    }
  };

  const handleColumnSelect = (table: string, column: string) => {
    if (selectedColumns[table]?.includes(column)) {
      setSelectedColumns({
        ...selectedColumns,
        [table]: selectedColumns[table].filter((c) => c !== column),
      });
    } else {
      setSelectedColumns({
        ...selectedColumns,
        [table]: [...(selectedColumns[table] || []), column],
      });
    }
  };

  const handlePreviewData = async () => {
    if (selectedTables.length === 0) {
      toast.warning("Please select at least one table");
      return;
    }

    if (Object.values(selectedColumns).every((cols) => cols.length === 0)) {
      toast.warning("Please select at least one column");
      return;
    }

    setStatus("Fetching preview data...");
    setProgress(30);
    setIsLoading(true);

    try {
      // For simplicity, we'll only preview the first selected table
      const table = selectedTables[0];
      const columns = selectedColumns[table] || [];

      const data = await apiService.previewData(table, columns);
      setPreviewData(data);
      setStatus("Preview ready");
      toast.success(`Loaded ${data.length} rows from ${table}`);
    } catch (error) {
      console.error("Failed to fetch preview data:", error);
      toast.error("Failed to fetch preview data");
      setStatus("Preview failed");
    } finally {
      setProgress(100);
      setIsLoading(false);
    }
  };

  const handleDataFetched = (data: any[]) => {
    setPreviewData(data);
    setStatus("Preview ready");
    setProgress(100);
    setIsLoading(false);
  };

  const handleStartIngestion = async () => {
    if (source === "clickhouse") {
      if (selectedTables.length === 0) {
        toast.warning("Please select at least one table");
        return;
      }

      if (Object.values(selectedColumns).every((cols) => cols.length === 0)) {
        toast.warning("Please select at least one column");
        return;
      }
    } else if (source === "flatfile" && !uploadedFileInfo) {
      toast.warning("Please upload a file first");
      return;
    }

    setStatus("Ingestion in progress...");
    setProgress(10);
    setRecordsIngested(0);
    setIsLoading(true);

    try {
      let result:
        | {
            success: boolean;
            records: number;
            message: string;
            filePath?: string;
          }
        | undefined;

      if (source === "clickhouse" && target === "flatfile") {
        // Check connection status first
        try {
          // Export from ClickHouse to CSV
          const exportFileName = `export_${Date.now()}.csv`;

          result = await apiService.ingestData({
            source,
            target,
            tables: selectedTables,
            columns: selectedColumns,
            targetFile: exportFileName,
            delimiter: ",",
          });

          // Offer download if successful
          if (result && result.success && result.filePath) {
            const filePath = result.filePath;

            // Automatically trigger download
            apiService.downloadFile(filePath);

            toast.success("Export complete", {
              description: `${result.records} records exported to ${filePath}`,
              action: {
                label: "Download Again",
                onClick: () => apiService.downloadFile(filePath),
              },
            });
          }
        } catch (error: any) {
          if (error.message && error.message.includes("Not connected")) {
            // Connection issue
            toast.error("Connection to ClickHouse lost", {
              description: "Please reconnect to ClickHouse and try again",
              action: {
                label: "Reconnect",
                onClick: () => setIsClickHouseConnected(false),
              },
            });
          } else {
            throw error; // Re-throw to be caught by outer catch
          }
        }
      } else if (source === "flatfile" && target === "clickhouse") {
        // This is handled by handleTableConfig for new tables
      }

      if (result) {
        setStatus("Ingestion completed");
        setRecordsIngested(result.records || 0);
        toast.success(`Processed ${result.records} records`);
      }
    } catch (error) {
      console.error("Ingestion error:", error);
      toast.error("Failed to complete data ingestion");
      setStatus("Ingestion failed");
    } finally {
      setProgress(100);
      setIsLoading(false);
    }
  };

  const handleIngestionComplete = (recordCount: number) => {
    setStatus("Ingestion completed");
    setProgress(100);
    setRecordsIngested(recordCount);
    setIsLoading(false);
  };

  const handleReset = () => {
    setSource("");
    setTarget("");
    setSelectedTables([]);
    setSelectedColumns({});
    setStatus("Ready");
    setProgress(0);
    setRecordsIngested(0);
    setPreviewData([]);
    setSelectedFile(null);
    setIsClickHouseConnected(false);
    setShowTableConfig(false);
    setTableData({ headers: [], rows: [] });
    setUploadedFileInfo(null);
    setShowTableSelector(false);
  };

  const handleClickHouseConnected = (success: boolean) => {
    setIsClickHouseConnected(success);
    if (success) {
      setStatus("Connected to ClickHouse");

      // If source is ClickHouse, show table selector
      if (source === "clickhouse") {
        setShowTableSelector(true);
      }
    }
  };

  const handleFileChange = async (file: File | null) => {
    setSelectedFile(file);
    setShowTableConfig(false);

    if (file) {
      setStatus("Uploading file...");
      setProgress(30);
      setIsLoading(true);

      try {
        // Upload file to server
        const result = await apiService.uploadFile(file);
        setUploadedFileInfo(result.file);

        // Extract preview data from CSV
        // Note: In a real app, you might want to add a backend endpoint to parse and return preview data
        const reader = new FileReader();
        reader.onload = (e) => {
          const text = e.target?.result as string;
          if (text) {
            const lines = text.split("\n");
            if (lines.length > 0) {
              const headers = lines[0].split(",").map((h) => h.trim());
              const dataRows = [];

              // Process up to 5 rows for preview
              for (let i = 1; i < Math.min(lines.length, 6); i++) {
                if (lines[i].trim()) {
                  const values = lines[i].split(",").map((v) => v.trim());
                  const row: Record<string, string> = {};

                  headers.forEach((header, index) => {
                    row[header] = values[index] || "";
                  });

                  dataRows.push(row);
                }
              }

              setTableData({
                headers,
                rows: dataRows,
              });

              setShowTableConfig(true);
              setStatus("File uploaded successfully");
              setProgress(100);
            }
          }
          setIsLoading(false);
        };

        reader.readAsText(file);
      } catch (error) {
        console.error("Error uploading file:", error);
        toast.error("Failed to upload file");
        setIsLoading(false);
      }
    }
  };

  const handleTableConfig = async (config: any) => {
    if (!uploadedFileInfo) {
      toast.error("No file uploaded");
      return;
    }

    setStatus("Starting data ingestion...");
    setProgress(10);
    setIsLoading(true);

    try {
      // Call the API to ingest data
      const result = await apiService.ingestData({
        source: "flatfile",
        target: "clickhouse",
        tables: [],
        columns: {},
        filePath: uploadedFileInfo.path,
        targetTable: config.tableName,
        delimiter: ",",
      });

      setStatus(result.message || "Import completed");
      setProgress(100);
      setRecordsIngested(result.records || 0);
      toast.success("Data imported successfully", {
        description: `${result.records} records imported to ${config.tableName}`,
      });

      // Update tables list after successful import
      if (isClickHouseConnected) {
        fetchClickHouseTables();
      }
    } catch (error) {
      console.error("Ingestion error:", error);
      toast.error("Failed to import data to ClickHouse");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="container mx-auto py-8 px-4">
      <div className="flex flex-col space-y-8 max-w-4xl mx-auto">
        <header className="text-center">
          <h1 className="text-3xl font-bold mb-2">Data Ingestion Tool</h1>
          <p className="text-muted-foreground">
            Transfer data between ClickHouse and Flat Files
          </p>
        </header>

        <SourceTargetSelector
          source={source}
          target={target}
          onSourceChange={handleSourceChange}
          onTargetChange={handleTargetChange}
        />

        {source && (
          <ClickHouseConfig
            type={source === "clickhouse" ? "source" : "target"}
            onConnected={handleClickHouseConnected}
          />
        )}

        {source === "flatfile" && (
          <>
            <FileUpload
              type={source === "flatfile" ? "source" : "target"}
              selectedFile={selectedFile}
              onFileChange={handleFileChange}
            />
            {showTableConfig && (
              <TableConfiguration
                sourceData={tableData}
                onImport={handleTableConfig}
                availableTables={clickhouseTables}
              />
            )}
          </>
        )}

        {/* Show table selector when ClickHouse is the source */}
        {source === "clickhouse" &&
          isClickHouseConnected &&
          showTableSelector && (
            <TableColumnSelector
              selectedTables={selectedTables}
              selectedColumns={selectedColumns}
              onTableSelect={handleTableSelect}
              onColumnSelect={handleColumnSelect}
              isClickHouseConnected={isClickHouseConnected}
              mockTables={[]}
              mockColumns={clickhouseColumns}
            />
          )}

        {/* Show data preview and ingestion controls for ClickHouse to Flatfile */}
        {source === "clickhouse" && selectedTables.length > 0 && (
          <DataPreview
            previewData={previewData}
            onPreviewData={handlePreviewData}
            onStartIngestion={handleStartIngestion}
            onReset={handleReset}
            isLoading={isLoading}
            source={source}
            target={target}
            selectedTables={selectedTables}
            selectedColumns={selectedColumns}
            onDataFetched={handleDataFetched}
            onIngestionComplete={handleIngestionComplete}
          />
        )}

        {/* Show status display when operation is in progress */}
        {isLoading && (
          <StatusDisplay
            status={status}
            progress={progress}
            recordsIngested={recordsIngested}
          />
        )}
      </div>
    </div>
  );
}
