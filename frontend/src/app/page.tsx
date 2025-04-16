"use client";

import { useState } from "react";
import { SourceTargetSelector } from "@/components/data-ingestion/SourceTargetSelector";
import { ClickHouseConfig } from "@/components/data-ingestion/ClickHouseConfig";
import { FileUpload } from "@/components/data-ingestion/FileUpload";
import { TableColumnSelector } from "@/components/data-ingestion/TableColumnSelector";
import { DataPreview } from "@/components/data-ingestion/DataPreview";
import { StatusDisplay } from "@/components/data-ingestion/StatusDisplay";

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

  // Mock data for demonstration
  const mockTables = ["users", "orders", "products", "transactions"];
  const mockColumns = {
    users: ["id", "name", "email", "created_at"],
    orders: ["id", "user_id", "total", "status", "created_at"],
    products: ["id", "name", "price", "stock", "category"],
    transactions: ["id", "order_id", "amount", "status", "created_at"],
  };

  const handleSourceChange = (value: string) => {
    setSource(value);
    setTarget(value === "clickhouse" ? "flatfile" : "clickhouse");
  };

  const handleTargetChange = (value: string) => {
    setTarget(value);
    setSource(value === "clickhouse" ? "flatfile" : "clickhouse");
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

  const handlePreviewData = () => {
    setStatus("Fetching preview data...");
    setProgress(50);
    setIsLoading(true);

    // Simulate API call
    setTimeout(() => {
      setPreviewData([
        {
          id: 1,
          name: "John Doe",
          email: "john@example.com",
          created_at: "2023-01-01",
        },
        {
          id: 2,
          name: "Jane Smith",
          email: "jane@example.com",
          created_at: "2023-01-02",
        },
        {
          id: 3,
          name: "Bob Johnson",
          email: "bob@example.com",
          created_at: "2023-01-03",
        },
      ]);
      setStatus("Preview ready");
      setProgress(100);
      setIsLoading(false);
    }, 1000);
  };

  const handleDataFetched = (data: any[]) => {
    setPreviewData(data);
    setStatus("Preview ready");
    setProgress(100);
    setIsLoading(false);
  };

  const handleStartIngestion = () => {
    setStatus("Ingestion in progress...");
    setProgress(0);
    setRecordsIngested(0);
    setIsLoading(true);

    // Simulate ingestion process
    const interval = setInterval(() => {
      setProgress((prev) => {
        const newProgress = prev + 10;
        if (newProgress >= 100) {
          clearInterval(interval);
          setStatus("Ingestion completed");
          setIsLoading(false);
          return 100;
        }
        setRecordsIngested(Math.floor(newProgress * 15));
        return newProgress;
      });
    }, 500);
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
  };

  const handleClickHouseConnected = (success: boolean) => {
    setIsClickHouseConnected(success);
    if (success) {
      setStatus("Connected to ClickHouse");
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

        {(source === "clickhouse" || target === "clickhouse") && (
          <ClickHouseConfig
            type={source === "clickhouse" ? "source" : "target"}
            onConnected={handleClickHouseConnected}
          />
        )}

        {(source === "flatfile" || target === "flatfile") && (
          <FileUpload
            type={source === "flatfile" ? "source" : "target"}
            selectedFile={selectedFile}
            onFileChange={setSelectedFile}
          />
        )}

        {source && target && (
          <TableColumnSelector
            mockTables={mockTables}
            mockColumns={mockColumns}
            selectedTables={selectedTables}
            selectedColumns={selectedColumns}
            onTableSelect={handleTableSelect}
            onColumnSelect={handleColumnSelect}
            isClickHouseConnected={isClickHouseConnected}
          />
        )}

        {source && target && selectedTables.length > 0 && (
          <>
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

            <StatusDisplay
              status={status}
              progress={progress}
              recordsIngested={recordsIngested}
            />
          </>
        )}
      </div>
    </div>
  );
}
