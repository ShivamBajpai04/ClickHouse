import { Eye, Play, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { apiService } from "@/lib/api";
import { useState } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface DataPreviewProps {
  previewData: any[];
  onPreviewData: () => void;
  onStartIngestion: () => void;
  onReset: () => void;
  isLoading: boolean;
  source: string;
  target: string;
  selectedTables: string[];
  selectedColumns: Record<string, string[]>;
  onDataFetched?: (data: any[]) => void;
  onIngestionComplete?: (recordCount: number) => void;
  selectedFile?: File | null;
}

export function DataPreview({
  previewData,
  onPreviewData,
  onStartIngestion,
  onReset,
  isLoading,
  source,
  target,
  selectedTables,
  selectedColumns,
  onDataFetched,
  onIngestionComplete,
  selectedFile,
}: DataPreviewProps) {
  const [targetTable, setTargetTable] = useState("");
  const [targetFile, setTargetFile] = useState("export.csv");
  const [uploadedFilePath, setUploadedFilePath] = useState<string | null>(null);

  const handlePreviewData = async () => {
    if (selectedTables.length === 0) {
      toast.warning("Warning", {
        description: "Please select at least one table to preview",
      });
      return;
    }

    if (
      !selectedColumns[selectedTables[0]] ||
      selectedColumns[selectedTables[0]].length === 0
    ) {
      toast.warning("Warning", {
        description: "Please select at least one column to preview",
      });
      return;
    }

    onPreviewData();

    try {
      // For now, we'll only preview the first selected table
      const table = selectedTables[0];
      const columns = selectedColumns[table];

      const data = await apiService.previewData(table, columns);
      if (onDataFetched) {
        onDataFetched(data);
      }

      toast.success("Preview Loaded", {
        description: `Loaded ${data.length} rows from ${table}`,
      });
    } catch (error) {
      // Error already handled by API service
    }
  };

  const handleStartIngestion = async () => {
    // Validate source data
    if (source === "clickhouse" && selectedTables.length === 0) {
      toast.warning("Warning", {
        description: "Please select at least one table to ingest",
      });
      return;
    }

    if (source === "flatfile" && !selectedFile) {
      toast.warning("Warning", {
        description: "Please upload a file first",
      });
      return;
    }

    // Validate target data
    if (target === "clickhouse" && !targetTable) {
      toast.warning("Warning", {
        description: "Please specify a target table name",
      });
      return;
    }

    onStartIngestion();

    try {
      // Get the file path if the source is a file
      let fileInfo: any = null;
      if (source === "flatfile" && selectedFile) {
        // Upload the file first if not already uploaded
        if (!uploadedFilePath) {
          const result = await apiService.uploadFile(selectedFile);
          if (result.success) {
            fileInfo = result.file;
            setUploadedFilePath(result.file.path);
          } else {
            throw new Error("Failed to upload file");
          }
        }
      }

      const result = await apiService.ingestData({
        source,
        target,
        tables: selectedTables,
        columns: selectedColumns,
        filePath: uploadedFilePath || fileInfo?.path || "",
        targetFile: target === "flatfile" ? targetFile : undefined,
        targetTable: target === "clickhouse" ? targetTable : undefined,
      });

      toast.success("Ingestion Complete", {
        description:
          result.message || `Processed ${result.records} records successfully`,
      });

      // Handle file download for flatfile target
      if (target === "flatfile" && result.filePath) {
        // Extract just the filename without path
        const safeFilename =
          result.filePath.split("/").pop()?.split("\\").pop() ||
          result.filePath;

        // Show download button
        toast.success("File Ready", {
          description: "Your export file is ready for download",
          action: {
            label: "Download",
            onClick: () => apiService.downloadFile(safeFilename),
          },
        });
      }

      if (onIngestionComplete) {
        onIngestionComplete(result.records);
      }
    } catch (error) {
      // Error already handled by API service
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Data Preview and Ingestion Controls</CardTitle>
        <CardDescription>
          Preview data and control the ingestion process
        </CardDescription>
      </CardHeader>
      <CardContent>
        {/* Target configuration section */}
        {target === "clickhouse" && (
          <div className="mb-4">
            <Label htmlFor="targetTable">Target Table Name</Label>
            <Input
              id="targetTable"
              placeholder="Enter target table name"
              value={targetTable}
              onChange={(e) => setTargetTable(e.target.value)}
              className="mb-4"
            />
            {source === "flatfile" && selectedFile && (
              <p className="text-xs text-muted-foreground mb-2">
                Your uploaded file will be ingested into the ClickHouse table
                specified above.
              </p>
            )}
          </div>
        )}

        {target === "flatfile" && (
          <div className="mb-4">
            <Label htmlFor="targetFile">Export File Name</Label>
            <div className="flex items-center gap-2">
              <Input
                id="targetFile"
                placeholder="export.csv"
                value={targetFile}
                onChange={(e) => setTargetFile(e.target.value)}
                className="mb-4"
              />
              {!targetFile.toLowerCase().endsWith(".csv") && (
                <p className="text-xs text-yellow-500 mb-4">
                  File name should end with .csv
                </p>
              )}
            </div>
            <p className="text-xs text-muted-foreground mb-2">
              Your selected data will be exported as a CSV file with this name.
            </p>
          </div>
        )}

        <div className="flex flex-wrap gap-2 mb-4">
          {source === "clickhouse" && (
            <Button
              onClick={handlePreviewData}
              className="flex items-center gap-1"
              disabled={isLoading}
            >
              <Eye className="h-4 w-4" />
              {isLoading ? "Loading..." : "Preview Data"}
            </Button>
          )}
          <Button
            onClick={handleStartIngestion}
            variant="default"
            className="flex items-center gap-1"
            disabled={isLoading}
          >
            <Play className="h-4 w-4" />
            {isLoading
              ? "Processing..."
              : source === "flatfile" && target === "clickhouse"
              ? "Send to ClickHouse"
              : source === "clickhouse" && target === "flatfile"
              ? "Export to CSV"
              : "Start Ingestion"}
          </Button>
          <Button
            onClick={onReset}
            variant="outline"
            className="flex items-center gap-1"
            disabled={isLoading}
          >
            <RefreshCw className="h-4 w-4" />
            Reset
          </Button>
        </div>

        {previewData.length > 0 && (
          <div className="border rounded-md overflow-auto max-h-60">
            <table className="min-w-full divide-y divide-border">
              <thead className="bg-muted">
                <tr>
                  {Object.keys(previewData[0]).map((key) => (
                    <th
                      key={key}
                      className="px-4 py-2 text-left text-xs font-medium text-muted-foreground uppercase tracking-wider"
                    >
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-card divide-y divide-border">
                {previewData.map((row, i) => (
                  <tr key={i}>
                    {Object.values(row).map((value, j) => (
                      <td key={j} className="px-4 py-2 text-sm">
                        {value as string}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
