"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Switch } from "@/components/ui/switch";
import { ChevronDown, Info, Plus, X } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface Column {
  name: string;
  type: string;
  defaultValue: string;
  nullable: boolean;
}

interface TableData {
  headers: string[];
  rows: Record<string, string>[];
}

interface TableConfigurationProps {
  sourceData: TableData;
  onImport: (config: any) => void;
  availableTables?: string[];
}

export default function TableConfiguration({
  sourceData,
  onImport,
  availableTables = [],
}: TableConfigurationProps) {
  const [uploadType, setUploadType] = useState<"new" | "existing">("new");
  const [database, setDatabase] = useState("default");
  const [tableName, setTableName] = useState("");
  const [existingTable, setExistingTable] = useState("");
  const [sortingKey, setSortingKey] = useState("");
  const [engine, setEngine] = useState("MergeTree");
  const [partitionBy, setPartitionBy] = useState("");
  const [primaryKey, setPrimaryKey] = useState("");

  // Initialize columns based on source data headers
  const [columns, setColumns] = useState<Column[]>([]);

  // Set default table name and initial columns when source data changes
  useEffect(() => {
    if (sourceData.headers.length > 0) {
      // Generate a default table name based on file
      setTableName(`imported_data_${Date.now().toString().slice(-6)}`);

      // Initialize columns based on source data headers
      setColumns(
        sourceData.headers.map((header) => {
          // Try to guess column types based on data
          let inferredType = "String";

          if (sourceData.rows.length > 0) {
            const sampleValue = sourceData.rows[0][header];
            if (!isNaN(Number(sampleValue))) {
              if (sampleValue.includes(".")) {
                inferredType = "Float64";
              } else {
                inferredType = "Int64";
              }
            } else if (
              /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(sampleValue) ||
              /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(sampleValue)
            ) {
              inferredType = "DateTime";
            } else if (/^\d{4}-\d{2}-\d{2}$/.test(sampleValue)) {
              inferredType = "Date";
            }
          }

          return {
            name: header,
            type: inferredType,
            defaultValue: "",
            nullable: false,
          };
        })
      );
    }
  }, [sourceData]);

  const addColumn = () => {
    setColumns([
      ...columns,
      { name: "", type: "String", defaultValue: "", nullable: false },
    ]);
  };

  const removeColumn = (index: number) => {
    const newColumns = [...columns];
    newColumns.splice(index, 1);
    setColumns(newColumns);
  };

  const updateColumn = (
    index: number,
    field: keyof Column,
    value: string | boolean
  ) => {
    const newColumns = [...columns];
    newColumns[index] = { ...newColumns[index], [field]: value };
    setColumns(newColumns);
  };

  const handleImport = () => {
    const config = {
      uploadType,
      database,
      tableName: uploadType === "new" ? tableName : existingTable,
      sortingKey,
      columns,
      engine,
      partitionBy,
      primaryKey,
    };

    onImport(config);
  };

  // Generate table name options for existing tables
  const tableOptions = availableTables.map((table) => (
    <SelectItem key={table} value={table}>
      {table}
    </SelectItem>
  ));

  // Generate sorting key options from column names
  const sortingKeyOptions = columns.map((column) => (
    <SelectItem key={column.name} value={column.name}>
      {column.name}
    </SelectItem>
  ));

  return (
    <Card className="w-full">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <div className="flex items-center justify-center rounded-full bg-black text-white w-6 h-6 text-sm">
            2
          </div>
          Configure table
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {/* Source Data Preview */}
        <div className="border rounded-md overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-muted">
              <tr>
                <th className="px-4 py-2 text-left text-xs font-medium text-muted-foreground">
                  #
                </th>
                {sourceData.headers.map((header, index) => (
                  <th
                    key={index}
                    className="px-4 py-2 text-left text-xs font-medium text-muted-foreground"
                  >
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-card divide-y divide-gray-200">
              {sourceData.rows.map((row, rowIndex) => (
                <tr key={rowIndex}>
                  <td className="px-4 py-2 text-sm">{rowIndex + 1}</td>
                  {sourceData.headers.map((header, colIndex) => (
                    <td key={colIndex} className="px-4 py-2 text-sm">
                      {row[header]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Destination Table */}
        <div>
          <h3 className="text-lg font-medium mb-2">Destination table</h3>
          <div className="space-y-4">
            <div>
              <p className="text-sm mb-2">Upload data to</p>
              <RadioGroup
                value={uploadType}
                onValueChange={(value) =>
                  setUploadType(value as "new" | "existing")
                }
                className="flex items-center gap-4"
              >
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="new" id="new-table" />
                  <Label htmlFor="new-table">New table</Label>
                </div>
                <div className="flex items-center space-x-2">
                  <RadioGroupItem value="existing" id="existing-table" />
                  <Label htmlFor="existing-table">Existing table</Label>
                </div>
              </RadioGroup>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="database">Database</Label>
                <Select value={database} onValueChange={setDatabase}>
                  <SelectTrigger id="database" className="mt-1">
                    <SelectValue placeholder="Select database" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="default">default</SelectItem>
                    <SelectItem value="system">system</SelectItem>
                    <SelectItem value="analytics">analytics</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {uploadType === "new" ? (
                <div>
                  <Label htmlFor="table-name">Name</Label>
                  <Input
                    id="table-name"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value)}
                    className="mt-1"
                    placeholder="Enter table name"
                  />
                </div>
              ) : (
                <div>
                  <Label htmlFor="existing-table">Table</Label>
                  <Select
                    value={existingTable}
                    onValueChange={setExistingTable}
                  >
                    <SelectTrigger id="existing-table" className="mt-1">
                      <SelectValue placeholder="Select existing table" />
                    </SelectTrigger>
                    <SelectContent>
                      {availableTables.length > 0 ? (
                        tableOptions
                      ) : (
                        <SelectItem value="no-tables" disabled>
                          No tables available
                        </SelectItem>
                      )}
                    </SelectContent>
                  </Select>
                </div>
              )}
            </div>

            {uploadType === "new" && (
              <>
                <div>
                  <Label
                    htmlFor="sorting-key"
                    className="flex items-center gap-1"
                  >
                    Sorting key
                    <TooltipProvider>
                      <Tooltip>
                        <TooltipTrigger>
                          <Info className="h-4 w-4 text-muted-foreground" />
                        </TooltipTrigger>
                        <TooltipContent>
                          <p className="max-w-xs">
                            The sorting key determines the order in which data
                            is stored in the table.
                          </p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  </Label>
                  <Select value={sortingKey} onValueChange={setSortingKey}>
                    <SelectTrigger id="sorting-key" className="mt-1">
                      <SelectValue placeholder="Select an option" />
                    </SelectTrigger>
                    <SelectContent>{sortingKeyOptions}</SelectContent>
                  </Select>
                </div>

                {/* Column Settings */}
                <Collapsible defaultOpen className="border rounded-md p-2">
                  <CollapsibleTrigger className="flex items-center justify-between w-full p-2">
                    <span className="font-medium">Column settings</span>
                    <ChevronDown className="h-4 w-4" />
                  </CollapsibleTrigger>
                  <CollapsibleContent className="pt-2">
                    <div className="space-y-2">
                      <div className="grid grid-cols-[2fr_1fr_1fr_80px_30px] gap-2 px-2 py-1 bg-muted rounded-sm">
                        <div className="text-xs font-medium">Source field</div>
                        <div className="text-xs font-medium">Type</div>
                        <div className="text-xs font-medium">Default value</div>
                        <div className="text-xs font-medium flex items-center gap-1">
                          Nullable
                          <TooltipProvider>
                            <Tooltip>
                              <TooltipTrigger>
                                <Info className="h-3 w-3 text-muted-foreground" />
                              </TooltipTrigger>
                              <TooltipContent>
                                <p>
                                  If enabled, the column can contain NULL
                                  values.
                                </p>
                              </TooltipContent>
                            </Tooltip>
                          </TooltipProvider>
                        </div>
                        <div></div>
                      </div>

                      {columns.map((column, index) => (
                        <div
                          key={index}
                          className="grid grid-cols-[2fr_1fr_1fr_80px_30px] gap-2 items-center"
                        >
                          <Input
                            value={column.name}
                            onChange={(e) =>
                              updateColumn(index, "name", e.target.value)
                            }
                          />
                          <Select
                            value={column.type}
                            onValueChange={(value) =>
                              updateColumn(index, "type", value)
                            }
                          >
                            <SelectTrigger>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="Int64">Int64</SelectItem>
                              <SelectItem value="String">String</SelectItem>
                              <SelectItem value="Float64">Float64</SelectItem>
                              <SelectItem value="Date">Date</SelectItem>
                              <SelectItem value="DateTime">DateTime</SelectItem>
                            </SelectContent>
                          </Select>
                          <Input
                            value={column.defaultValue}
                            onChange={(e) =>
                              updateColumn(
                                index,
                                "defaultValue",
                                e.target.value
                              )
                            }
                          />
                          <div className="flex justify-center">
                            <Switch
                              checked={column.nullable}
                              onCheckedChange={(checked) =>
                                updateColumn(index, "nullable", checked)
                              }
                            />
                          </div>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => removeColumn(index)}
                            className="h-8 w-8"
                          >
                            <X className="h-4 w-4" />
                          </Button>
                        </div>
                      ))}

                      <Button
                        variant="outline"
                        size="sm"
                        onClick={addColumn}
                        className="mt-2 flex items-center gap-1"
                      >
                        <Plus className="h-4 w-4" /> Add column
                      </Button>
                    </div>
                  </CollapsibleContent>
                </Collapsible>

                {/* Advanced Settings */}
                <Collapsible className="border rounded-md p-2">
                  <CollapsibleTrigger className="flex items-center justify-between w-full p-2">
                    <span className="font-medium">Advanced settings</span>
                    <ChevronDown className="h-4 w-4" />
                  </CollapsibleTrigger>
                  <CollapsibleContent className="pt-2">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <div>
                        <Label htmlFor="engine">Engine</Label>
                        <Select value={engine} onValueChange={setEngine}>
                          <SelectTrigger id="engine" className="mt-1">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="MergeTree">MergeTree</SelectItem>
                            <SelectItem value="ReplacingMergeTree">
                              ReplacingMergeTree
                            </SelectItem>
                            <SelectItem value="SummingMergeTree">
                              SummingMergeTree
                            </SelectItem>
                            <SelectItem value="AggregatingMergeTree">
                              AggregatingMergeTree
                            </SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      <div>
                        <Label htmlFor="partition-by">Partition by</Label>
                        <Input
                          id="partition-by"
                          value={partitionBy}
                          onChange={(e) => setPartitionBy(e.target.value)}
                          className="mt-1"
                          placeholder="Partitioning key expression"
                        />
                      </div>
                      <div>
                        <Label htmlFor="primary-key">Primary key</Label>
                        <Input
                          id="primary-key"
                          value={primaryKey}
                          onChange={(e) => setPrimaryKey(e.target.value)}
                          className="mt-1"
                          placeholder="Primary key expression"
                        />
                      </div>
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </>
            )}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex justify-between pt-4">
          <Button variant="outline" className="flex items-center gap-1">
            Open as query
          </Button>
          <Button
            onClick={handleImport}
            className="bg-black text-white hover:bg-gray-800"
            disabled={
              (uploadType === "new" && !tableName) ||
              (uploadType === "existing" && !existingTable)
            }
          >
            Import to ClickHouse
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
