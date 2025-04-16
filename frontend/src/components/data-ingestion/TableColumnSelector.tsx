import { useEffect, useState } from "react";
import { toast } from "sonner";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Input } from "@/components/ui/input";
import { apiService } from "@/lib/api";

interface TableColumnSelectorProps {
  selectedTables: string[];
  selectedColumns: Record<string, string[]>;
  onTableSelect: (table: string) => void;
  onColumnSelect: (table: string, column: string) => void;
  isClickHouseConnected?: boolean;
  mockTables?: string[];
  mockColumns?: Record<string, string[]>;
}

export function TableColumnSelector({
  selectedTables,
  selectedColumns,
  onTableSelect,
  onColumnSelect,
  isClickHouseConnected,
  mockTables = [],
  mockColumns = {},
}: TableColumnSelectorProps) {
  const [tables, setTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<Record<string, string[]>>({});
  const [loading, setLoading] = useState(false);
  const [columnsLoading, setColumnsLoading] = useState<Record<string, boolean>>(
    {}
  );
  const [error, setError] = useState<string | null>(null);

  // Fetch tables when ClickHouse is connected
  useEffect(() => {
    if (isClickHouseConnected) {
      fetchTables();
    } else if (mockTables.length > 0) {
      // Use mock data if provided and not connected to ClickHouse
      setTables(mockTables);
      setColumns(mockColumns);
    }
  }, [isClickHouseConnected, mockTables, mockColumns]);

  // Fetch columns when tables are selected
  useEffect(() => {
    if (selectedTables.length > 0 && isClickHouseConnected) {
      selectedTables.forEach(fetchColumns);
    }
  }, [selectedTables, isClickHouseConnected]);

  const fetchTables = async () => {
    setLoading(true);
    setError(null);

    try {
      const tableList = await apiService.getTables();
      setTables(tableList);

      toast.success("Tables Loaded", {
        description: `Found ${tableList.length} tables in database`,
      });
    } catch (error) {
      console.error("Error fetching tables:", error);
      setError(
        "Failed to fetch tables from ClickHouse. Please check your connection."
      );
      // Error already handled by API service
    } finally {
      setLoading(false);
    }
  };

  const fetchColumns = async (table: string) => {
    if (columns[table]) return; // Skip if already fetched

    setColumnsLoading((prev) => ({ ...prev, [table]: true }));

    try {
      const columnList = await apiService.getColumns(table);
      setColumns((prev) => ({ ...prev, [table]: columnList }));

      toast.success(`Columns for ${table} Loaded`, {
        description: `Found ${columnList.length} columns in table ${table}`,
      });
    } catch (error) {
      console.error(`Error fetching columns for ${table}:`, error);
      // Error already handled by API service
    } finally {
      setColumnsLoading((prev) => ({ ...prev, [table]: false }));
    }
  };

  // Use the tables and columns from state, which could be from API or mock data
  const displayTables = tables.length > 0 ? tables : mockTables;
  const displayColumns =
    Object.keys(columns).length > 0 ? columns : mockColumns;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Table and Column Selection</CardTitle>
        <CardDescription>
          Select tables and columns to include in the ingestion
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <div>
            <Label className="text-base">Select Tables</Label>
            {loading ? (
              <div className="text-sm text-muted-foreground my-2">
                Loading tables...
              </div>
            ) : error ? (
              <div className="text-sm text-red-500 my-2">{error}</div>
            ) : displayTables.length === 0 ? (
              <div className="text-sm text-muted-foreground my-2">
                No tables found. Please check your database connection.
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-2 mt-2">
                {displayTables.map((table) => (
                  <div key={table} className="flex items-center space-x-2">
                    <Checkbox
                      id={`table-${table}`}
                      checked={selectedTables.includes(table)}
                      onCheckedChange={() => onTableSelect(table)}
                    />
                    <Label htmlFor={`table-${table}`} className="font-normal">
                      {table}
                    </Label>
                  </div>
                ))}
              </div>
            )}
          </div>

          {selectedTables.length > 0 && (
            <div>
              <Label className="text-base">Select Columns</Label>
              <Tabs defaultValue={selectedTables[0]} className="mt-2">
                <TabsList className="mb-2">
                  {selectedTables.map((table) => (
                    <TabsTrigger key={table} value={table}>
                      {table}
                    </TabsTrigger>
                  ))}
                </TabsList>
                {selectedTables.map((table) => (
                  <TabsContent key={table} value={table}>
                    {columnsLoading[table] ? (
                      <div className="text-sm text-muted-foreground my-2">
                        Loading columns for {table}...
                      </div>
                    ) : !displayColumns[table] ? (
                      <div className="text-sm text-muted-foreground my-2">
                        No columns found for this table.
                      </div>
                    ) : (
                      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
                        {displayColumns[table]?.map((column) => (
                          <div
                            key={column}
                            className="flex items-center space-x-2"
                          >
                            <Checkbox
                              id={`${table}-${column}`}
                              checked={
                                selectedColumns[table]?.includes(column) ||
                                false
                              }
                              onCheckedChange={() =>
                                onColumnSelect(table, column)
                              }
                            />
                            <Label
                              htmlFor={`${table}-${column}`}
                              className="font-normal"
                            >
                              {column}
                            </Label>
                          </div>
                        ))}
                      </div>
                    )}
                  </TabsContent>
                ))}
              </Tabs>
            </div>
          )}

          {selectedTables.length > 1 && (
            <div>
              <Label htmlFor="join-keys">JOIN Keys or Conditions</Label>
              <Input
                id="join-keys"
                placeholder="e.g., users.id = orders.user_id"
                className="mt-1"
              />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
