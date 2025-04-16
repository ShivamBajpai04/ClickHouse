import { toast } from "sonner";

const API_BASE_URL = "http://localhost:3001";

export interface ClickHouseConfig {
  host: string;
  port: string;
  database: string;
  username: string;
  password?: string;
}

export interface IngestionResult {
  success: boolean;
  records: number;
  message: string;
  filePath?: string;
}

export const apiService = {
  // Connect to ClickHouse
  connectToClickHouse: async (config: ClickHouseConfig) => {
    try {
      const response = await fetch(`${API_BASE_URL}/connect`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(config),
      });

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      const result = await response.json();
      return result;
    } catch (error) {
      toast.error("Connection Error", {
        description: "Failed to connect to ClickHouse server",
      });
      throw error;
    }
  },

  // Get all tables
  getTables: async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/tables`);

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      toast.error("Data Error", {
        description: "Failed to fetch tables from the database",
      });
      throw error;
    }
  },

  // Get columns for a specific table
  getColumns: async (table: string) => {
    try {
      const response = await fetch(`${API_BASE_URL}/columns/${table}`);

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      toast.error("Data Error", {
        description: `Failed to fetch columns for table ${table}`,
      });
      throw error;
    }
  },

  // Upload a file to the server
  uploadFile: async (file: File, delimiter: string = ",") => {
    try {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("delimiter", delimiter);

      const response = await fetch(`${API_BASE_URL}/upload`, {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      const result = await response.json();
      return result;
    } catch (error) {
      toast.error("Upload Error", {
        description: "Failed to upload file to server",
      });
      throw error;
    }
  },

  // Preview data from a table
  previewData: async (table: string, columns: string[]) => {
    try {
      const response = await fetch(`${API_BASE_URL}/preview`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ table, columns }),
      });

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      toast.error("Preview Error", {
        description: "Failed to fetch preview data",
      });
      throw error;
    }
  },

  // Start data ingestion process
  ingestData: async (data: {
    source: string;
    target: string;
    tables: string[];
    columns: Record<string, string[]>;
    filePath?: string;
    targetFile?: string;
    targetTable?: string;
    delimiter?: string;
  }): Promise<IngestionResult> => {
    try {
      const response = await fetch(`${API_BASE_URL}/ingest`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        throw new Error(`Server responded with status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      toast.error("Ingestion Error", {
        description: "An error occurred during data ingestion",
      });
      throw error;
    }
  },

  // Download an exported file
  downloadFile: (filename: string) => {
    // Ensure we're only using the basename of the filename
    const safeFilename =
      filename.split("/").pop()?.split("\\").pop() || filename;
    const url = `${API_BASE_URL}/download/${safeFilename}`;

    // Only run browser-specific code on the client side
    if (typeof window !== "undefined") {
      try {
        const link = document.createElement("a");
        link.href = url;
        link.setAttribute("download", safeFilename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        // Show success toast
        toast.success("CSV Download Started", {
          description: `Your file "${safeFilename}" is being downloaded`,
        });
      } catch (error) {
        console.error("Download error:", error);
        toast.error("Download Error", {
          description: "Failed to download CSV file",
        });
      }
    } else {
      console.log(`Server-side download requested for: ${url}`);
      // When called server-side, we just log the URL but don't try to download
    }
  },
};
