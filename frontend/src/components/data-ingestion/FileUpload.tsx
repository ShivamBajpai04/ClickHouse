import { FileText, Upload } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { apiService } from "@/lib/api";

interface FileUploadProps {
  type: "source" | "target";
  selectedFile: File | null;
  onFileChange: (file: File | null) => void;
}

export function FileUpload({
  type,
  selectedFile,
  onFileChange,
}: FileUploadProps) {
  const [delimiter, setDelimiter] = useState(",");
  const [uploading, setUploading] = useState(false);
  const [uploadedFilePath, setUploadedFilePath] = useState<string | null>(null);

  const handleFileChange = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0] || null;
    if (file) {
      await validateAndUploadFile(file);
    }
  };

  const handleFileDrop = async (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();

    if (event.dataTransfer.files?.length) {
      await validateAndUploadFile(event.dataTransfer.files[0]);
    }
  };

  const validateAndUploadFile = async (file: File) => {
    // Check file type
    const validTypes = [".csv", ".tsv", ".txt", ".json"];
    const extension = file.name.slice(file.name.lastIndexOf(".")).toLowerCase();

    if (!validTypes.includes(extension)) {
      toast.error("Invalid File Type", {
        description: "Please upload a CSV, TSV, TXT, or JSON file",
      });
      return;
    }

    // Check file size (5MB limit)
    if (file.size > 5 * 1024 * 1024) {
      toast.error("File Too Large", {
        description: "Maximum file size is 5MB",
      });
      return;
    }

    // Upload the file to the server
    setUploading(true);
    try {
      const result = await apiService.uploadFile(file, delimiter);

      if (result.success) {
        onFileChange(file);
        setUploadedFilePath(result.file.path);

        toast.success("File Uploaded", {
          description: `${file.name} (${(file.size / 1024).toFixed(2)} KB)`,
        });
      } else {
        toast.error("Upload Failed", {
          description: result.error || "Failed to upload file",
        });
      }
    } catch (error) {
      // Error already handled by API service
    } finally {
      setUploading(false);
    }
  };

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleRemoveFile = () => {
    onFileChange(null);
    setUploadedFilePath(null);
    toast.info("File Removed", {
      description: "The uploaded file has been removed",
    });
  };

  const handleDelimiterChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setDelimiter(e.target.value);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <FileText className="h-5 w-5" />
          Flat File Configuration
        </CardTitle>
        <CardDescription>
          {type === "source" ? "Source" : "Target"} file details
        </CardDescription>
      </CardHeader>
      <CardContent className="grid md:grid-cols-2 gap-4">
        <div>
          <Label htmlFor="filename">File Name</Label>
          <Input
            id="filename"
            placeholder="data.csv"
            className="mt-1"
            value={selectedFile?.name || ""}
            readOnly
          />
        </div>
        <div>
          <Label htmlFor="delimiter">Delimiter</Label>
          <Input
            id="delimiter"
            placeholder=","
            className="mt-1"
            value={delimiter}
            onChange={handleDelimiterChange}
          />
        </div>
        <div className="md:col-span-2">
          <Label htmlFor="fileupload">Upload File</Label>
          <div
            className={`mt-1 border-2 border-dashed rounded-md p-6 flex flex-col items-center justify-center ${
              selectedFile ? "bg-muted/30" : ""
            }`}
            onDrop={handleFileDrop}
            onDragOver={handleDragOver}
          >
            {uploading ? (
              <div className="flex flex-col items-center">
                <div className="animate-spin h-8 w-8 border-2 border-primary border-t-transparent rounded-full mb-2"></div>
                <p className="text-sm">Uploading file...</p>
              </div>
            ) : selectedFile ? (
              <>
                <FileText className="h-8 w-8 text-primary mb-2" />
                <p className="text-sm mb-1 font-medium">{selectedFile.name}</p>
                <p className="text-xs text-muted-foreground mb-2">
                  {(selectedFile.size / 1024).toFixed(2)} KB
                </p>
                <Button variant="outline" size="sm" onClick={handleRemoveFile}>
                  Remove File
                </Button>
              </>
            ) : (
              <>
                <Upload className="h-8 w-8 text-muted-foreground mb-2" />
                <p className="text-sm text-muted-foreground mb-2">
                  Drag and drop your file here or click to browse
                </p>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => document.getElementById("file-input")?.click()}
                >
                  Browse Files
                </Button>
                <input
                  id="file-input"
                  type="file"
                  className="hidden"
                  onChange={handleFileChange}
                  accept=".csv,.tsv,.txt,.json"
                />
              </>
            )}
          </div>

          {type === "source" && (
            <p className="text-xs text-muted-foreground mt-2">
              Supported formats: CSV, TSV, TXT, JSON (up to 5MB)
            </p>
          )}

          {type === "target" && (
            <p className="text-xs text-muted-foreground mt-2">
              The data will be exported as a CSV file with the delimiter
              specified above.
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
