import { ArrowRightLeft, Database, FileText } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface SourceTargetSelectorProps {
  source: string;
  target: string;
  onSourceChange: (value: string) => void;
  onTargetChange: (value: string) => void;
}

export function SourceTargetSelector({
  source,
  target,
  onSourceChange,
  onTargetChange,
}: SourceTargetSelectorProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ArrowRightLeft className="h-5 w-5" />
          Source and Target Selection
        </CardTitle>
        <CardDescription>
          Select the source and target for your data ingestion
        </CardDescription>
      </CardHeader>
      <CardContent className="grid md:grid-cols-2 gap-6">
        <div>
          <Label htmlFor="source">Source</Label>
          <Select value={source} onValueChange={onSourceChange}>
            <SelectTrigger id="source" className="mt-1">
              <SelectValue placeholder="Select source" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="clickhouse">
                <div className="flex items-center gap-2">
                  <Database className="h-4 w-4" />
                  ClickHouse
                </div>
              </SelectItem>
              <SelectItem value="flatfile">
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4" />
                  Flat File
                </div>
              </SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div>
          <Label htmlFor="target">Target</Label>
          <Select value={target} onValueChange={onTargetChange}>
            <SelectTrigger id="target" className="mt-1">
              <SelectValue placeholder="Select target" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="clickhouse">
                <div className="flex items-center gap-2">
                  <Database className="h-4 w-4" />
                  ClickHouse
                </div>
              </SelectItem>
              <SelectItem value="flatfile">
                <div className="flex items-center gap-2">
                  <FileText className="h-4 w-4" />
                  Flat File
                </div>
              </SelectItem>
            </SelectContent>
          </Select>
        </div>
      </CardContent>
    </Card>
  );
}
