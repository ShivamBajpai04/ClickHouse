import { Database, Lock, Server } from "lucide-react";
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

interface ClickHouseConfigProps {
  type: "source" | "target";
  onConnected?: (success: boolean) => void;
}

export function ClickHouseConfig({ type, onConnected }: ClickHouseConfigProps) {
  const [host, setHost] = useState("localhost");
  const [port, setPort] = useState("8123");
  const [database, setDatabase] = useState("default");
  const [username, setUsername] = useState("default");
  const [password, setPassword] = useState("");
  const [isConnecting, setIsConnecting] = useState(false);
  const [isConnected, setIsConnected] = useState(false);

  const handleConnect = async () => {
    if (!host || !port || !database || !username) {
      toast.warning("Warning", {
        description: "Please fill in all required fields",
      });
      return;
    }

    setIsConnecting(true);
    try {
      const result = await apiService.connectToClickHouse({
        host,
        port,
        database,
        username,
        password,
      });

      if (result.success) {
        setIsConnected(true);
        toast.success("Connection Successful", {
          description: "Connected to ClickHouse database",
        });
        if (onConnected) {
          onConnected(true);
        }
      } else {
        toast.error("Connection Failed", {
          description: result.message || "Failed to connect to ClickHouse",
        });
        if (onConnected) {
          onConnected(false);
        }
      }
    } catch (error) {
      // Error already handled by API service
      if (onConnected) {
        onConnected(false);
      }
    } finally {
      setIsConnecting(false);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          ClickHouse Configuration
        </CardTitle>
        <CardDescription>
          {type === "source" ? "Source" : "Target"} database connection details
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <Label htmlFor="host">Host</Label>
            <div className="flex items-center mt-1">
              <Server className="h-4 w-4 mr-2 text-muted-foreground" />
              <Input
                id="host"
                placeholder="localhost"
                value={host}
                onChange={(e) => setHost(e.target.value)}
                disabled={isConnected}
              />
            </div>
          </div>
          <div>
            <Label htmlFor="port">Port</Label>
            <Input
              id="port"
              placeholder="8123"
              value={port}
              onChange={(e) => setPort(e.target.value)}
              disabled={isConnected}
            />
          </div>
          <div>
            <Label htmlFor="database">Database</Label>
            <div className="flex items-center mt-1">
              <Database className="h-4 w-4 mr-2 text-muted-foreground" />
              <Input
                id="database"
                placeholder="default"
                value={database}
                onChange={(e) => setDatabase(e.target.value)}
                disabled={isConnected}
              />
            </div>
          </div>
          <div>
            <Label htmlFor="username">Username</Label>
            <Input
              id="username"
              placeholder="default"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              disabled={isConnected}
            />
          </div>
          <div>
            <Label htmlFor="password">Password</Label>
            <div className="flex items-center mt-1">
              <Lock className="h-4 w-4 mr-2 text-muted-foreground" />
              <Input
                id="password"
                type="password"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isConnected}
              />
            </div>
          </div>
          <div className="flex items-end">
            <Button
              onClick={handleConnect}
              disabled={isConnecting || isConnected}
              className="w-full"
            >
              {isConnecting
                ? "Connecting..."
                : isConnected
                ? "Connected ✓"
                : "Connect"}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
