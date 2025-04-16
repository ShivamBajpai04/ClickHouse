import { Database, Lock, Server, RefreshCw } from "lucide-react";
import { useState, useEffect } from "react";
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
  isConnected?: boolean;
}

export function ClickHouseConfig({ 
  type, 
  onConnected,
  isConnected: externalIsConnected 
}: ClickHouseConfigProps) {
  const [host, setHost] = useState("localhost");
  const [port, setPort] = useState("8443");
  const [database, setDatabase] = useState("default");
  const [username, setUsername] = useState("default");
  const [password, setPassword] = useState("");
  const [isConnecting, setIsConnecting] = useState(false);
  const [internalIsConnected, setInternalIsConnected] = useState(false);

  const isConnected = externalIsConnected !== undefined ? externalIsConnected : internalIsConnected;

  useEffect(() => {
    if (externalIsConnected !== undefined) {
      setInternalIsConnected(externalIsConnected);
    }
  }, [externalIsConnected]);

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
        setInternalIsConnected(true);
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

  const handleReset = () => {
    setInternalIsConnected(false);
    if (onConnected) {
      onConnected(false);
    }
    toast.info("Connection Reset", {
      description: "You can now modify connection details and reconnect",
    });
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
          <div className="flex items-end gap-2">
            {isConnected ? (
              <Button 
                onClick={handleReset}
                className="w-full"
                variant="outline"
              >
                <RefreshCw className="h-4 w-4 mr-2" /> Reset Connection
              </Button>
            ) : (
              <Button
                onClick={handleConnect}
                disabled={isConnecting}
                className="w-full"
              >
                {isConnecting ? "Connecting..." : "Connect"}
              </Button>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
