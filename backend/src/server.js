import express from "express";
import cors from "cors";
import clickhouseRoutes from "./routes/clickhouseRoutes.js";
import { PORT } from "./config/server.js";

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Routes
app.use("/", clickhouseRoutes);

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server listening on http://localhost:${PORT}`);
});

export default app;
