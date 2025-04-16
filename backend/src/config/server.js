import path from "path";
import { fileURLToPath } from "url";
import multer from "multer";
import fs from "fs";

// ES Module equivalent for __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.join(__dirname, "../..");

// Server configuration
export const PORT = 3001;

// File storage configuration
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = path.join(rootDir, "uploads");
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});

export const upload = multer({ storage });

// Path for exports directory
export const getExportsDirectory = () => {
  const dirPath = path.join(rootDir, "exports");
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
  }
  return dirPath;
};

// Path for uploads directory
export const getUploadsDirectory = () => {
  const dirPath = path.join(rootDir, "uploads");
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath);
  }
  return dirPath;
};

// Get root directory
export const getRootDirectory = () => rootDir;
