import path from "path";

// Function to check if a string starts with a prefix
export function startsWith(str, prefix) {
  return str.slice(0, prefix.length) === prefix;
}

// Ensure we have a valid filename (remove any path characters)
export function getSafeFileName(fileName) {
  return path.basename(fileName || "export.csv");
}

// Create a CSV header from object keys
export function createCsvHeader(obj) {
  return Object.keys(obj).map((key) => ({
    id: key,
    title: key,
  }));
}