
# Data Pipeline for CSV Ingestion

## Project Overview

This project builds a data pipeline to handle downloading, decompressing, extracting, and ingesting large `.tar.gz` files containing CSV data into a SQLite database. The focus of this challenge was to build a memory-efficient, stream-based solution that can handle large datasets without causing memory overload.

The project utilizes various Node.js modules and libraries to achieve this, ensuring optimal performance and error handling.

## Features

- **Stream-based Downloading**: Uses Axios to download the `.tar.gz` file as a stream to avoid loading large files into memory.
- **Decompression**: Utilizes Zlib to decompress the `.tar.gz` file and extract its contents.
- **Efficient Parsing**: CSV data is parsed in a streaming manner using `fast-csv`, which processes rows one by one, avoiding large memory consumption.
- **Batch Database Insertion**: Inserts data into SQLite in batches using `knex` to reduce the number of database transactions, improving performance.
- **Atomicity & Error Handling**: Batch inserts are done within transactions to ensure data integrity. If any batch fails, the entire transaction is rolled back, and the error is logged.

## Tech Stack

- **Node.js**: Handles streams, file system interactions, and backend logic.
- **TypeScript**: Provides static typing and better maintainability.
- **Axios**: Streams the file download to avoid loading the entire file in memory.
- **Zlib**: Decompresses `.tar.gz` files.
- **tar-fs**: Extracts the files from the decompressed tar archive.
- **fast-csv**: Parses the CSV files row by row in a streaming manner.
- **knex**: Used to set up and interact with the SQLite database.

---

## Project Structure

```bash
├── challenge.ts      # Main file containing the data pipeline implementation
├── package.json      # Node.js dependencies and scripts
├── package-lock.json # Dependency lock file
├── README.md         # This readme file
├── resources.ts      # Constants like file paths and URLs
├── runner.ts         # Script to execute the pipeline
├── tsconfig.json     # TypeScript configuration file
└── tsbuildinfo       # TypeScript build info (auto-generated)
```

---

## How It Works

1. **Download**: The `.tar.gz` file is downloaded from a given URL using Axios in a streaming manner.
2. **Extract**: The compressed file is decompressed using Zlib and extracted using tar-fs.
3. **Parse CSV**: Once the CSV files are extracted, they are parsed row by row using `fast-csv`.
4. **Database Insertion**: Parsed data is inserted into the SQLite database in batches for efficiency, using Knex.
5. **Error Handling**: If any error occurs during the batch insertion, the error is logged, and the database transaction is rolled back.

---

## Getting Started

### Prerequisites

- Node.js (v18 or greater)
- SQLite

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/challenge-1-data-pipeline.git
   ```

2. Navigate to the project directory:
   ```bash
   cd challenge-1-data-pipeline
   ```

3. Install the required dependencies:
   ```bash
   npm install
   ```

---

## Usage

1. Open the **`resources.ts`** file and modify the `DUMP_DOWNLOAD_URL` and `SQLITE_DB_PATH` constants if necessary.

2. Run the project:
   ```bash
   npm run start
   ```

The script will download the `.tar.gz` file, extract it, and insert the CSV data into the SQLite database.

---

## Error Handling

- **Failed Rows**: If any rows fail to insert into the database during the batch process, the error is logged to a file (`failed_rows.log`).
- **Transaction Rollbacks**: If an error occurs while processing a batch, the entire transaction is rolled back to maintain database integrity.

---

## Improvements

- **Scalability**: The pipeline can be extended to work with larger datasets by using more robust cloud solutions like Amazon S3 for file storage and Amazon RDS for database management.
- **Error Recovery**: Currently, the system logs the errors, but adding retry logic for failed batches could make the process more resilient.

