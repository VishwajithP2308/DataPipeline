import * as fs from 'fs';
import * as fsExtra from 'fs-extra';
import * as zlib from 'zlib';
import * as tar from 'tar-fs';
import * as fastcsv from 'fast-csv';
import knex, { Knex } from 'knex';
import axios from 'axios';
import { createWriteStream } from 'fs';
import { DUMP_DOWNLOAD_URL, SQLITE_DB_PATH } from './resources';

/**
 * Set up the SQLite database using Knex.
 */
function setupDatabase(dbPath: string): Knex {
  return knex({
    client: 'sqlite3',
    connection: { filename: dbPath },
    useNullAsDefault: true,
  });
}

/**
 * Create the required tables in SQLite for customers and organizations.
 */
async function createTables(db: Knex): Promise<void> {
  await db.schema.createTable('customers', (table: Knex.CreateTableBuilder) => {
    table.increments('Index').primary();
    table.string('Customer Id').notNullable();
    table.string('First Name').notNullable();
    table.string('Last Name').notNullable();
    table.string('Company').notNullable();
    table.string('City').notNullable();
    table.string('Country').notNullable();
    table.string('Phone 1').notNullable();
    table.string('Phone 2');
    table.string('Email').notNullable();
    table.date('Subscription Date').notNullable();
    table.string('Website').notNullable();
  });

  await db.schema.createTable('organizations', (table: Knex.CreateTableBuilder) => {
    table.increments('Index').primary();
    table.string('Organization Id').notNullable();
    table.string('Name').notNullable();
    table.string('Website').notNullable();
    table.string('Country').notNullable();
    table.string('Description').notNullable();
    table.integer('Founded').notNullable();
    table.string('Industry').notNullable();
    table.integer('Number of employees').notNullable();
  });
}

/**
 * Download the tar.gz file from a given URL using streaming.
 */
async function downloadFile(url: string, outputPath: string): Promise<void> {
  const response = await axios.get(url, { responseType: 'stream' });
  const writer = createWriteStream(outputPath);

  response.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on('finish', resolve);
    writer.on('error', reject);
  });
}

/**
 * Extract a tar.gz file to a specified folder using streams.
 */
async function extractTar(filePath: string, extractDir: string): Promise<void> {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(zlib.createGunzip())
      .pipe(tar.extract(extractDir))
      .on('finish', resolve)
      .on('error', reject);
  });
}

/**
 * Insert data from a CSV into a specific table in the database with smaller batches and manual conflict handling.
 * Stop the process and log a message when an error occurs, but continue to the next table.
 */
async function insertDataWithSummary(
  filePath: string,
  tableName: string,
  db: Knex,
  batchSize = 100  // Reduced batch size to avoid overwhelming SQLite
): Promise<number> {
  let totalRowCount = 0; // Track total number of successfully inserted rows
  const rows: Array<object> = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(fastcsv.parse({ headers: true }))
      .on('data', async (row: object) => {
        rows.push(row);

        // Insert batch when batch size is reached
        if (rows.length === batchSize) {
          try {
            await db.transaction(async (trx) => { //We wrap the insert operation in a database transaction to ensure atomicity and consistency in case of failure.
              await trx(tableName).insert(rows);  // Insert batch
              totalRowCount += rows.length;  // Update total count with batch size
              console.log(`Total rows inserted into Customers tableso far: ${totalRowCount}`);  // Display progress
            });
            rows.length = 0; // Clear the array for the next batch
          } catch (err) {
            logFailedRows(rows);  // Log all rows in the batch if there is an error
            return reject({ totalRowCount, message: `Stopped processing ${tableName} due to an error.` });
          }
        }
      })
      .on('end', async () => {
        // Insert remaining rows
        if (rows.length > 0) {
          try {
            await db.transaction(async (trx) => {
              await trx(tableName).insert(rows);  // Insert remaining rows
              totalRowCount += rows.length;  // Update total count with remaining rows
              console.log(`Total rows inserted into Organizations table so far: ${totalRowCount}`);
            });
          } catch (err) {
            logFailedRows(rows);  // Log remaining rows in the case of an error
            return reject({ totalRowCount, message: `Stopped processing ${tableName} due to an error.` });
          }
        }
        resolve(totalRowCount);  // Resolve with the total number of rows inserted
      })
      .on('error', (error) => {
        reject({ totalRowCount, message: `Error reading CSV file for ${tableName}: ${error}` });
      });
  });
}

/**
 * Log failed rows to a file for debugging purposes.
 */
function logFailedRows(rows: object[]): void {
  if (rows.length > 0) {
    const logFilePath = 'failed_rows.log';
    const logData = rows.map(row => JSON.stringify(row)).join('\n');  // Stringify each row for logging
    fs.appendFileSync(logFilePath, logData + '\n');
    console.log(`Failed rows logged to ${logFilePath}`);
  }
}

/**
 * Main function to handle the database and data population process.
 */
export async function processDataDump(): Promise<void> {
  const tmpFolder = 'tmp/';
  const tarFilePath = `${tmpFolder}dump.tar.gz`;
  const extractedFolder = `${tmpFolder}extracted/`;
  let customerRowsInserted = 0;
  let organizationRowsInserted = 0;

  await fs.promises.mkdir(tmpFolder, { recursive: true });
  await fs.promises.mkdir(extractedFolder, { recursive: true });
  await fs.promises.mkdir('out', { recursive: true });

  // Download the tar.gz file
  await downloadFile(DUMP_DOWNLOAD_URL, tarFilePath);

  // Extract the tar.gz file
  await extractTar(tarFilePath, extractedFolder);

  // Set up the SQLite database and create tables
  const db = setupDatabase(SQLITE_DB_PATH);
  try {
    await db.raw('PRAGMA foreign_keys = OFF');
    await db.raw('PRAGMA synchronous = OFF');

    await createTables(db);

    // Insert customers.csv data
    try {
      console.log('Starting insertion for customers.csv...');
      customerRowsInserted = await insertDataWithSummary(`${extractedFolder}/dump/customers.csv`, 'customers', db);
      console.log(`Finished inserting customers.csv. Total inserted: ${customerRowsInserted}`);
    } catch (error) {
      if (error && typeof error === 'object' && 'totalRowCount' in error) {
        customerRowsInserted = (error as { totalRowCount: number }).totalRowCount;  
      }
      console.log(`Inserted ${customerRowsInserted} rows into customers table before stopping.`);
    }

    // Insert organizations.csv data, continue even if customers.csv failed
    try {
      console.log('Starting insertion for organizations.csv...');
      organizationRowsInserted = await insertDataWithSummary(`${extractedFolder}/dump/organizations.csv`, 'organizations', db);
      console.log(`Finished inserting organizations.csv. Total inserted: ${organizationRowsInserted}`);
    } catch (error) {
      if (error && typeof error === 'object' && 'totalRowCount' in error) {
        organizationRowsInserted = (error as { totalRowCount: number }).totalRowCount;  
      }
      console.log(`Inserted ${organizationRowsInserted} rows into organizations table before stopping.`);
    }

    console.log('Data insertion process completed.');
    console.log(`Total rows inserted into customers table: ${customerRowsInserted}`);
    console.log(`Total rows inserted into organizations table: ${organizationRowsInserted}`);
  } finally {
    await db.raw('PRAGMA foreign_keys = ON');
    await db.raw('PRAGMA synchronous = FULL');
    await db.destroy();
    console.log('Database connection closed.');
  }
}








