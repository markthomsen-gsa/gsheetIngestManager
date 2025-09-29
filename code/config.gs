/**
 * System configuration and constants
 * All configurable values centralized here
 */

// Data Processing Limits
const MAX_ROWS_PER_FILE = 75000;           // Maximum rows per CSV attachment
const MAX_FILE_SIZE_MB = 25;               // GAS attachment limit
const EXECUTION_TIMEOUT_MS = 300000;       // 5 minutes max execution

// Retry and Error Handling
const RETRY_ATTEMPTS = 3;                  // Number of retry attempts
const RETRY_DELAY_BASE_MS = 1000;          // Base delay for exponential backoff

// Performance Settings
const BATCH_SIZE = 1000;                   // Rows per batch operation (if needed)
const CACHE_TIMEOUT_MS = 300000;           // 5-minute cache expiration
const LOG_RETENTION_DAYS = 30;             // Days to keep logs

// Sheet Names (User-Configurable)
const SHEET_NAMES = {
  rules: 'data-rules',
  logs: 'ingest-logs'
};

// Toast Message Settings
const TOAST_DURATION_MS = 5000;            // Default toast display time
const TOAST_LONG_DURATION_MS = 8000;       // Extended toast for errors

// Email Settings
const EMAIL_RECIPIENTS = [];               // Default: no emails (user configures)
const EMAIL_SUBJECT_PREFIX = '[Data Ingest]';

// CSV Processing Settings
const CSV_ENCODING = 'UTF-8';              // Default CSV encoding
const SUPPORTED_CSV_EXTENSIONS = ['.csv']; // Supported file extensions

// Rule Configuration Column Mappings
const RULE_COLUMNS = {
  ID: 0,
  ACTIVE: 1,
  METHOD: 2,
  SOURCE_QUERY: 3,
  ATTACHMENT_PATTERN: 4,
  DESTINATION: 5,
  MODE: 6,
  EMAIL_RECIPIENTS: 7
};

// Valid values for rule fields
const VALID_METHODS = ['email', 'gSheet', 'push'];
const VALID_MODES = ['clearAndReuse', 'append', 'recreate'];

// Logging Configuration
const LOG_COLUMNS = {
  SESSION_ID: 0,
  TIMESTAMP: 1,
  RULE_ID: 2,
  STATUS: 3,
  MESSAGE: 4,
  ROWS_PROCESSED: 5
};

// Status values for logging
const LOG_STATUS = {
  START: 'START',
  SUCCESS: 'SUCCESS',
  ERROR: 'ERROR',
  INFO: 'INFO',
  WARNING: 'WARNING'
};

/**
 * Get sheet by type with error handling
 */
function getSheet(sheetType) {
  const sheetName = SHEET_NAMES[sheetType];
  if (!sheetName) {
    throw new Error(`Unknown sheet type: ${sheetType}`);
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = spreadsheet.getSheetByName(sheetName);

  if (!sheet) {
    throw new Error(`Sheet '${sheetName}' not found. Run 'Initialize System' first.`);
  }

  return sheet;
}

/**
 * Create sheet with error handling
 */
function createSheet(sheetType) {
  const sheetName = SHEET_NAMES[sheetType];
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  // Check if sheet already exists
  if (spreadsheet.getSheetByName(sheetName)) {
    console.log(`Sheet '${sheetName}' already exists`);
    return spreadsheet.getSheetByName(sheetName);
  }

  // Create new sheet
  const newSheet = spreadsheet.insertSheet(sheetName);
  console.log(`Created sheet: ${sheetName}`);

  return newSheet;
}

/**
 * Get system configuration summary
 */
function getSystemConfig() {
  return {
    maxRows: MAX_ROWS_PER_FILE,
    maxFileSize: MAX_FILE_SIZE_MB,
    retryAttempts: RETRY_ATTEMPTS,
    supportedExtensions: SUPPORTED_CSV_EXTENSIONS,
    validMethods: VALID_METHODS,
    validModes: VALID_MODES
  };
}