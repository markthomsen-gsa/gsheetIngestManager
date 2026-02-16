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
  ACTIVE: 0,                  // Active column is now first
  ID: 1,                      // Rule ID moved to second
  VALIDATION_FORMULA: 2,      // Validation formula column (between ID and Method)
  METHOD: 3,
  SOURCE_QUERY: 4,
  ATTACHMENT_PATTERN: 5,
  SOURCE_TAB: 6,              // Tab name for gSheet source
  MAX_ROWS: 7,                // Max rows to ingest (blank = no limit)
  COLUMN_FILTER: 8,           // Column filter mode: All, Include only, Exclude
  COLUMN_NAMES: 9,            // Comma-separated column names for filter
  ON_MISSING_COLUMN: 10,      // Behavior when named column missing: Halt, Warn
  DESTINATION: 11,
  DESTINATION_TAB: 12,
  MODE: 13,
  LAST_SUCCESS_DIMENSIONS: 14, // Data dimensions of last successful ingest
  LAST_RUN_RESULT: 15,        // Result status (SUCCESS/FAIL)
  DAYS_SINCE_LAST_SUCCESS: 16, // Days since last successful ingest (formula)
  LAST_RUN_TIMESTAMP: 17,     // Timestamp of last run (success or fail)
  EMAIL_RECIPIENTS: 18        // Email recipients (last column)
};

// Valid values for rule fields
const VALID_METHODS = ['email', 'gSheet', 'push'];
const VALID_MODES = ['clearAndReuse', 'append', 'recreate'];
const VALID_COLUMN_FILTERS = ['All', 'Include only', 'Exclude'];
const VALID_ON_MISSING = ['Halt', 'Warn'];

// Logging Configuration
const LOG_COLUMNS = {
  SESSION_ID: 0,
  TIMESTAMP: 1,
  LOG_LEVEL: 2,              // Log level (TRACE, DEBUG, INFO, WARNING, ERROR, FATAL)
  RULE_ID: 3,
  STATUS: 4,                 // Status (START, SUCCESS, ERROR, etc.)
  MESSAGE: 5,
  EXECUTION_TIME_MS: 6,      // Execution time in milliseconds
  ROWS_PROCESSED: 7,
  COLUMNS_PROCESSED: 8,      // Column count
  FILE_SIZE_BYTES: 9,       // File size (for email attachments)
  SOURCE_TYPE: 10,          // Source type (email, gSheet, push)
  SOURCE_IDENTIFIER: 11,    // Source URL/ID/query
  ERROR_CODE: 12,           // Error code for categorization
  ERROR_TYPE: 13,           // Error type (validation, network, etc.)
  RETRY_ATTEMPT: 14,        // Retry attempt number (0 = first attempt)
  DESTINATION_ID: 15,       // Destination sheet ID
  DESTINATION_TAB: 16,      // Destination tab name
  PROCESSING_MODE: 17,      // Processing mode (clearAndReuse, append, etc.)
  METADATA: 18              // JSON string with additional context
};

// Enhanced log levels for better filtering and debugging
const LOG_LEVEL = {
  TRACE: 'TRACE',       // Very detailed, step-by-step execution
  DEBUG: 'DEBUG',       // Detailed debugging information
  INFO: 'INFO',         // General informational messages
  WARNING: 'WARNING',   // Warning messages (non-critical issues)
  ERROR: 'ERROR',       // Error messages (recoverable)
  FATAL: 'FATAL',       // Critical errors (system failures)
  SUCCESS: 'SUCCESS',   // Successful operation completion
  START: 'START'        // Operation start markers
};

// Status values for logging (used with log levels)
const LOG_STATUS = {
  START: 'START',
  SUCCESS: 'SUCCESS',
  ERROR: 'ERROR',
  INFO: 'INFO',
  WARNING: 'WARNING'
};

// Error codes for categorization
const ERROR_CODES = {
  // Validation Errors (1000-1999)
  VALIDATION_RULE_ID_MISSING: 'VAL-1001',
  VALIDATION_METHOD_INVALID: 'VAL-1002',
  VALIDATION_SHEET_ID_INVALID: 'VAL-1003',
  VALIDATION_EMAIL_INVALID: 'VAL-1004',
  VALIDATION_REGEX_INVALID: 'VAL-1005',
  VALIDATION_MODE_INVALID: 'VAL-1006',
  
  // Source Errors (2000-2999)
  SOURCE_SHEET_NOT_FOUND: 'SRC-2001',
  SOURCE_SHEET_ACCESS_DENIED: 'SRC-2002',
  SOURCE_TAB_NOT_FOUND: 'SRC-2003',
  SOURCE_EMAIL_NOT_FOUND: 'SRC-2004',
  SOURCE_ATTACHMENT_NOT_FOUND: 'SRC-2005',
  SOURCE_QUERY_INVALID: 'SRC-2006',
  
  // Processing Errors (3000-3999)
  PROCESSING_CSV_PARSE_ERROR: 'PRC-3001',
  PROCESSING_FILE_TOO_LARGE: 'PRC-3002',
  PROCESSING_TOO_MANY_ROWS: 'PRC-3003',
  PROCESSING_TIMEOUT: 'PRC-3004',
  PROCESSING_MEMORY_LIMIT: 'PRC-3005',
  PROCESSING_COLUMN_MISMATCH: 'PRC-3006',
  
  // Destination Errors (4000-4999)
  DEST_SHEET_NOT_FOUND: 'DST-4001',
  DEST_SHEET_ACCESS_DENIED: 'DST-4002',
  DEST_TAB_CREATE_FAILED: 'DST-4003',
  DEST_WRITE_FAILED: 'DST-4004',
  
  // System Errors (5000-5999)
  SYSTEM_TIMEOUT: 'SYS-5001',
  SYSTEM_MEMORY_LIMIT: 'SYS-5002',
  SYSTEM_RATE_LIMIT: 'SYS-5003',
  SYSTEM_UNKNOWN_ERROR: 'SYS-5999'
};

// Error types for categorization
const ERROR_TYPES = {
  VALIDATION: 'validation',
  SOURCE: 'source',
  PROCESSING: 'processing',
  DESTINATION: 'destination',
  SYSTEM: 'system',
  NETWORK: 'network',
  PERMISSION: 'permission'
};

// Logging configuration
const LOGGING_CONFIG = {
  // Log level filtering
  MIN_LOG_LEVEL: LOG_LEVEL.INFO,
  ENABLE_DEBUG_LOGS: false,
  ENABLE_TRACE_LOGS: false,
  
  // Performance tracking
  ENABLE_PERFORMANCE_TRACKING: true,
  LOG_SLOW_OPERATIONS_MS: 5000,  // Log operations slower than 5 seconds
  
  // Error tracking
  ENABLE_ERROR_CATEGORIZATION: true,
  LOG_ERROR_STACK_TRACES: false,  // Set to true for detailed error tracking
  
  // Metadata
  ENABLE_METADATA_LOGGING: true,
  MAX_METADATA_SIZE_BYTES: 10000  // Limit metadata JSON size
};

/**
 * Get sheet by type with error handling
 * Retrieves sheet by configured name with comprehensive error handling
 * @param {string} sheetType - Sheet type (rules, logs)
 * @returns {Sheet} Google Sheets Sheet object
 * @throws {Error} If sheet type is unknown or sheet not found
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
 * Creates new sheet with configured name if it doesn't exist
 * @param {string} sheetType - Sheet type (rules, logs)
 * @returns {Sheet} Google Sheets Sheet object
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
 * Returns current system configuration values
 * @returns {Object} Configuration object
 * @returns {number} returns.maxRows - Maximum rows per file
 * @returns {number} returns.maxFileSize - Maximum file size in MB
 * @returns {number} returns.retryAttempts - Number of retry attempts
 * @returns {Array<string>} returns.supportedExtensions - Supported file extensions
 * @returns {Array<string>} returns.validMethods - Valid processing methods
 * @returns {Array<string>} returns.validModes - Valid processing modes
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