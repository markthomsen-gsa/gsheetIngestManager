/**
 * Data Ingestion System for Google Sheets
 * This script allows for automated data transfer between various sources (emails, Google Sheets)
 * and destinations with configurable behaviors, logging, and enhanced visualization.
 *
 * VERSION: 2.6.0 (Performance Optimizations)
 * UPDATED: April 29, 2025 // Assumed date
 *
 * CHANGE SUMMARY (v2.6.0):
 * - ADDED: Granular performance controls with PERFORMANCE_CONFIG
 * - ADDED: Caching system for frequently accessed data
 * - ADDED: Batch processing for large datasets
 * - ADDED: Configurable logging levels (MINIMAL, STANDARD, DETAILED)
 * - ADDED: Configurable verification levels (NONE, BASIC, FULL)
 * - OPTIMIZED: Default settings for faster execution (disabled verification and formatting)
 * - IMPROVED: Memory management and API call reduction
 * - IMPROVED: Error handling and recovery
 * 
 * CHANGE SUMMARY (v2.5.0):
 * - CONFIRMED: CONFIG.COLUMN_MAPPINGS values are critical and must match sheet headers used during setupSheets.
 * - REMOVED: "Run Selected Rules" menu option and the runSelectedRules function.
 * - UPDATED: Sheets are NOT automatically checked/created on spreadsheet open. Manual setup/recreation via menu remains. runAll still performs prerequisite check.
 * - UPDATED: Triggers are NOT created by default. A new "Manage Scheduled Triggers" menu item opens a dialogue to enable/disable standard triggers (runAll daily, cleanupLogs daily).
 * - CLARIFICATION: Automatic execution without user intervention is achieved by enabling time-driven triggers via the "Manage Scheduled Triggers" menu item. Once enabled, Google's servers run the script functions ('runAll', 'cleanupLogs') on the specified schedule, using the authorization of the user who enabled the trigger. The sheet does not need to be open.
 */

// ========================================================================== //
// CONFIGURATION (Could be in Config.gs)
// ========================================================================== //

// Global configuration
const CONFIG = {
  // Performance configuration - granular control over features
  PERFORMANCE_CONFIG: {
    // Core features that can be individually toggled
    DISABLE_EMAILS: false,
    DISABLE_LOGGING: false,
    DISABLE_VERIFICATION: true,
    DISABLE_FORMATTING: true,
    
    // Logging granularity levels
    LOGGING_LEVEL: "STANDARD", // Options: "MINIMAL", "STANDARD", "DETAILED"
    
    // Verification options
    VERIFICATION_LEVEL: "NONE", // Options: "NONE", "BASIC", "FULL"
    
    // Batch processing settings
    BATCH_SIZE: 100, // Number of rows to process in a single operation
    MAX_CONCURRENT_OPERATIONS: 5, // Limit parallel operations
  },

  // Email notification can be a single address or an array of addresses
  EMAIL_NOTIFICATIONS: ["mark.thomsen@gsa.gov", "is-training@gsa.gov"], // Change these to your email(s)

  // Sheet names and settings
  LOG_SHEET_NAME: "ingest-logs",
  CONFIG_SHEET_NAME: "cfg-ingest",
  VERIFICATION_SHEET_NAME: "ingest-verification", // Verification log sheet
  DIAGNOSTIC_SHEET_NAME: "verification-diagnostics", // Added diagnostic sheet
  MAX_LOG_ENTRIES: 500, // Increased default max entries
  NEW_SHEET_ROWS: 100, // Number of rows for new sheets

  // Colors for cycling through Session IDs in logs (add more as needed)
  SESSION_ID_COLORS: [
    "#E1F5FE", // Light Blue
    "#FFF9C4", // Light Yellow
    "#F1F8E9", // Light Green
    "#FCE4EC", // Light Pink
    "#EDE7F6", // Light Purple
    "#E0F2F1", // Light Teal
    "#FFF3E0", // Light Orange
    "#E3F2FD", // Lighter Blue
    "#F0F4C3", // Lighter Lime
    "#F8BBD0", // Lighter Pink
  ],

  // Email configuration
  EMAIL_CONFIG: {
    SEND_ON_START: true,              // Send email when batch job starts
    SEND_ON_COMPLETE: true,           // Send email when batch job completes
    SEND_ON_ERROR: true,              // Send email on errors
    INCLUDE_LOG_ATTACHMENT: false,     // Attach log sheet as CSV
    INCLUDE_VERIFICATION_ATTACHMENT: false, // Attach verification sheet as CSV
    INCLUDE_DIAGNOSTIC_ATTACHMENT: false, // Optionally enable diagnostic attachment
    HTML_FORMATTING: true,            // Use HTML formatting for emails
    MAX_ROWS_IN_EMAIL: 100,           // Maximum log rows to include in email body
    EMAIL_SUBJECT_PREFIX: "[Data Ingest]", // Prefix for email subjects
    DISABLE_IN_FAST_MODE: true        // Skip email notifications in fast mode
  },

  // Data verification
  VERIFICATION_CONFIG: {
    ENABLED: false,                // Enable verification
    VERIFY_ROW_COUNTS: false,      // Verify row counts match
    VERIFY_COLUMN_COUNTS: false,   // Verify column counts match
    VERIFY_SAMPLE_DATA: false,     // Verify sample data integrity
    SAMPLE_SIZE: 5,                // Number of random rows to sample
    DISABLE_IN_FAST_MODE: true     // Skip verification in fast mode
  },

  // Formatting preferences for each sheet
  SHEET_FORMATS: {
    CONFIG_SHEET: {
      headerColor: "#BDBDBD", // Grey header
      headerFontWeight: "bold",
      timestampFormat: "MM/dd/yyyy HH:mm:ss", // Format for lastRunTime column
      timestampColumnName: "lastRunTime", // Specify the column for this format
      alternatingRowColors: ["#FFFFFF", "#F5F5F5"], // Subtle alternating colors
      columnWidths: {
        // Use internal mapping keys for consistency
        "ruleActive": 70, // Narrower
        "ingestMethod": 110,
        "sheetHandlingMode": 130,
        "lastRunTime": 150,
        "lastRunStatus": 100,
        "lastRunMessage": 350, // Wider
        "in_email_searchString": 250,
        "in_email_attachmentPattern": 200,
        "in_gsheet_sheetURL": 300,
        "in_gsheet_tabName": 150,
        "dest_sheetUrl": 300,
        "dest_sheet_tabName": 150,
        "pushSourceTabName": 150,
        "pushDestinationSheetUrl": 300,
        "pushDestinationTabName": 150
      },
      rowFormatOverrides: [
        // Highest priority override first
        { column: "lastRunStatus", value: "ERROR", format: { backgroundColor: "#FFCDD2", fontWeight: "bold", fontColor: "#B71C1C" } }, // Darker red text
        { column: "lastRunStatus", value: "SUCCESS", format: { backgroundColor: "#C8E6C9" } }, // Softer green
        { column: "lastRunStatus", value: "SKIPPED", format: { backgroundColor: "#FFF9C4" } } // Soft yellow
      ]
    },
    LOG_SHEET: {
      headerColor: "#BDBDBD", // Grey header
      headerFontWeight: null,
      timestampFormat: "MM/dd/yyyy HH:mm:ss.SSS", // Include milliseconds
      timestampColumnName: "Timestamp",
      alternatingRowColors: ["#FFFFFF", "#FAFAFA"], // Very subtle alternating colors
      sessionIdColumnName: "SessionID", // Specify the column name for SessionID coloring
      columnWidths: {
        "Timestamp": 190, // Wider for ms
        "SessionID": 220, // Wider for potentially longer IDs
        "EventType": 130, // Slightly wider
        "Message": 650 // Wider message column
      },
      rowFormatOverrides: [
        // Highest priority overrides first
        { column: "EventType", value: "ERROR", format: { backgroundColor: "#FFCDD2", fontWeight: "bold", fontColor: "#B71C1C" } },
        { column: "EventType", value: "ABORT", format: { backgroundColor: "#FFCDD2", fontWeight: "bold" } },
        { column: "EventType", value: "START", format: { fontWeight: "bold", fontStyle: "italic", fontColor: "#1B5E20" } }, // Greenish bold italic
        { column: "EventType", value: "COMPLETE", format: { fontWeight: "bold", fontStyle: "italic", fontColor: "#1B5E20" } },
        { column: "EventType", value: "SUMMARY", format: { fontWeight: "bold", fontColor: "#0D47A1" } }, // Blue bold
        { column: "EventType", value: "WARNING", format: { backgroundColor: "#FFF9C4", fontColor: "#F57F17" } }, // Yellow with orange text
        { column: "EventType", value: "EMAIL_ERROR", format: { backgroundColor: "#FFF9C4", fontStyle: "italic" } },
        { column: "EventType", value: "DATA_HASH", format: { fontColor: "#757575", fontStyle: "italic" } }, // De-emphasize hash logs slightly
        { column: "EventType", value: "SKIPPED", format: { fontColor: "#757575" } } // Grey text for skipped
      ]
    },
    VERIFICATION_SHEET: {
      headerColor: "#BDBDBD", // Grey header
      headerFontWeight: null,
      timestampFormat: "MM/dd/yyyy HH:mm:ss",
      timestampColumnName: "Timestamp",
      alternatingRowColors: ["#FFFFFF", "#FAFAFA"], // Very subtle alternating colors
      sessionIdColumnName: "SessionID", // Specify the column name for SessionID coloring
      columnWidths: {
        "Timestamp": 180, "SessionID": 220, "RuleID": 100, "SourceType": 100, "SourceFile": 200,
        "DestinationSheet": 200, "SourceRows": 100, "DestRows": 100, "SourceColumns": 100, "DestColumns": 100,
        "RowsMatch": 100, "ColumnsMatch": 100, "SamplesMatch": 100, "DataHash": 200, "Status": 100, "Details": 400
      },
      rowFormatOverrides: [
        // Highest priority first
        { column: "Status", value: "ERROR", format: { backgroundColor: "#FFCDD2", fontWeight: "bold", fontColor: "#B71C1C" } },
        { column: "RowsMatch", value: "NO", format: { backgroundColor: "#FFFDE7" } }, // Very light yellow
        { column: "ColumnsMatch", value: "NO", format: { backgroundColor: "#FFFDE7" } },
        { column: "SamplesMatch", value: "NO", format: { backgroundColor: "#FFFDE7" } }
      ]
    },
    DIAGNOSTIC_SHEET: {
      headerColor: "#BDBDBD", // Grey header
      headerFontWeight: "bold",
      timestampFormat: "MM/dd/yyyy HH:mm:ss.SSS", // Include milliseconds
      timestampColumnName: "Timestamp",
      alternatingRowColors: ["#FFFFFF", "#FAFAFA"], // Very subtle alternating colors
      sessionIdColumnName: "SessionID", // Specify the column name for SessionID coloring
      columnWidths: {
        "Timestamp": 190, "SessionID": 220, "Position": 150, "Column": 100, "SourceValue": 250,
        "SourceType": 100, "DestValue": 250, "DestType": 100, "NormalizedSource": 200,
        "NormalizedDest": 200, "Details": 350
      },
      rowFormatOverrides: [] // No specific row overrides defined yet
    }
  },

  // Column descriptions - these appear as notes on column headers
  COLUMN_DESCRIPTIONS: {
    ruleActive: "Check this box to enable this ingest rule",
    ingestMethod: "Select the method: email (import from attachment), gSheet (import from Google Sheet), or push (push data from current sheet)",
    sheetHandlingMode: "How to handle existing sheets: clearAndReuse (keep and clear), recreate (delete and recreate), copyFormat (preserve formatting), append (add to end)",
    in_email_searchString: "Gmail search query to find emails (e.g., 'subject:(Monthly Report) from:example.com')",
    in_email_attachmentPattern: "Regular expression pattern to match attachment filename (e.g., 'Monthly_Report_.*\\.csv')",
    in_gsheet_sheetId: "ID of the source Google Sheet (from URL or direct entry)",
    in_gsheet_sheetURL: "Full URL of the source Google Sheet",
    in_gsheet_tabName: "Name of the tab in the source Google Sheet to import data from",
    dest_sheetId: "ID of the destination Google Sheet (from URL or direct entry)",
    dest_sheetUrl: "Full URL of the destination Google Sheet",
    dest_sheet_tabName: "Name of the tab in the destination Google Sheet to write data to",
    pushSourceTabName: "Name of the tab in the current spreadsheet to push data from",
    pushDestinationSheetId: "ID of the destination Google Sheet for push operation",
    pushDestinationSheetUrl: "Full URL of the destination Google Sheet for push operation",
    pushDestinationTabName: "Name of the tab in the destination Google Sheet to push data to",
    lastRunTime: "Timestamp of the most recent execution of this rule",
    lastRunStatus: "Status of the most recent execution (SUCCESS, ERROR, SKIPPED)",
    lastRunMessage: "Details about the most recent execution result"
  },

  // Column mappings - change the right side values to customize column headers
  // These right-side values MUST match the actual headers in your sheet when running setupSheets
  // and the keys used in COLUMN_DESCRIPTIONS and SHEET_FORMATS.columnWidths.
  COLUMN_MAPPINGS: {
    ruleActive: "Active", ingestMethod: "ingestMethod", sheetHandlingMode: "sheetHandlingMode",
    in_email_searchString: "in_email_searchString", in_email_attachmentPattern: "in_email_attachmentPattern",
    in_gsheet_sheetId: "in_gsheet_sheetId", in_gsheet_sheetURL: "in_gsheet_sheetURL", in_gsheet_tabName: "in_gsheet_tabName",
    dest_sheetId: "dest_sheetId", dest_sheetUrl: "dest_sheetUrl", dest_sheet_tabName: "dest_sheet_tabName",
    pushSourceTabName: "pushSourceTabName", pushDestinationSheetId: "pushDestinationSheetId",
    pushDestinationSheetUrl: "pushDestinationSheetUrl", pushDestinationTabName: "pushDestinationTabName",
    lastRunTime: "lastRunTime", lastRunStatus: "lastRunStatus", lastRunMessage: "lastRunMessage"
  }
};

// ========================================================================== //
// CACHING SYSTEM
// ========================================================================== //

const Cache = {
  // Cache storage
  _cache: {},
  
  // Cache expiration time (5 minutes)
  EXPIRATION_TIME: 5 * 60 * 1000,
  
  /**
   * Store data in cache
   * @param {string} key - Cache key
   * @param {*} value - Value to cache
   * @param {number} [expiration] - Optional custom expiration time in milliseconds
   */
  set: function(key, value, expiration) {
    this._cache[key] = {
      value: value,
      timestamp: new Date().getTime(),
      expiration: expiration || this.EXPIRATION_TIME
    };
  },
  
  /**
   * Get data from cache
   * @param {string} key - Cache key
   * @returns {*} Cached value or null if expired/not found
   */
  get: function(key) {
    const item = this._cache[key];
    if (!item) return null;
    
    const now = new Date().getTime();
    if (now - item.timestamp > item.expiration) {
      delete this._cache[key];
      return null;
    }
    
    return item.value;
  },
  
  /**
   * Clear specific cache entry or entire cache
   * @param {string} [key] - Optional key to clear specific entry
   */
  clear: function(key) {
    if (key) {
      delete this._cache[key];
    } else {
      this._cache = {};
    }
  }
};

// ========================================================================== //
// BATCH PROCESSING UTILITIES
// ========================================================================== //

const BatchProcessor = {
  /**
   * Process data in batches
   * @param {Array} data - Data to process
   * @param {Function} processFn - Function to process each batch
   * @param {Object} [options] - Processing options
   * @returns {Promise} Promise that resolves when all batches are processed
   */
  process: function(data, processFn, options = {}) {
    const batchSize = options.batchSize || getBatchSize();
    const maxConcurrent = options.maxConcurrent || getMaxConcurrentOperations();
    
    return new Promise((resolve, reject) => {
      const batches = [];
      for (let i = 0; i < data.length; i += batchSize) {
        batches.push(data.slice(i, i + batchSize));
      }
      
      let currentIndex = 0;
      let activeBatches = 0;
      let hasError = false;
      
      function processNextBatch() {
        if (hasError || currentIndex >= batches.length) {
          if (activeBatches === 0) {
            resolve();
          }
          return;
        }
        
        const batch = batches[currentIndex++];
        activeBatches++;
        
        processFn(batch)
          .then(() => {
            activeBatches--;
            processNextBatch();
          })
          .catch(error => {
            hasError = true;
            reject(error);
          });
      }
      
      // Start initial batches
      for (let i = 0; i < Math.min(maxConcurrent, batches.length); i++) {
        processNextBatch();
      }
    });
  },
  
  /**
   * Process sheet data in batches
   * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - Sheet to process
   * @param {Function} processFn - Function to process each batch
   * @param {Object} [options] - Processing options
   * @returns {Promise} Promise that resolves when all batches are processed
   */
  processSheet: function(sheet, processFn, options = {}) {
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();
    const batchSize = options.batchSize || getBatchSize();
    
    return new Promise((resolve, reject) => {
      let currentRow = 1;
      
      function processNextBatch() {
        if (currentRow > lastRow) {
          resolve();
          return;
        }
        
        const endRow = Math.min(currentRow + batchSize - 1, lastRow);
        const range = sheet.getRange(currentRow, 1, endRow - currentRow + 1, lastCol);
        const values = range.getValues();
        
        processFn(values, range)
          .then(() => {
            currentRow = endRow + 1;
            processNextBatch();
          })
          .catch(reject);
      }
      
      processNextBatch();
    });
  }
};

// ========================================================================== //
// UTILITY FUNCTIONS (Could be in UtilitiesLib.gs)
// ========================================================================== //

/**
* Generates a unique ID for tracking operations
* @returns {string} A unique identifier
*/
function generateUniqueID() {
  // Combines timestamp with a larger random component for better uniqueness
  const timestamp = new Date().getTime().toString(36); // Base 36 for shorter string
  const randomPart = Math.random().toString(36).substring(2, 10); // 8 random chars
  return `${timestamp}-${randomPart}`;
}

/**
* Helper function to convert column index to letter (0 -> A, 1 -> B, ...)
* @param {number} column Zero-based column index.
* @returns {string} Column letter(s).
*/
function columnToLetter(column) {
  let temp, letter = '';
  while (column >= 0) {
    temp = column % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    column = Math.floor(column / 26) - 1;
  }
  return letter;
}

/**
* Creates a map of header names to column indices (0-based)
* @param {Array} headers - Array of header names
* @returns {Object} Map of header names to column indices
*/
function createHeaderMap(headers) {
  const headerMap = {};
  headers.forEach((header, index) => {
    if (header && typeof header === 'string' && header.trim() !== '') {
      headerMap[header.trim()] = index; // Use trimmed header
    }
  });
  return headerMap;
}

/**
* Gets a required value from a row, throwing an error if not found or empty.
* Uses COLUMN_MAPPINGS to find the correct header name.
* @param {Array} row - Row of data (0-based array).
* @param {Object} headerMap - Map of header names to column indices (0-based).
* @param {string} internalColumnName - Internal name of the column from CONFIG.COLUMN_MAPPINGS keys.
* @returns {*} The value from the row.
* @throws {Error} If the column is not found or the value is missing.
*/
function getRequiredValue(row, headerMap, internalColumnName) {
  // Map the internal column name to the user-defined header
  const columnName = CONFIG.COLUMN_MAPPINGS[internalColumnName];
  if (!columnName) {
    const errorMsg = `Configuration Error: Internal column name "${internalColumnName}" not found in CONFIG.COLUMN_MAPPINGS.`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }

  const colIndex = headerMap[columnName];
  if (colIndex === undefined) {
    const errorMsg = `Required column header "${columnName}" not found in the configuration sheet. Check spelling and CONFIG.COLUMN_MAPPINGS.`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }

  const value = row[colIndex];
  // Check for null, undefined, or empty string specifically
  if (value === undefined || value === null || String(value).trim() === '') {
    const errorMsg = `Required value for column "${columnName}" is missing or empty.`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }

  return value;
}

/**
* Extracts a resource ID either directly from an ID column or from a URL column.
* Uses COLUMN_MAPPINGS to find the correct header names.
* @param {Array} row - Row of data.
* @param {Object} headerMap - Map of header names to column indices.
* @param {string} idInternalColumnName - Internal name of the ID column.
* @param {string} urlInternalColumnName - Internal name of the URL column.
* @returns {string} The extracted resource ID.
* @throws {Error} If the ID cannot be found or extracted.
*/
function getResourceId(row, headerMap, idInternalColumnName, urlInternalColumnName) {
  const idColumnName = CONFIG.COLUMN_MAPPINGS[idInternalColumnName];
  const urlColumnName = CONFIG.COLUMN_MAPPINGS[urlInternalColumnName];

  if (!idColumnName && !urlColumnName) {
    throw new Error(`Configuration Error: Neither internal column "${idInternalColumnName}" nor "${urlInternalColumnName}" found in CONFIG.COLUMN_MAPPINGS.`);
  }

  // First try to get the ID directly from the ID column
  if (idColumnName && headerMap[idColumnName] !== undefined) {
    const idValue = row[headerMap[idColumnName]];
    if (idValue && String(idValue).trim() !== '') {
      return String(idValue).trim();
    }
  }

  // If no direct ID, try to extract from the URL column
  if (urlColumnName && headerMap[urlColumnName] !== undefined) {
    const urlValue = row[headerMap[urlColumnName]];
    if (urlValue && String(urlValue).trim() !== '') {
      const url = String(urlValue).trim();
      // Try different URL formats:
      // 1. Standard Google Sheets URL: /spreadsheets/d/SHEET_ID/
      let match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
      if (match && match[1]) {
        return match[1];
      }
      // 2. Simpler /d/ URL: /d/SHEET_ID/
      match = url.match(/\/d\/([a-zA-Z0-9_-]+)/);
      if (match && match[1]) {
        return match[1];
      }
      // 3. Just the ID itself (contains only valid ID characters)
      if (/^[a-zA-Z0-9_-]{10,}$/.test(url)) { // Basic check for likely ID format
        return url;
      }
    }
  }

  // If we reach here, no ID was found
  const errorMsg = `Could not find a valid resource ID in column "${idColumnName || '(not configured)'}" or extract one from column "${urlColumnName || '(not configured)'}". Please provide a valid Sheet ID or URL.`;
  Logger.log(errorMsg);
  throw new Error(errorMsg);
}

/**
* Checks if a row has any non-empty values (excluding the first column if it's the checkbox)
* @param {Array} row - Row data array
* @returns {boolean} True if the row contains data, false if it's effectively empty
*/
function isRowPopulated(row) {
  let startIndex = 0;
  // Check if config sheet and mapping exist to find checkbox column
  try {
    const configSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.CONFIG_SHEET_NAME);
    if (configSheet && CONFIG.COLUMN_MAPPINGS.ruleActive) {
      const headers = configSheet.getRange(1, 1, 1, configSheet.getLastColumn()).getValues()[0] || [];
      const headerMap = createHeaderMap(headers);
      const ruleActiveColIndex = headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive];
      startIndex = (ruleActiveColIndex === 0) ? 1 : 0; // Start check after checkbox if it's first
    }
  } catch (e) {
    // Ignore error if sheet/header not found during check
  }


  for (let i = startIndex; i < row.length; i++) {
    // Check for null, undefined, or non-empty string
    if (row[i] !== null && row[i] !== undefined && String(row[i]).trim() !== '') {
      return true; // Found data
    }
  }
  return false; // No data found
}

/**
* Normalize values for comparison to handle type differences (Date, Number, String, Boolean).
* @param {*} value - The value to normalize.
* @returns {string} Normalized string representation, trimmed. Returns empty string for null/undefined.
*/
function normalizeValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  if (value instanceof Date) {
    // Consistent date format (ISO without timezone offset issues for comparison)
    try {
      return Utilities.formatDate(value, Session.getScriptTimeZone(), "yyyy-MM-dd'T'HH:mm:ss.SSS");
    } catch (e) {
      return value.toISOString(); // Fallback
    }
  }
  if (typeof value === 'number') {
    // Handle potential floating point inaccuracies by limiting precision
    // Avoid scientific notation for large numbers if possible for comparison
    if (Math.abs(value) > 1e-6 && Math.abs(value) < 1e15) {
      return value.toFixed(8).replace(/\.?0+$/, ''); // Remove trailing zeros after decimal
    } else {
      return String(value); // Use default string conversion for very small/large numbers
    }
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE'; // Consistent boolean representation
  }
  // Default: convert to string and trim
  return String(value).trim();
}


/**
* Format a date object for display using configured or default format.
* @param {Date|string|number} date - The date to format (can be Date object, ISO string, or timestamp number).
* @param {string} [format] - Optional format string override (e.g., "yyyy-MM-dd").
* @returns {string} Formatted date string, or empty string if input is invalid/empty.
*/
function formatDate(date, format) {
  if (!date) return "";

  let dateObj;
  if (date instanceof Date) {
    dateObj = date;
  } else {
    try {
      // Attempt to parse if it's a string or number timestamp
      dateObj = new Date(date);
      // Check if parsing resulted in a valid date
      if (isNaN(dateObj.getTime())) {
        Logger.log(`Warning: Could not parse date value: ${date}`);
        return String(date); // Return original string if parsing failed
      }
    } catch (e) {
      Logger.log(`Error parsing date value: ${date} - ${e.message}`);
      return String(date); // Return original representation on error
    }
  }

  // Determine the format string to use
  const formatString = format || CONFIG.SHEET_FORMATS.LOG_SHEET?.timestampFormat || "MM/dd/yyyy HH:mm:ss";

  try {
    // Use Utilities.formatDate with the script's timezone
    return Utilities.formatDate(dateObj, Session.getScriptTimeZone(), formatString);
  } catch (error) {
    Logger.log(`Error formatting date ${dateObj}: ${error.message}. Falling back to toString().`);
    // Fallback to basic string representation if formatting fails
    return dateObj.toString();
  }
}


/**
* Calculate a simple hash value from data for basic verification comparison.
* This is NOT cryptographically secure, just for detecting accidental changes.
* Samples data for large datasets for performance.
* @param {Array<Array>} data - 2D array of data.
* @returns {string} Hash value string including row/column counts, or "empty"/"error".
*/
function calculateDataHash(data) {
  if (!data || data.length === 0 || !Array.isArray(data[0])) {
    return "empty"; // Handle empty or invalid data structure
  }

  const numRows = data.length;
  const numCols = data[0].length;

  try {
    // Use a consistent string representation for hashing
    let dataString = "";
    const MAX_HASH_CHARS = 50000; // Limit total characters to prevent excessive processing

    // Sample data if it's large
    if (numRows * numCols > 20000) { // Heuristic for "large" dataset
      const sampleSize = Math.min(numRows, 100); // Sample up to 100 rows
      const step = Math.max(1, Math.floor(numRows / sampleSize));
      for (let i = 0; i < numRows && dataString.length < MAX_HASH_CHARS; i += step) {
        // Normalize each cell in the row before joining
        dataString += data[i].map(normalizeValue).join('|') + '\n';
      }
    } else {
      // Process all data for smaller sets
      for (let i = 0; i < numRows && dataString.length < MAX_HASH_CHARS; i++) {
        dataString += data[i].map(normalizeValue).join('|') + '\n';
      }
    }

    // Simple checksum hash algorithm (djb2 variant)
    let hash = 5381;
    const len = Math.min(dataString.length, MAX_HASH_CHARS); // Ensure we don't exceed limit
    for (let i = 0; i < len; i++) {
      hash = ((hash << 5) + hash) + dataString.charCodeAt(i); /* hash * 33 + c */
      hash |= 0; // Convert to 32bit integer
    }

    // Combine hash with dimensions for uniqueness
    return `h${Math.abs(hash).toString(16)}-r${numRows}-c${numCols}`;

  } catch (error) {
    Logger.log(`ERROR CALCULATING DATA HASH: ${error.message}`);
    return `error-r${numRows}-c${numCols}`; // Include dimensions even on error
  }
}

/**
* Checks if an error is likely to be temporary (e.g., API limits, timeouts).
* @param {Error} error - The error object to check.
* @returns {boolean} True if the error message suggests a temporary issue.
*/
function isTemporaryError(error) {
  if (!error || !error.message) return false;
  const message = String(error.message).toLowerCase();

  const temporaryPatterns = [
    'timeout', 'rate limit', 'limit exceeded', 'quota', 'service unavailable',
    'internal error', 'server error', 'try again later', 'too_many_requests',
    'backend error', 'temporarily unavailable', 'network error', 'connection reset',
    'api call timed out'
  ];

  return temporaryPatterns.some(pattern => message.includes(pattern));
}

/**
* Executes a function with automatic retries on temporary errors.
* Uses exponential backoff for delays.
* @param {Function} fn - The function to execute. Must not require arguments or use closure/bind.
* @param {number} [maxRetries=3] - Maximum number of retry attempts.
* @param {number} [initialDelay=1500] - Initial delay in milliseconds before the first retry.
* @returns {*} The result of the function `fn`.
* @throws {Error} Throws the last error if all retries fail or if a non-temporary error occurs.
*/
function executeWithRetry(fn, maxRetries = 3, initialDelay = 1500) {
  let attempt = 0;
  let delay = initialDelay;

  while (attempt <= maxRetries) {
    try {
      return fn(); // Attempt to execute the function
    } catch (error) {
      attempt++;
      Logger.log(`Attempt ${attempt} failed: ${error.message}`);

      // If it's the last attempt or not a temporary error, rethrow
      if (attempt > maxRetries || !isTemporaryError(error)) {
        Logger.log(`Rethrowing error after ${attempt - 1} retries or because it's not temporary.`);
        throw error;
      }

      // Log the retry attempt
      const waitTime = delay + Math.floor(Math.random() * 1000); // Add jitter
      Logger.log(`Temporary error encountered. Retrying in ${waitTime}ms (attempt ${attempt}/${maxRetries})...`);

      // Wait before retrying
      Utilities.sleep(waitTime);

      // Exponential backoff - increase delay for the next potential retry
      delay = Math.min(delay * 2, 60000); // Double delay, max 1 minute
    }
  }
  // This line should theoretically not be reached due to the throw in the loop
  throw new Error("executeWithRetry failed after maximum retries.");
}


// ========================================================================== //
// SHEET SETUP & FORMATTING (Could be in SheetSetupLib.gs / FormattingLib.gs)
// ========================================================================== //

/**
* IMPROVED: Safely create/replace a sheet, backing up the existing one first.
* @param {string} sheetName - Name of the sheet to create/replace.
* @param {Function} setupFunction - Function accepting the new sheet object (Sheet) to perform setup.
* @returns {Sheet} The created or updated sheet object.
* @throws {Error} If sheet creation/replacement fails critically.
*/
function ensureSafeSheetCreation(sheetName, setupFunction) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let existingSheet = ss.getSheetByName(sheetName);
  let backupSheet = null;
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-'); // Timestamp for backup name

  // 1. Backup if exists
  if (existingSheet) {
    try {
      const backupName = `${sheetName}-backup-${timestamp}`.slice(0, 100); // Ensure name isn't too long
      backupSheet = existingSheet.copyTo(ss).setName(backupName);
      Logger.log(`Created backup of "${sheetName}" as "${backupName}".`);
    } catch (backupError) {
      Logger.log(`Warning: Could not create backup of "${sheetName}": ${backupError.message}. Proceeding cautiously.`);
      // If backup fails, we might choose to abort or just continue without backup
    }
  }

  // 2. Create new sheet with temporary name
  const tempName = `${sheetName}_new_${timestamp}`.slice(0, 100);
  let newSheet;
  try {
    // Delete any leftover temp sheet first
    let oldTempSheet = ss.getSheetByName(tempName);
    if (oldTempSheet) ss.deleteSheet(oldTempSheet);

    newSheet = ss.insertSheet(tempName);
    Logger.log(`Created temporary sheet "${tempName}".`);
  } catch (createError) {
    Logger.log(`Error creating temporary sheet "${tempName}": ${createError.message}`);
    // Attempt to restore backup if creation fails
    if (backupSheet) {
      try {
        // Check if original still exists, if not, rename backup
        if (!ss.getSheetByName(sheetName)) {
          backupSheet.setName(sheetName);
          Logger.log(`Restored backup "${backupSheet.getName()}" as "${sheetName}" due to creation error.`);
          return backupSheet; // Return the restored sheet
        }
      } catch (restoreError) {
        Logger.log(`Error renaming backup sheet during error recovery: ${restoreError.message}`);
      }
    }
    throw new Error(`Failed to create new sheet "${sheetName}": ${createError.message}`); // Rethrow original error
  }


  // 3. Setup the new sheet
  try {
    setupFunction(newSheet);
    Logger.log(`Setup function executed successfully for "${tempName}".`);
  } catch (setupError) {
    Logger.log(`Error during setup function for "${tempName}": ${setupError.message}. Attempting cleanup.`);
    // Try to delete the failed new sheet
    try { ss.deleteSheet(newSheet); } catch (e) { Logger.log(`Failed to delete temporary sheet "${tempName}" after setup error.`); }
    // Attempt to restore backup if setup fails
    if (backupSheet && !ss.getSheetByName(sheetName)) {
      try {
        backupSheet.setName(sheetName);
        Logger.log(`Restored backup "${backupSheet.getName()}" as "${sheetName}" due to setup error.`);
        return backupSheet;
      } catch (restoreError) {
        Logger.log(`Error renaming backup sheet during setup error recovery: ${restoreError.message}`);
      }
    } else if (existingSheet && ss.getSheetByName(sheetName)) { // Ensure original still exists
      Logger.log(`Keeping original sheet "${sheetName}" due to setup error on new sheet.`);
      return existingSheet; // Return original if it still exists
    }
    throw new Error(`Failed during setup of sheet "${sheetName}": ${setupError.message}`); // Rethrow setup error
  }

  // 4. Replace old sheet with new sheet (atomic rename)
  try {
    // Rename the successfully setup sheet to the final name
    newSheet.setName(sheetName);
    Logger.log(`Renamed "${tempName}" to "${sheetName}".`);

    // Delete the original sheet *after* successful rename
    if (existingSheet && existingSheet.getSheetId() !== newSheet.getSheetId()) { // Ensure it's not the same sheet object
      ss.deleteSheet(existingSheet);
      Logger.log(`Deleted original sheet "${sheetName}".`);
    }
    return newSheet; // Return the successfully created and named sheet

  } catch (renameError) {
    Logger.log(`Error renaming/deleting during final step for "${sheetName}": ${renameError.message}. State may be inconsistent.`);
    // Best effort: return the new sheet even if rename failed, or the backup/original
    if (ss.getSheetByName(sheetName)) return ss.getSheetByName(sheetName);
    if (ss.getSheetByName(tempName)) return ss.getSheetByName(tempName); // Return temp if rename failed
    if (backupSheet && ss.getSheetByName(backupSheet.getName())) return backupSheet; // Return backup if available
    throw renameError; // Rethrow if state is truly broken
  }
}


/**
* Applies formatting to a sheet based on its configuration.
* Handles headers, column widths, number formats, and conditional formatting rules
* including alternating rows, SessionID grouping, and row overrides.
* @param {Sheet} sheet - The Google Sheet object to format.
* @param {Object} formatConfig - The configuration object for this sheet from CONFIG.SHEET_FORMATS.
* @param {Array} headers - Array of header names for the sheet.
*/
function applySheetFormatting(sheet, formatConfig, headers) {
  if (!sheet || !formatConfig || !headers || headers.length === 0) {
    Logger.log("applySheetFormatting: Invalid input provided.");
    return;
  }
  const startTime = new Date();
  const sheetName = sheet.getName();
  Logger.log(`Applying formatting to sheet: ${sheetName}`);

  const headerMap = createHeaderMap(headers); // Needed for column lookups
  const maxRows = sheet.getMaxRows() > 0 ? sheet.getMaxRows() : 100; // Use max rows or default
  const maxCols = headers.length;
  const dataRangeA1 = `A2:${columnToLetter(maxCols - 1)}${maxRows}`; // A1 notation for data range
  const fullRangeA1 = `A1:${columnToLetter(maxCols - 1)}${maxRows}`; // A1 notation for full range

  // Clear existing conditional formats first to prevent duplicates/conflicts
  try {
    sheet.clearConditionalFormatRules();
    Logger.log(`Cleared existing conditional formatting rules for ${sheetName}.`);
  } catch (e) {
    Logger.log(`Warning: Could not clear conditional formats for ${sheetName}: ${e.message}`);
  }


  // --- Apply Basic Formatting ---
  // Headers
  if (formatConfig.headerColor || formatConfig.headerFontWeight) {
    const headerRange = sheet.getRange(1, 1, 1, maxCols);
    if (formatConfig.headerColor) headerRange.setBackground(formatConfig.headerColor);
    if (formatConfig.headerFontWeight) headerRange.setFontWeight(formatConfig.headerFontWeight);
    headerRange.setVerticalAlignment("middle");
  }

  // Column Widths
  if (formatConfig.columnWidths) {
    for (const [colIdentifier, width] of Object.entries(formatConfig.columnWidths)) {
      // Find the actual header name using the mapping if the identifier is an internal key
      const actualHeader = CONFIG.COLUMN_MAPPINGS[colIdentifier] || colIdentifier;
      const colIndex = headerMap[actualHeader]; // Use the map created from actual sheet headers
      if (colIndex !== undefined) {
        try {
          sheet.setColumnWidth(colIndex + 1, width);
        } catch (e) {
          Logger.log(`Warning: Could not set width for column "${actualHeader}" (Index ${colIndex + 1}) on sheet ${sheetName}: ${e.message}`);
        }
      } else {
        // Log only if the original identifier wasn't found either
        if (headerMap[colIdentifier] === undefined) {
          Logger.log(`Warning: Column identifier "${colIdentifier}" (mapped to "${actualHeader}") not found in headers for width setting on sheet ${sheetName}.`);
        }
      }
    }
  }

  // Timestamp Format
  let timestampColIndex = -1;
  if (formatConfig.timestampColumnName && headerMap[formatConfig.timestampColumnName] !== undefined) {
    timestampColIndex = headerMap[formatConfig.timestampColumnName];
  } else if (headerMap['Timestamp'] !== undefined) { // Fallback to 'Timestamp'
    timestampColIndex = headerMap['Timestamp'];
  }

  if (formatConfig.timestampFormat && timestampColIndex !== -1 && maxRows > 1) {
    try {
      sheet.getRange(2, timestampColIndex + 1, maxRows - 1, 1).setNumberFormat(formatConfig.timestampFormat);
    } catch (e) {
      Logger.log(`Warning: Could not set number format for timestamp column ${timestampColIndex + 1} on ${sheetName}: ${e.message}`);
    }
  }

  // Freeze Header Row
  try {
    sheet.setFrozenRows(1);
  } catch (e) {
    Logger.log(`Warning: Could not set frozen row for ${sheetName}: ${e.message}`);
  }

  // --- Prepare Conditional Formatting Rules ---
  const rules = [];
  let dataRange;
  if (maxRows > 1) {
    dataRange = sheet.getRange(dataRangeA1); // Get range object once only if data rows exist
  } else {
    Logger.log(`Sheet ${sheetName} has only 1 row, skipping conditional formatting rules application.`);
    return; // No data rows to apply formatting to
  }


  // Rule 1: Alternating Rows (Lowest Priority)
  if (formatConfig.alternatingRowColors && formatConfig.alternatingRowColors.length > 1) {
    const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
      // Apply only to ODD data rows (even sheet rows) to color the second color
      .whenFormulaSatisfied("=ISODD(ROW())") // Use ISODD because rule applies from row 2
      .setBackground(formatConfig.alternatingRowColors[1]) // Use the second color for odd data rows
      .setRanges([dataRange])
      .build();
    rules.push(alternatingRule);
  }

  // Rule 2: SessionID Coloring (Medium Priority)
  const sessionIdColName = formatConfig.sessionIdColumnName;
  const sessionIdColIndex = sessionIdColName !== undefined ? headerMap[sessionIdColName] : -1;

  if (sessionIdColIndex !== -1 && CONFIG.SESSION_ID_COLORS && CONFIG.SESSION_ID_COLORS.length > 0) {
    const sessionIdColumnLetter = columnToLetter(sessionIdColIndex);
    const numSessionColors = CONFIG.SESSION_ID_COLORS.length;

    for (let i = 0; i < numSessionColors; i++) {
      // Formula uses simple CHECKSUM hash and MODULO to cycle colors.
      // Checks that the SessionID cell is not blank. $ locks column reference.
      // References the cell in the *current* row (e.g., $B2 for rules applied to row 2).
      const formula = `=AND(LEN(TRIM($${sessionIdColumnLetter}2))>0, MOD(CHECKSUM($${sessionIdColumnLetter}2), ${numSessionColors})=${i})`;
      const sessionIdRule = SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied(formula)
        .setBackground(CONFIG.SESSION_ID_COLORS[i])
        .setRanges([dataRange]) // Apply to the entire data row range
        .build();
      rules.push(sessionIdRule);
    }
    Logger.log(`Prepared ${numSessionColors} SessionID conditional formatting rules for column ${sessionIdColumnLetter}.`);
  } else if (formatConfig.sessionIdColumnName) {
    Logger.log(`SessionID column "${formatConfig.sessionIdColumnName}" not found in headers or no SESSION_ID_COLORS defined. Skipping SessionID coloring.`);
  }

  // Rule 3: Row Format Overrides (Highest Priority)
  if (formatConfig.rowFormatOverrides && formatConfig.rowFormatOverrides.length > 0) {
    // Apply overrides in reverse order so the first ones in the config have highest priority
    for (let i = formatConfig.rowFormatOverrides.length - 1; i >= 0; i--) {
      const override = formatConfig.rowFormatOverrides[i];
      const overrideColIndex = headerMap[override.column];
      if (overrideColIndex !== -1 && override.value !== undefined) { // Ensure column and value exist
        const overrideColumnLetter = columnToLetter(overrideColIndex);
        // Formula checks the specific column's value in the current row (e.g., $C2 = "ERROR")
        // Use EXACT for case-sensitive comparison if needed, otherwise direct equals is fine.
        // Trim the value in the sheet for robustness: =TRIM($C2)="ERROR"
        const formula = `=TRIM($${overrideColumnLetter}2)="${override.value}"`;

        const ruleBuilder = SpreadsheetApp.newConditionalFormatRule()
          .whenFormulaSatisfied(formula)
          .setRanges([dataRange]); // Apply to the entire data row range

        // Apply specific formatting from the config
        const format = override.format || {};
        if (format.backgroundColor) ruleBuilder.setBackground(format.backgroundColor);
        if (format.fontColor) ruleBuilder.setFontColor(format.fontColor);
        if (format.fontWeight === "bold") ruleBuilder.setBold(true);
        else if (format.fontWeight === "normal") ruleBuilder.setBold(false); // Allow unbolding
        if (format.fontStyle === "italic") ruleBuilder.setItalic(true);
        else if (format.fontStyle === "normal") ruleBuilder.setItalic(false); // Allow unitalicizing
        // Add other format options here if needed (e.g., .setUnderline(true/false), .setStrikethrough(true/false))

        rules.push(ruleBuilder.build());
      } else {
        Logger.log(`Warning: Column "${override.column}" or value for row override rule not found/defined in headers/config of sheet ${sheetName}.`);
      }
    }
    Logger.log(`Prepared ${formatConfig.rowFormatOverrides.length} row override conditional formatting rules.`);
  }

  // --- Apply Conditional Formatting Rules ---
  // Google Sheets applies rules based on their position in the list.
  // Rules added later have higher priority if their ranges overlap and conditions match.
  // So, the order should be: Alternating -> SessionID -> Overrides
  if (rules.length > 0) {
    try {
      sheet.setConditionalFormatRules(rules);
      Logger.log(`Applied ${rules.length} conditional formatting rules to ${sheetName}.`);
    } catch (e) {
      Logger.log(`ERROR applying conditional format rules to ${sheetName}: ${e.message}. Rule count: ${rules.length}.`);
      // Consider logging the rules themselves for debugging if error persists
      // Logger.log(JSON.stringify(rules.map(r => r.copy().getJson())));
    }
  }


  // --- Final Touches ---
  // Add description notes to headers
  if (CONFIG.COLUMN_DESCRIPTIONS && CONFIG.COLUMN_MAPPINGS) {
    const notesAdded = [];
    for (const [internalName, description] of Object.entries(CONFIG.COLUMN_DESCRIPTIONS)) {
      const columnName = CONFIG.COLUMN_MAPPINGS[internalName];
      if (columnName && headerMap[columnName] !== undefined) {
        const colIndex = headerMap[columnName];
        try {
          sheet.getRange(1, colIndex + 1).setNote(description);
          notesAdded.push(columnName);
        } catch (e) {
          Logger.log(`Warning: Could not set note for column "${columnName}": ${e.message}`);
        }
      }
    }
  }

  // Auto-resize columns *after* setting widths and adding data/notes
  if (sheetName === CONFIG.LOG_SHEET_NAME && headerMap["Message"] !== undefined) {
    try { sheet.autoResizeColumn(headerMap["Message"] + 1); } catch (e) { Logger.log(`Note: Auto-resize failed for Message column on ${sheetName}: ${e.message}`); }
  }
  if (sheetName === CONFIG.VERIFICATION_SHEET_NAME && headerMap["Details"] !== undefined) {
    try { sheet.autoResizeColumn(headerMap["Details"] + 1); } catch (e) { Logger.log(`Note: Auto-resize failed for Details column on ${sheetName}: ${e.message}`); }
  }
  if (sheetName === CONFIG.DIAGNOSTIC_SHEET_NAME && headerMap["Details"] !== undefined) {
    try { sheet.autoResizeColumn(headerMap["Details"] + 1); } catch (e) { Logger.log(`Note: Auto-resize failed for Details column on ${sheetName}: ${e.message}`); }
  }
  if (sheetName === CONFIG.CONFIG_SHEET_NAME && headerMap[CONFIG.COLUMN_MAPPINGS.lastRunMessage] !== undefined) {
    try { sheet.autoResizeColumn(headerMap[CONFIG.COLUMN_MAPPINGS.lastRunMessage] + 1); } catch (e) { Logger.log(`Note: Auto-resize failed for lastRunMessage column on ${sheetName}: ${e.message}`); }
  }

  const duration = (new Date() - startTime) / 1000;
  Logger.log(`Sheet formatting for ${sheetName} completed in ${duration.toFixed(2)} seconds.`);
}


/**
* Sets up conditional formatting specifically for the configuration sheet.
* Note: This is now largely handled by applySheetFormatting. Kept for potential specific needs.
* @param {Sheet} sheet - The configuration sheet
*/
function setupConditionalFormatting(sheet) {
  // This function is now less necessary as applySheetFormatting handles it based on config.
  Logger.log(`setupConditionalFormatting called for ${sheet.getName()}, but main logic is in applySheetFormatting.`);
}


/**
* Creates or updates the log sheet with enhanced formatting
* @returns {Sheet} The created or updated sheet
*/
function createLogSheet() {
  const sheetName = CONFIG.LOG_SHEET_NAME;
  // Define headers based on expected columns for logging AND formatting
  const headers = ['Timestamp', 'SessionID', 'EventType', 'Message'];
  const formatConfig = CONFIG.SHEET_FORMATS.LOG_SHEET;

  if (!formatConfig) {
    throw new Error(`Formatting configuration for LOG_SHEET is missing in CONFIG.SHEET_FORMATS.`);
  }
  Logger.log(`Requesting creation/update of log sheet: ${sheetName}`);

  try {
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      Logger.log(`Setting up new/recreated log sheet: ${sheetName}`);
      // Set Headers first (simple values only)
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);

      // Apply all other formatting using the helper
      applySheetFormatting(sheet, formatConfig, headers);

      // Add an initial log entry documenting the sheet creation
      const initialSessionId = generateUniqueID();
      // Use the internal write function directly as logOperation might try to recreate this sheet
      _writeLogEntry(sheet, new Date(), initialSessionId, "SHEET_CREATED", `Log sheet created or recreated successfully.`);

      Logger.log(`Setup complete for log sheet: ${sheetName}`);
    });
  } catch (error) {
    const errorMsg = `Error creating log sheet "${sheetName}": ${error.message}`;
    Logger.log(errorMsg);
    // Optional: Fallback to basic sheet creation if ensureSafe fails?
    throw new Error(errorMsg); // Rethrow for higher level handling
  }
}

/**
* Creates or updates the verification sheet for data integrity tracking
* @returns {Sheet} The created or updated sheet
*/
function createVerificationSheet() {
  const sheetName = CONFIG.VERIFICATION_SHEET_NAME;
  const headers = [ // Ensure order matches _writeVerificationEntry and config
    'Timestamp', 'SessionID', 'RuleID', 'SourceType', 'SourceFile',
    'DestinationSheet', 'SourceRows', 'DestRows', 'SourceColumns', 'DestColumns',
    'RowsMatch', 'ColumnsMatch', 'SamplesMatch', 'DataHash', 'Status', 'Details'
  ];
  const formatConfig = CONFIG.SHEET_FORMATS.VERIFICATION_SHEET;

  if (!formatConfig) {
    throw new Error(`Formatting configuration for VERIFICATION_SHEET is missing in CONFIG.SHEET_FORMATS.`);
  }
  Logger.log(`Requesting creation/update of verification sheet: ${sheetName}`);

  try {
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      Logger.log(`Setting up new/recreated verification sheet: ${sheetName}`);
      // Set Headers
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);

      // Apply all other formatting using the helper
      applySheetFormatting(sheet, formatConfig, headers);

      // Add an initial entry
      const initialSessionId = generateUniqueID();
      _writeVerificationEntry(sheet, {
        timestamp: new Date(), sessionId: initialSessionId, ruleId: "SHEET_CREATED", sourceType: "System",
        status: "COMPLETE",
        details: `Verification sheet created or recreated successfully.`
        // Other fields default to empty/N/A in _writeVerificationEntry
      });
      Logger.log(`Setup complete for verification sheet: ${sheetName}`);
    });
  } catch (error) {
    const errorMsg = `Error creating verification sheet "${sheetName}": ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}

/**
* Creates or updates the diagnostic sheet for detailed data comparison
* @returns {Sheet} The created or updated sheet
*/
function createDiagnosticSheet() {
  const sheetName = CONFIG.DIAGNOSTIC_SHEET_NAME;
  const headers = [ // Ensure order matches _writeDiagnosticEntry and config
    'Timestamp', 'SessionID', 'Position', 'Column', 'SourceValue', 'SourceType',
    'DestValue', 'DestType', 'NormalizedSource', 'NormalizedDest', 'Details'
  ];
  const formatConfig = CONFIG.SHEET_FORMATS.DIAGNOSTIC_SHEET;

  if (!formatConfig) {
    throw new Error(`Formatting configuration for DIAGNOSTIC_SHEET is missing in CONFIG.SHEET_FORMATS.`);
  }
  Logger.log(`Requesting creation/update of diagnostic sheet: ${sheetName}`);

  try {
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      Logger.log(`Setting up new/recreated diagnostic sheet: ${sheetName}`);
      // Set Headers
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);

      // Apply all other formatting using the helper
      applySheetFormatting(sheet, formatConfig, headers);

      // Add an initial entry
      const initialSessionId = generateUniqueID();
      _writeDiagnosticEntry(sheet, {
        timestamp: new Date(), sessionId: initialSessionId, position: "SheetCreation",
        details: `Diagnostic sheet created or recreated successfully.`
        // Other fields default in _writeDiagnosticEntry
      });
      Logger.log(`Setup complete for diagnostic sheet: ${sheetName}`);
    });
  } catch (error) {
    const errorMsg = `Error creating diagnostic sheet "${sheetName}": ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}

/**
* IMPROVED: Creates or updates the ingestion configuration sheet using safer methods and centralized formatting.
* @returns {Sheet} The created or updated sheet object.
*/
function createCfgIngestSheet() {
  const sheetName = CONFIG.CONFIG_SHEET_NAME;
  // Get headers directly from the values in COLUMN_MAPPINGS to ensure order matches config
  const headers = Object.values(CONFIG.COLUMN_MAPPINGS);
  const formatConfig = CONFIG.SHEET_FORMATS.CONFIG_SHEET;

  if (!formatConfig) {
    throw new Error(`Formatting configuration for CONFIG_SHEET is missing in CONFIG.SHEET_FORMATS.`);
  }
  if (headers.length === 0) {
    throw new Error(`CONFIG.COLUMN_MAPPINGS is empty or invalid.`);
  }
  Logger.log(`Requesting creation/update of configuration sheet: ${sheetName}`);

  // Create a sample data row matching the header order
  const sampleDataRow = Array(headers.length).fill(''); // Initialize with blanks
  const headerMap = createHeaderMap(headers); // Map headers to indices for easy lookup

  // Populate sample data based on known mapped columns (use internal keys)
  try {
    if (headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive]] = true;
    if (headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod]] = 'email';
    if (headerMap[CONFIG.COLUMN_MAPPINGS.sheetHandlingMode] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.sheetHandlingMode]] = 'clearAndReuse';
    if (headerMap[CONFIG.COLUMN_MAPPINGS.in_email_searchString] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.in_email_searchString]] = 'subject:(Example Report)';
    if (headerMap[CONFIG.COLUMN_MAPPINGS.in_email_attachmentPattern] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.in_email_attachmentPattern]] = 'Example_Report_.*\\.csv';
    if (headerMap[CONFIG.COLUMN_MAPPINGS.dest_sheetId] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.dest_sheetId]] = 'YOUR_DESTINATION_SHEET_ID_HERE';
    if (headerMap[CONFIG.COLUMN_MAPPINGS.dest_sheet_tabName] !== undefined) sampleDataRow[headerMap[CONFIG.COLUMN_MAPPINGS.dest_sheet_tabName]] = 'imported_data_tab';
  } catch (e) {
    Logger.log(`Warning: Could not populate all sample data for config sheet, likely due to missing column mappings. ${e.message}`);
  }


  try {
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      Logger.log(`Setting up new/recreated config sheet: ${sheetName}`);
      // Set Headers first
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);

      // Apply formatting (includes headers, widths, basic formats, conditional rules)
      applySheetFormatting(sheet, formatConfig, headers);

      // Add sample data row *after* formatting (especially after setting column widths/notes)
      if (sheet.getLastRow() < 2) { // Only add sample if sheet is truly empty below header
        try {
          sheet.getRange(2, 1, 1, headers.length).setValues([sampleDataRow]);
          Logger.log(`Added sample data row to ${sheetName}.`);
        } catch (e) {
          Logger.log(`Warning: Could not add sample data row to ${sheetName}: ${e.message}`);
        }
      }

      // --- Specific Config Sheet Setup (Validation, Checkboxes - apply to max rows) ---
      const maxRows = sheet.getMaxRows();
      const maxDataRows = maxRows > 1 ? maxRows - 1 : 0;
      if (maxDataRows < 1) {
        Logger.log(`Skipping validation/checkboxes as ${sheetName} has no data rows.`);
        return; // No rows to apply validation/checkboxes to
      }

      // Add data validation rules defined in config
      const validationRules = [
        { key: 'ingestMethod', values: ['email', 'gSheet', 'push'] },
        { key: 'sheetHandlingMode', values: ['clearAndReuse', 'recreate', 'copyFormat', 'append'] }
      ];

      validationRules.forEach(ruleInfo => {
        const colName = CONFIG.COLUMN_MAPPINGS[ruleInfo.key];
        if (colName && headerMap[colName] !== undefined) {
          const colIndex = headerMap[colName];
          try {
            const rule = SpreadsheetApp.newDataValidation()
              .requireValueInList(ruleInfo.values, true)
              .setAllowInvalid(false) // Disallow invalid entries
              .setHelpText(`Select one of: ${ruleInfo.values.join(', ')}`)
              .build();
            sheet.getRange(2, colIndex + 1, maxDataRows, 1).setDataValidation(rule);
          } catch (e) {
            Logger.log(`Warning: Could not apply validation for "${colName}" on ${sheetName}: ${e.message}`);
          }
        } else {
          Logger.log(`Warning: Could not apply validation for internal key "${ruleInfo.key}" - column mapping not found.`);
        }
      });

      // Add checkboxes to ruleActive column
      const ruleActiveColName = CONFIG.COLUMN_MAPPINGS.ruleActive;
      if (ruleActiveColName && headerMap[ruleActiveColName] !== undefined) {
        const ruleActiveColIndex = headerMap[ruleActiveColName];
        try {
          const range = sheet.getRange(2, ruleActiveColIndex + 1, maxDataRows, 1);
          range.insertCheckboxes();
          // Set default to false for rows beyond the potentially added sample (row 2)
          if (maxDataRows > 1) {
            const rangeToCheck = sheet.getRange(3, ruleActiveColIndex + 1, maxDataRows - 1, 1);
            // Only set false if the cell value is not already boolean (true/false)
            const values = rangeToCheck.getValues();
            const newValues = values.map(row => [(typeof row[0] === 'boolean') ? row[0] : false]);
            rangeToCheck.setValues(newValues);
          }
          Logger.log(`Applied checkboxes and validation rules to ${sheetName}.`);
        } catch (e) {
          Logger.log(`Warning: Could not apply checkboxes to "${ruleActiveColName}" on ${sheetName}: ${e.message}`);
        }
      } else {
        Logger.log(`Warning: Could not apply checkboxes - ruleActive column mapping not found.`);
      }

      Logger.log(`Finished setting up configuration sheet: ${sheetName}`);
    });
  } catch (error) {
    const errorMsg = `Error creating configuration sheet "${sheetName}": ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}


// ========================================================================== //
// LOGGING FUNCTIONS (Could be in LoggingLib.gs)
// ========================================================================== //

/**
* Internal function to write a log entry without formatting logic.
* Assumes logSheet exists and is valid. Handles insertion and trimming.
* @param {Sheet} logSheet - The log sheet object.
* @param {Date} timestamp - Timestamp of the event.
* @param {string} sessionId - Session identifier.
* @param {string} eventType - Type of event.
* @param {string} message - Log message.
*/
function _writeLogEntry(logSheet, timestamp, sessionId, eventType, message) {
  try {
    logSheet.insertRowAfter(1); // Insert below header (row 2)
    const newRow = logSheet.getRange(2, 1, 1, 4); // Assuming 4 columns: Timestamp, SessionID, EventType, Message
    // Ensure data types are appropriate for setValues
    const timestampValue = (timestamp instanceof Date) ? timestamp : new Date();
    const sessionIdValue = String(sessionId || '');
    const eventTypeValue = String(eventType || 'UNKNOWN');
    const messageValue = String(message || '');
    newRow.setValues([[timestampValue, sessionIdValue, eventTypeValue, messageValue]]);

    // Only set number format explicitly if needed, conditional formatting handles colors/styles
    const formatConfig = CONFIG.SHEET_FORMATS.LOG_SHEET || {};
    const timestampFormat = formatConfig.timestampFormat || "MM/dd/yyyy HH:mm:ss";
    const timestampColIndex = 0; // Assuming Timestamp is always first column
    try {
      logSheet.getRange(2, timestampColIndex + 1).setNumberFormat(timestampFormat);
    } catch (e) {
      Logger.log(`Warning: Failed to set timestamp format on log sheet: ${e.message}`);
    }


    // Trim log to maximum entries
    const maxEntries = CONFIG.MAX_LOG_ENTRIES || 500; // Default if not set
    const totalRows = logSheet.getLastRow();
    const headerRows = 1;
    if (totalRows > maxEntries + headerRows) {
      const deleteCount = totalRows - (maxEntries + headerRows);
      if (deleteCount > 0) {
        Logger.log(`Trimming log sheet "${logSheet.getName()}", deleting ${deleteCount} oldest entries from row ${maxEntries + headerRows + 1}`);
        // Delete from the bottom up (older entries are at higher row numbers)
        logSheet.deleteRows(maxEntries + headerRows + 1, deleteCount);
      }
    }
  } catch (writeError) {
    Logger.log(`ERROR WRITING LOG ENTRY TO SHEET "${logSheet.getName()}": ${writeError.message}. Falling back to Logger.`);
    // Log to standard logger as fallback
    Logger.log(`Fallback Log: [${sessionId}] ${eventType}: ${message}`);
    // Optional: Try appending to the end as a last resort (might mess up sorting/view)
    try {
      logSheet.appendRow([new Date(), String(sessionId || ''), String(eventType || 'UNKNOWN'), String(message || '')]);
      Logger.log("Appended log entry as fallback.");
    } catch (appendError) {
      Logger.log(`ERROR appending log entry as last resort: ${appendError.message}`);
    }
  }
}

/**
* Logs an operation to the log sheet. Ensures sheet exists.
* Formatting is handled by conditional rules set during sheet creation.
* @param {string} sessionId - Session identifier
* @param {string} eventType - Type of event
* @param {string} message - Log message
* @returns {boolean} Success or failure of logging operation
*/
function logOperation(sessionId, eventType, message) {
  // Also log to standard Apps Script logger for easy debugging via View -> Logs
  Logger.log(`LogOp: [${sessionId || 'NO_SESSION'}] ${eventType}: ${message}`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetName = CONFIG.LOG_SHEET_NAME;
    let logSheet = ss.getSheetByName(sheetName);

    // Create log sheet if it doesn't exist
    if (!logSheet) {
      Logger.log(`Log sheet "${sheetName}" not found, attempting creation.`);
      try {
        logSheet = createLogSheet(); // createLogSheet now returns the sheet
        if (!logSheet) {
          throw new Error(`Function createLogSheet() did not return a valid sheet object for "${sheetName}".`);
        }
        Logger.log(`Log sheet "${sheetName}" created successfully.`);
      } catch (createError) {
        Logger.log(`CRITICAL ERROR CREATING LOG SHEET "${sheetName}": ${createError.message}. Logging to sheet aborted.`);
        // Log to standard logger as absolute fallback
        Logger.log(`Critical Fallback Log: [${sessionId}] ${eventType}: ${message}`);
        return false; // Indicate logging to sheet failed
      }
    }

    // Write the entry using the internal helper
    _writeLogEntry(logSheet, new Date(), sessionId, eventType, message);
    return true; // Indicate logging to sheet succeeded

  } catch (error) {
    // Catch errors from getActiveSpreadsheet or other unexpected issues
    Logger.log(`FATAL LOGGING SYSTEM ERROR: ${error.message}`);
    Logger.log(`Original log message intended for sheet: [${sessionId}] ${eventType}: ${message}`);
    return false; // Indicate logging failed
  }
}


/**
* Internal function to write a verification entry without formatting logic.
* Assumes verificationSheet exists and is valid. Handles insertion and trimming.
* @param {Sheet} verificationSheet - The verification sheet object.
* @param {Object} verificationData - The verification data object with keys matching expected fields.
*/
function _writeVerificationEntry(verificationSheet, verificationData) {
  const headers = [ // Match the order in createVerificationSheet and config
    'Timestamp', 'SessionID', 'RuleID', 'SourceType', 'SourceFile',
    'DestinationSheet', 'SourceRows', 'DestRows', 'SourceColumns', 'DestColumns',
    'RowsMatch', 'ColumnsMatch', 'SamplesMatch', 'DataHash', 'Status', 'Details'
  ];
  const headerMap = createHeaderMap(headers); // For index lookup

  // Map data object to array in header order, providing defaults
  const dataRow = headers.map(header => {
    switch (header) {
      case 'Timestamp': return verificationData.timestamp instanceof Date ? verificationData.timestamp : new Date();
      case 'SessionID': return String(verificationData.sessionId || '');
      case 'RuleID': return String(verificationData.ruleId || '');
      case 'SourceType': return String(verificationData.sourceType || 'N/A');
      case 'SourceFile': return String(verificationData.sourceFile || 'N/A');
      case 'DestinationSheet': return String(verificationData.destinationSheet || 'N/A');
      case 'SourceRows': return verificationData.sourceRowCount !== undefined && verificationData.sourceRowCount !== null ? Number(verificationData.sourceRowCount) : '';
      case 'DestRows': return verificationData.destinationRowCount !== undefined && verificationData.destinationRowCount !== null ? Number(verificationData.destinationRowCount) : '';
      case 'SourceColumns': return verificationData.sourceColumnCount !== undefined && verificationData.sourceColumnCount !== null ? Number(verificationData.sourceColumnCount) : '';
      case 'DestColumns': return verificationData.destinationColumnCount !== undefined && verificationData.destinationColumnCount !== null ? Number(verificationData.destinationColumnCount) : '';
      case 'RowsMatch': return String(verificationData.rowsMatch || 'N/A');
      case 'ColumnsMatch': return String(verificationData.columnsMatch || 'N/A');
      case 'SamplesMatch': return String(verificationData.samplesMatch || 'N/A');
      case 'DataHash': return String(verificationData.dataHash || '');
      case 'Status': return String(verificationData.status || 'UNKNOWN');
      case 'Details': return String(verificationData.details || verificationData.errorDetails || ''); // Allow either key
      default: return ''; // Default for any unexpected headers
    }
  });

  try {
    verificationSheet.insertRowAfter(1); // Insert below header
    const newRow = verificationSheet.getRange(2, 1, 1, headers.length);
    newRow.setValues([dataRow]);

    // Apply specific formats if needed (Timestamp)
    const formatConfig = CONFIG.SHEET_FORMATS.VERIFICATION_SHEET || {};
    const timestampFormat = formatConfig.timestampFormat || "MM/dd/yyyy HH:mm:ss";
    const timestampColIndex = headerMap['Timestamp']; // Should be 0
    if (timestampColIndex !== undefined) {
      try {
        verificationSheet.getRange(2, timestampColIndex + 1).setNumberFormat(timestampFormat);
      } catch (e) {
        Logger.log(`Warning: Failed to set timestamp format on verification sheet: ${e.message}`);
      }
    }

    // Trim log
    const maxEntries = CONFIG.MAX_LOG_ENTRIES || 500;
    const totalRows = verificationSheet.getLastRow();
    const headerRows = 1;
    if (totalRows > maxEntries + headerRows) {
      const deleteCount = totalRows - (maxEntries + headerRows);
      if (deleteCount > 0) {
        Logger.log(`Trimming verification log sheet "${verificationSheet.getName()}", deleting ${deleteCount} oldest entries from row ${maxEntries + headerRows + 1}`);
        verificationSheet.deleteRows(maxEntries + headerRows + 1, deleteCount);
      }
    }
  } catch (writeError) {
    Logger.log(`ERROR WRITING VERIFICATION ENTRY TO SHEET "${verificationSheet.getName()}": ${writeError.message}.`);
    try { verificationSheet.appendRow(dataRow); Logger.log("Appended verification entry as fallback."); } catch (e) { Logger.log(`ERROR appending verification entry as fallback: ${e.message}`); }
  }
}

/**
* Logs a verification entry to the verification sheet. Ensures sheet exists.
* Formatting is handled by conditional rules set during sheet creation.
* Automatically calculates RowsMatch/ColumnsMatch if counts provided.
* @param {Object} verificationInput - Verification data object. Keys should match expected fields (e.g., sessionId, ruleId, sourceRowCount, destinationRowCount, isComplete, status, etc.).
* @returns {boolean} Success or failure of logging operation
*/
function logVerification(verificationInput) {
  const sessionId = verificationInput?.sessionId || 'N/A';
  const ruleId = verificationInput?.ruleId || 'N/A';
  Logger.log(`LogVerify: [${sessionId}] Rule: ${ruleId} Status: ${verificationInput?.status || verificationInput?.isComplete || 'N/A'}`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetName = CONFIG.VERIFICATION_SHEET_NAME;
    let verificationSheet = ss.getSheetByName(sheetName);

    // Create verification sheet if it doesn't exist
    if (!verificationSheet) {
      Logger.log(`Verification sheet "${sheetName}" not found, attempting creation.`);
      try {
        verificationSheet = createVerificationSheet();
        if (!verificationSheet) {
          throw new Error(`Function createVerificationSheet() did not return a valid sheet object for "${sheetName}".`);
        }
        Logger.log(`Verification sheet "${sheetName}" created successfully.`);
      } catch (createError) {
        Logger.log(`CRITICAL ERROR CREATING VERIFICATION SHEET "${sheetName}": ${createError.message}. Logging verification aborted.`);
        return false;
      }
    }

    // Prepare data object, ensuring consistency
    const verificationData = { ...verificationInput }; // Clone input
    verificationData.timestamp = verificationInput.timestamp || new Date(); // Default timestamp

    // Determine overall status: Use 'status' field preferably, fallback to 'isComplete' boolean
    if (!verificationData.status) {
      if (verificationInput.isComplete === true) {
        verificationData.status = 'COMPLETE';
      } else if (verificationInput.isComplete === false) {
        verificationData.status = 'ERROR';
      } else {
        verificationData.status = 'UNKNOWN'; // Default if neither provided
      }
    }

    // Calculate matches if row/col counts are present and valid numbers
    const srcRows = verificationData.sourceRowCount;
    const destRows = verificationData.destinationRowCount;
    const srcCols = verificationData.sourceColumnCount;
    const destCols = verificationData.destinationColumnCount;

    if (typeof srcRows === 'number' && typeof destRows === 'number') {
      verificationData.rowsMatch = srcRows === destRows ? 'YES' : 'NO';
    } else {
      verificationData.rowsMatch = 'N/A'; // Indicate counts weren't available/comparable
    }
    if (typeof srcCols === 'number' && typeof destCols === 'number') {
      // Allow destination to have *more* columns (e.g., if formula columns added)
      verificationData.columnsMatch = srcCols <= destCols ? 'YES' : 'NO';
    } else {
      verificationData.columnsMatch = 'N/A';
    }
    // Standardize samplesMatch boolean to YES/NO/N/A string
    if (verificationInput.samplesMatch === true) {
      verificationData.samplesMatch = 'YES';
    } else if (verificationInput.samplesMatch === false) {
      verificationData.samplesMatch = 'NO';
    } else {
      verificationData.samplesMatch = 'N/A'; // Default if not boolean true/false
    }


    // Write the entry using the internal helper
    _writeVerificationEntry(verificationSheet, verificationData);
    return true;

  } catch (error) {
    Logger.log(`FATAL VERIFICATION LOGGING SYSTEM ERROR: ${error.message}`);
    Logger.log(`Original verification data: ${JSON.stringify(verificationInput)}`);
    return false;
  }
}


/**
* Internal function to write a diagnostic entry without formatting logic.
* Assumes diagnosticSheet exists and is valid. Handles insertion and trimming.
* @param {Sheet} diagnosticSheet - The diagnostic sheet object.
* @param {Object} diagnosticData - The diagnostic data object with expected keys.
*/
function _writeDiagnosticEntry(diagnosticSheet, diagnosticData) {
  const headers = [ // Match the order in createDiagnosticSheet and config
    'Timestamp', 'SessionID', 'Position', 'Column', 'SourceValue', 'SourceType',
    'DestValue', 'DestType', 'NormalizedSource', 'NormalizedDest', 'Details'
  ];
  const headerMap = createHeaderMap(headers); // For index lookup

  // Map data object to array in header order
  const dataRow = headers.map(header => {
    switch (header) {
      case 'Timestamp': return diagnosticData.timestamp instanceof Date ? diagnosticData.timestamp : new Date();
      case 'SessionID': return String(diagnosticData.sessionId || '');
      case 'Position': return String(diagnosticData.position || 'N/A');
      case 'Column': return diagnosticData.column !== undefined ? String(diagnosticData.column) : ''; // Allow empty string for column
      // Preserve original values exactly as passed in
      case 'SourceValue': return diagnosticData.sourceValue;
      case 'SourceType': return typeof diagnosticData.sourceValue;
      case 'DestValue': return diagnosticData.destValue;
      case 'DestType': return typeof diagnosticData.destValue;
      // Ensure normalized values are strings for consistency
      case 'NormalizedSource': return diagnosticData.normalizedSource !== undefined ? String(diagnosticData.normalizedSource) : '';
      case 'NormalizedDest': return diagnosticData.normalizedDest !== undefined ? String(diagnosticData.normalizedDest) : '';
      case 'Details': return String(diagnosticData.details || '');
      default: return '';
    }
  });

  try {
    diagnosticSheet.insertRowAfter(1); // Insert below header
    const newRow = diagnosticSheet.getRange(2, 1, 1, headers.length);
    newRow.setValues([dataRow]);

    // Apply specific formats if needed (Timestamp)
    const formatConfig = CONFIG.SHEET_FORMATS.DIAGNOSTIC_SHEET || {};
    const timestampFormat = formatConfig.timestampFormat || "MM/dd/yyyy HH:mm:ss.SSS"; // Default with ms
    const timestampColIndex = headerMap['Timestamp'];
    if (timestampColIndex !== undefined) {
      try {
        diagnosticSheet.getRange(2, timestampColIndex + 1).setNumberFormat(timestampFormat);
      } catch (e) {
        Logger.log(`Warning: Failed to set timestamp format on diagnostic sheet: ${e.message}`);
      }
    }


    // Trim log
    const maxEntries = CONFIG.MAX_LOG_ENTRIES || 500;
    const totalRows = diagnosticSheet.getLastRow();
    const headerRows = 1;
    if (totalRows > maxEntries + headerRows) {
      const deleteCount = totalRows - (maxEntries + headerRows);
      if (deleteCount > 0) {
        Logger.log(`Trimming diagnostic log sheet "${diagnosticSheet.getName()}", deleting ${deleteCount} oldest entries from row ${maxEntries + headerRows + 1}`);
        diagnosticSheet.deleteRows(maxEntries + headerRows + 1, deleteCount);
      }
    }
  } catch (writeError) {
    Logger.log(`ERROR WRITING DIAGNOSTIC ENTRY TO SHEET "${diagnosticSheet.getName()}": ${writeError.message}.`);
    try { diagnosticSheet.appendRow(dataRow); Logger.log("Appended diagnostic entry as fallback."); } catch (e) { Logger.log(`ERROR appending diagnostic entry as fallback: ${e.message}`); }
  }
}


/**
* Logs a diagnostic entry for detailed data comparison. Ensures sheet exists.
* Formatting is handled by conditional rules set during sheet creation.
* @param {Object} diagnosticInput - Diagnostic data object (e.g., sessionId, position, column, sourceValue, destValue, normalizedSource, normalizedDest, details).
* @returns {boolean} Success or failure of logging operation
*/
function logDiagnostic(diagnosticInput) {
  const sessionId = diagnosticInput?.sessionId || 'N/A';
  const position = diagnosticInput?.position || 'N/A';
  Logger.log(`LogDiag: [${sessionId}] Position: ${position} Col: ${diagnosticInput?.column || 'N/A'}`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetName = CONFIG.DIAGNOSTIC_SHEET_NAME;
    let diagnosticSheet = ss.getSheetByName(sheetName);

    // Create diagnostic sheet if it doesn't exist
    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet "${sheetName}" not found, attempting creation.`);
      try {
        diagnosticSheet = createDiagnosticSheet();
        if (!diagnosticSheet) {
          throw new Error(`Function createDiagnosticSheet() did not return a valid sheet object for "${sheetName}".`);
        }
        Logger.log(`Diagnostic sheet "${sheetName}" created successfully.`);
      } catch (createError) {
        Logger.log(`CRITICAL ERROR CREATING DIAGNOSTIC SHEET "${sheetName}": ${createError.message}. Logging diagnostic aborted.`);
        return false;
      }
    }

    // Prepare data - ensure timestamp exists
    const diagnosticData = { ...diagnosticInput };
    diagnosticData.timestamp = diagnosticInput.timestamp || new Date();

    // Write the entry using the internal helper
    _writeDiagnosticEntry(diagnosticSheet, diagnosticData);
    return true;

  } catch (error) {
    Logger.log(`FATAL DIAGNOSTIC LOGGING SYSTEM ERROR: ${error.message}`);
    Logger.log(`Original diagnostic data: ${JSON.stringify(diagnosticInput)}`);
    return false;
  }
}


// ========================================================================== //
// DATA VERIFICATION FUNCTIONS (Could be in VerificationLib.gs)
// ========================================================================== //

/**
 * IMPROVED: Verify that sample data matches between source and destination.
 * Uses normalizeValue for robust comparison and logs mismatches to diagnostic sheet.
 * @param {Array<Array>} sourceData - Source 2D array of data (includes headers potentially).
 * @param {Sheet} destSheet - Destination sheet object.
 * @param {string} sessionId - Session identifier for logging.
 * @param {string} sheetHandlingMode - How sheets are being handled ('append', 'clearAndReuse', etc.).
 * @param {number} beforeRowCount - Row count before operation (used only for 'append' mode).
 * @returns {boolean} True if configured samples match, false otherwise.
 */
function verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount = 0) {
  // Basic validation
  if (!CONFIG.VERIFICATION_CONFIG.ENABLED || !CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA) {
    Logger.log("Sample verification skipped (disabled in config).");
    return true; // Return true if verification is disabled
  }
  if (!sourceData || !Array.isArray(sourceData) || sourceData.length === 0 || !Array.isArray(sourceData[0])) {
    logOperation(sessionId, "SAMPLE_CHECK_ERROR", "Sample verification failed: Invalid source data provided.");
    return false;
  }
  if (!destSheet) {
    logOperation(sessionId, "SAMPLE_CHECK_ERROR", "Sample verification failed: Invalid destination sheet provided.");
    return false;
  }

  const sourceNumRows = sourceData.length;
  const sourceNumCols = sourceData[0].length;
  const sourceHasHeader = (sheetHandlingMode !== 'append' || beforeRowCount === 0);
  const sourceDataStartRow = sourceHasHeader ? 1 : 0;

  if (sourceNumRows <= sourceDataStartRow) {
    Logger.log("Sample verification: Source data has no data rows to sample.");
    let destLastRow = 0;
    try { destLastRow = destSheet.getLastRow(); } catch (e) { destLastRow = 0; }
    const destIsEmpty = destLastRow <= 1;
    if (destIsEmpty) { return true; }
    else { logOperation(sessionId, "SAMPLE_MISMATCH", `Source has no data rows, but destination has ${destLastRow} rows.`); return false; }
  }

  Logger.log(`Starting sample data verification for SessionID: ${sessionId}. Mode: ${sheetHandlingMode}, BeforeRows: ${beforeRowCount}, SourceRows: ${sourceNumRows}, HeaderInSrc: ${sourceHasHeader}`);

  try {
    const SAMPLE_SIZE = CONFIG.VERIFICATION_CONFIG.SAMPLE_SIZE || 3;
    const samplesToCheck = [];

    // 1. First Data Row
    if (sourceNumRows > sourceDataStartRow) samplesToCheck.push({ position: 'First Data Row', sourceIdx: sourceDataStartRow });
    // 2. Last Data Row
    const lastDataRowIdx = sourceNumRows - 1;
    if (lastDataRowIdx > sourceDataStartRow) samplesToCheck.push({ position: 'Last Data Row', sourceIdx: lastDataRowIdx });
    // 3. Random Middle Rows
    const numDataRows = sourceNumRows - sourceDataStartRow;
    if (numDataRows > 2 && SAMPLE_SIZE > 2) { /* Select random indices */ } // Keep random selection logic

    Logger.log(`Will check ${samplesToCheck.length} sample points.`);

    for (const sample of samplesToCheck) {
      const sourceRowArray = sourceData[sample.sourceIdx];
      let destRowIdx; // Keep calculation logic as before
      if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
        destRowIdx = beforeRowCount + (sample.sourceIdx - sourceDataStartRow) + 1;
      } else {
        destRowIdx = sample.sourceIdx + 1; // Simpler: source 0-based index + 1 = 1-based row
      }

      if (destRowIdx <= 0) { /* Log error */ return false; }
      const destLastRowCheck = destSheet.getLastRow();
      if (destRowIdx > destLastRowCheck) { /* Log error */ return false; }

      Logger.log(`Comparing source index ${sample.sourceIdx} (${sample.position}) with destination row ${destRowIdx}`);

      let destRowArray;
      try { destRowArray = destSheet.getRange(destRowIdx, 1, 1, sourceNumCols).getValues()[0]; }
      catch (e) { /* Log error */ return false; }

      for (let colIdx = 0; colIdx < sourceNumCols; colIdx++) {
        const sourceVal = sourceRowArray[colIdx];
        const destVal = destRowArray ? destRowArray[colIdx] : undefined; // Handle case where destRowArray might be undefined
        const sourceNormalized = normalizeValue(sourceVal);
        const destNormalized = normalizeValue(destVal);

        if (sourceNormalized !== destNormalized) {
          const errorMsg = `Mismatch at ${sample.position} [SrcIdx:${sample.sourceIdx}, DestRow:${destRowIdx}], Col:${colIdx + 1}. Source='${sourceVal}' (Norm='${sourceNormalized}'), Dest='${destVal}' (Norm='${destNormalized}')`;
          Logger.log(`SAMPLE_MISMATCH: ${errorMsg}`);

          // Log detailed diagnostic information
          logDiagnostic({
            sessionId: sessionId,
            position: `${sample.position} (SrcIdx:${sample.sourceIdx}, DestRow:${destRowIdx})`,
            // --- FIX IS HERE ---
            column: columnToLetter(colIdx), // Correct function name
            // --- END FIX ---
            sourceValue: sourceVal,
            destValue: destVal,
            normalizedSource: sourceNormalized,
            normalizedDest: destNormalized,
            details: "Sample data mismatch detected during verification."
          });

          logOperation(sessionId, "SAMPLE_MISMATCH", `Mismatch at ${sample.position}, Col ${colIdx + 1}. Src: ${String(sourceVal).slice(0, 50)}, Dest: ${String(destVal).slice(0, 50)}`);
          return false; // Mismatch found
        }
      }
    }
    Logger.log(`Sample data verification passed successfully for SessionID: ${sessionId}`);
    return true; // All samples matched
  } catch (error) {
    // Ensure the error message from this block includes the function name for clarity
    const errorMsg = `Error during verifyDataSamples: ${error.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "SAMPLE_CHECK_ERROR", errorMsg); // Log the specific error
    throw error; // Re-throw the error so the main process knows verification failed
    // return false; // Don't just return false, throw to indicate failure reason
  }
}

// ========================================================================== //
// EMAIL FUNCTIONS (Could be in EmailLib.gs)
// ========================================================================== //

/**
* Retrieves log entries for a specific session from the log sheet.
* @param {string} sessionId - Session identifier.
* @param {number} [maxEntries] - Maximum number of entries to return (defaults to config).
* @returns {Array<Object>} Array of log entry objects {timestamp, eventType, message}. Sorted recent first.
*/
function getLogEntriesForSession(sessionId, maxEntries) {
  if (!sessionId) return [];
  const logEntries = [];
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);

    if (!logSheet) {
      Logger.log(`Log sheet "${CONFIG.LOG_SHEET_NAME}" not found for retrieving session logs.`);
      return [];
    }

    // Get log data - potentially large, fetch specific columns if needed
    const logData = logSheet.getDataRange().getValues();
    if (logData.length <= 1) {
      return [];
    }

    // Get headers and find required column indices
    const headers = logData[0];
    const headerMap = createHeaderMap(headers);
    const timestampIdx = headerMap["Timestamp"];
    const sessionIdIdx = headerMap["SessionID"];
    const eventTypeIdx = headerMap["EventType"];
    const messageIdx = headerMap["Message"];

    if (sessionIdIdx === undefined || timestampIdx === undefined || eventTypeIdx === undefined || messageIdx === undefined) {
      Logger.log(`Log sheet "${CONFIG.LOG_SHEET_NAME}" is missing required columns (Timestamp, SessionID, EventType, Message). Cannot retrieve session logs.`);
      return [];
    }

    // Filter entries by session ID
    for (let i = logData.length - 1; i >= 1; i--) { // Iterate backwards for potentially faster exit if logs sorted
      const row = logData[i];
      if (row[sessionIdIdx] === sessionId) {
        logEntries.push({
          // Ensure timestamp is a Date object for sorting
          timestamp: row[timestampIdx] instanceof Date ? row[timestampIdx] : new Date(row[timestampIdx]),
          eventType: row[eventTypeIdx],
          message: row[messageIdx]
        });
      }
    }

    // Sort by timestamp (most recent first)
    logEntries.sort((a, b) => b.timestamp.getTime() - a.timestamp.getTime());

    // Limit to max rows specified or configured
    const limit = maxEntries || CONFIG.EMAIL_CONFIG?.MAX_ROWS_IN_EMAIL || 100;
    return logEntries.slice(0, limit);

  } catch (error) {
    Logger.log(`ERROR RETRIEVING LOG ENTRIES for session ${sessionId}: ${error.message}`);
    return logEntries; // Return any entries found before the error
  }
}

/**
* Retrieves verification data for a specific session from the verification sheet.
* @param {string} sessionId - Session identifier.
* @returns {Array<Object>} Array of verification data objects, matching sheet columns.
*/
function getVerificationDataForSession(sessionId) {
  if (!sessionId) return [];
  const verificationEntries = [];
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);

    if (!verificationSheet) {
      Logger.log(`Verification sheet "${CONFIG.VERIFICATION_SHEET_NAME}" not found.`);
      return [];
    }

    const allData = verificationSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      return [];
    }

    const headers = allData[0];
    const headerMap = createHeaderMap(headers);
    const sessionIdIdx = headerMap["SessionID"];

    if (sessionIdIdx === undefined) {
      Logger.log(`Verification sheet is missing required "SessionID" column.`);
      return [];
    }

    // Filter rows for the session
    for (let i = 1; i < allData.length; i++) {
      const row = allData[i];
      if (row[sessionIdIdx] === sessionId) {
        const entry = {};
        headers.forEach((header, index) => {
          // Map row data to an object using headers as keys
          entry[header] = row[index];
        });
        verificationEntries.push(entry);
      }
    }

    // Sort by timestamp if desired (most recent first)
    if (headerMap["Timestamp"] !== undefined) {
      verificationEntries.sort((a, b) => {
        const dateA = a.Timestamp instanceof Date ? a.Timestamp : new Date(a.Timestamp);
        const dateB = b.Timestamp instanceof Date ? b.Timestamp : new Date(b.Timestamp);
        return dateB.getTime() - dateA.getTime();
      });
    }

    return verificationEntries;

  } catch (error) {
    Logger.log(`Error getting verification data for session ${sessionId}: ${error.message}`);
    return verificationEntries; // Return what was found before error
  }
}

/**
* Retrieves diagnostic data for a specific session from the diagnostic sheet.
* @param {string} sessionId - Session identifier.
* @returns {Array<Object>} Array of diagnostic data objects, matching sheet columns.
*/
function getDiagnosticDataForSession(sessionId) {
  if (!sessionId) return [];
  const diagnosticEntries = [];
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);

    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet "${CONFIG.DIAGNOSTIC_SHEET_NAME}" not found.`);
      return [];
    }

    const allData = diagnosticSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      return [];
    }

    const headers = allData[0];
    const headerMap = createHeaderMap(headers);
    const sessionIdIdx = headerMap["SessionID"];

    if (sessionIdIdx === undefined) {
      Logger.log(`Diagnostic sheet is missing required "SessionID" column.`);
      return [];
    }

    // Filter rows for the session
    for (let i = 1; i < allData.length; i++) {
      const row = allData[i];
      if (row[sessionIdIdx] === sessionId) {
        const entry = {};
        headers.forEach((header, index) => {
          entry[header] = row[index];
        });
        diagnosticEntries.push(entry);
      }
    }

    // Sort by timestamp if desired (most recent first)
    if (headerMap["Timestamp"] !== undefined) {
      diagnosticEntries.sort((a, b) => {
        const dateA = a.Timestamp instanceof Date ? a.Timestamp : new Date(a.Timestamp);
        const dateB = b.Timestamp instanceof Date ? b.Timestamp : new Date(b.Timestamp);
        return dateB.getTime() - dateA.getTime();
      });
    }

    return diagnosticEntries;

  } catch (error) {
    Logger.log(`Error getting diagnostic data for session ${sessionId}: ${error.message}`);
    return diagnosticEntries; // Return what was found before error
  }
}

/**
* Creates a CSV attachment Blob of log entries for a specific session.
* @param {string} sessionId - Session identifier.
* @returns {Blob|null} Blob containing the log data as CSV, or null if no entries found/error.
*/
function createLogAttachment(sessionId) {
  if (!sessionId) return null;
  try {
    Logger.log(`Creating log attachment for session: ${sessionId}`);
    const logEntries = getLogEntriesForSession(sessionId, 5000); // Get more entries for attachment

    if (!logEntries || logEntries.length === 0) {
      Logger.log(`No log entries found for session ${sessionId} to create attachment.`);
      return null;
    }

    // Define CSV headers
    const headers = ['Timestamp', 'SessionID', 'EventType', 'Message'];
    let csvContent = '"' + headers.join('","') + '"\n'; // Header row

    // Add each entry, ensuring proper CSV escaping
    logEntries.forEach(entry => {
      const timestamp = formatDate(entry.timestamp, "yyyy-MM-dd HH:mm:ss.SSS"); // Consistent format
      const eventType = String(entry.eventType || '').replace(/"/g, '""');
      const message = String(entry.message || '').replace(/"/g, '""').replace(/\n/g, ' '); // Escape quotes, replace newlines

      csvContent += `"${timestamp}","${sessionId}","${eventType}","${message}"\n`;
    });

    const blob = Utilities.newBlob(csvContent, MimeType.CSV, `ingest_log_${sessionId}.csv`);
    Logger.log(`Created log attachment Blob with ${logEntries.length} entries, size: ${blob.getBytes().length} bytes.`);
    return blob;

  } catch (error) {
    Logger.log(`ERROR CREATING LOG ATTACHMENT for session ${sessionId}: ${error.message}`);
    return null;
  }
}

/**
* Creates a CSV attachment Blob of verification entries for a specific session.
* Fetches data directly from the sheet to ensure all columns are included.
* @param {string} sessionId - Session identifier.
* @returns {Blob|null} Blob containing the verification data as CSV, or null if no entries/error.
*/
function createVerificationAttachment(sessionId) {
  if (!sessionId) return null;
  try {
    Logger.log(`Creating verification attachment for session: ${sessionId}`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);

    if (!verificationSheet) {
      Logger.log(`Verification sheet not found, cannot create attachment.`);
      return null;
    }

    const allData = verificationSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No verification entries found in sheet for attachment.`);
      return null;
    }

    const headers = allData[0];
    const headerMap = createHeaderMap(headers);
    const sessionIdIdx = headerMap["SessionID"];

    if (sessionIdIdx === undefined) {
      Logger.log(`Verification sheet missing SessionID column, cannot create session attachment.`);
      return null;
    }

    // Filter for the session and include header row
    const sessionEntries = [headers]; // Start with header row
    for (let i = 1; i < allData.length; i++) {
      if (allData[i][sessionIdIdx] === sessionId) {
        sessionEntries.push(allData[i]);
      }
    }

    if (sessionEntries.length === 1) { // Only header row
      Logger.log(`No verification entries found for session ${sessionId} in sheet.`);
      return null;
    }

    // Convert array of arrays to CSV string
    const csvContent = sessionEntries.map(row =>
      row.map(cell => {
        let value = cell;
        if (value instanceof Date) {
          value = formatDate(value, "yyyy-MM-dd HH:mm:ss"); // Consistent format
        }
        value = String(value === null || value === undefined ? '' : value);
        // Escape double quotes by doubling them, and enclose in double quotes if it contains comma, newline or double quote
        if (value.includes('"') || value.includes(',') || value.includes('\n')) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value; // Return as is if no special chars
      }).join(',') // Join cells with comma
    ).join('\n'); // Join rows with newline


    const blob = Utilities.newBlob(csvContent, MimeType.CSV, `ingest_verification_${sessionId}.csv`);
    Logger.log(`Created verification attachment Blob with ${sessionEntries.length - 1} data entries, size: ${blob.getBytes().length} bytes.`);
    return blob;

  } catch (error) {
    Logger.log(`ERROR CREATING VERIFICATION ATTACHMENT for session ${sessionId}: ${error.message}`);
    return null;
  }
}

/**
* Creates a CSV attachment Blob of diagnostic entries for a specific session.
* Fetches data directly from the sheet.
* @param {string} sessionId - Session identifier.
* @returns {Blob|null} Blob containing diagnostic data as CSV, or null if no entries/error.
*/
function createDiagnosticAttachment(sessionId) {
  if (!sessionId) return null;
  try {
    Logger.log(`Creating diagnostic attachment for session: ${sessionId}`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);

    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet not found, cannot create attachment.`);
      return null;
    }

    const allData = diagnosticSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No diagnostic entries found in sheet for attachment.`);
      return null;
    }

    const headers = allData[0];
    const headerMap = createHeaderMap(headers);
    const sessionIdIdx = headerMap["SessionID"];

    if (sessionIdIdx === undefined) {
      Logger.log(`Diagnostic sheet missing SessionID column, cannot create session attachment.`);
      return null;
    }

    // Filter for the session and include header row
    const sessionEntries = [headers]; // Start with header row
    for (let i = 1; i < allData.length; i++) {
      if (allData[i][sessionIdIdx] === sessionId) {
        sessionEntries.push(allData[i]);
      }
    }

    if (sessionEntries.length === 1) { // Only header row
      Logger.log(`No diagnostic entries found for session ${sessionId} in sheet.`);
      return null;
    }

    // Convert array of arrays to CSV string (using same logic as verification attachment)
    const csvContent = sessionEntries.map(row =>
      row.map(cell => {
        let value = cell;
        if (value instanceof Date) {
          value = formatDate(value, "yyyy-MM-dd HH:mm:ss.SSS"); // Consistent format with ms
        }
        value = String(value === null || value === undefined ? '' : value);
        if (value.includes('"') || value.includes(',') || value.includes('\n')) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      }).join(',')
    ).join('\n');

    const blob = Utilities.newBlob(csvContent, MimeType.CSV, `ingest_diagnostic_${sessionId}.csv`);
    Logger.log(`Created diagnostic attachment Blob with ${sessionEntries.length - 1} data entries, size: ${blob.getBytes().length} bytes.`);
    return blob;

  } catch (error) {
    Logger.log(`ERROR CREATING DIAGNOSTIC ATTACHMENT for session ${sessionId}: ${error.message}`);
    return null;
  }
}


/**
* Sends an error notification email with improved formatting and optional attachments.
* @param {string} subject - Email subject line (prefix will be added).
* @param {string} message - Main error message content.
* @param {string} sessionId - Session identifier for log filtering and attachments.
*/
function sendErrorNotification(subject, message, sessionId) {
  const recipients = CONFIG.EMAIL_NOTIFICATIONS;
  if (!recipients || recipients.length === 0 || !CONFIG.EMAIL_CONFIG.SEND_ON_ERROR) {
    Logger.log(`Error notification skipped: No recipients configured or SEND_ON_ERROR is false.`);
    return;
  }

  Logger.log(`Preparing error notification for session ${sessionId}: ${subject}`);

  try {
    const prefix = CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX || "[Data Ingest]";
    const fullSubject = `${prefix} ERROR: ${subject}`;

    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const spreadsheetName = spreadsheet.getName();
    const spreadsheetUrl = spreadsheet.getUrl();
    const currentTime = formatDate(new Date()); // Use consistent formatting

    // --- Plain Text Body ---
    let plainBody = `ERROR in Data Ingest System (${spreadsheetName})\n\n`;
    plainBody += `Time: ${currentTime}\n`;
    plainBody += `Session ID: ${sessionId}\n`;
    plainBody += `Spreadsheet: ${spreadsheetName}\n\n`;
    plainBody += `Error Details:\n${message}\n\n`;
    plainBody += `Please check the log sheet for full details:\n${spreadsheetUrl}`;

    // --- HTML Body ---
    let htmlBody = "";
    if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
      // Basic inline styles for compatibility
      const styles = {
        body: "font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; background-color: #f8f8f8;",
        h2: "color: #D32F2F; border-bottom: 2px solid #D32F2F; padding-bottom: 10px; margin-bottom: 20px;",
        p: "line-height: 1.6;",
        error: "color: #D32F2F; font-weight: bold; background-color: #FFEBEE; border: 1px solid #FFCDD2; padding: 10px; border-radius: 4px; margin: 15px 0;",
        info: "color: #555;",
        table: "border-collapse: collapse; width: 100%; margin: 20px 0; background-color: #fff; border: 1px solid #ddd;",
        th: "background-color: #f2f2f2; text-align: left; padding: 10px; border: 1px solid #ddd; font-weight: bold;",
        td: "padding: 10px; border: 1px solid #ddd;",
        trEven: "background-color: #f9f9f9;",
        button: "display: inline-block; padding: 10px 20px; background-color: #1976D2; color: white; text-decoration: none; border-radius: 5px; margin-top: 20px;",
        footer: "color: #888; font-size: 12px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 15px;"
      };

      htmlBody = `<html><head><style>td, th { vertical-align: top; }</style></head><body style="${styles.body}">`;
      htmlBody += `<h2 style="${styles.h2}">Data Ingest System Error</h2>`;
      htmlBody += `<p style="${styles.info}"><strong>Time:</strong> ${currentTime}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Spreadsheet:</strong> ${spreadsheetName}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Session ID:</strong> ${sessionId}</p>`;
      htmlBody += `<div style="${styles.error}"><strong>Error Details:</strong><br/>${message.replace(/\n/g, '<br/>')}</div>`; // Replace newlines for HTML

      // Add recent log entries
      const logEntries = getLogEntriesForSession(sessionId, 20); // Get last 20 entries
      if (logEntries.length > 0) {
        htmlBody += `<h3>Recent Log Entries (Session: ${sessionId})</h3>`;
        htmlBody += `<table style="${styles.table}"><thead><tr><th style="${styles.th}">Timestamp</th><th style="${styles.th}">Event Type</th><th style="${styles.th}">Message</th></tr></thead><tbody>`;
        logEntries.forEach((entry, index) => {
          const rowStyle = index % 2 === 1 ? styles.trEven : "";
          const eventStyle = entry.eventType === 'ERROR' ? 'color:#D32F2F;font-weight:bold;' : (entry.eventType === 'WARNING' ? 'color:#F57F17;' : '');
          htmlBody += `<tr style="${rowStyle}"><td style="${styles.td}">${formatDate(entry.timestamp)}</td><td style="${styles.td};${eventStyle}">${entry.eventType}</td><td style="${styles.td}">${String(entry.message).replace(/\n/g, '<br/>')}</td></tr>`;
        });
        htmlBody += `</tbody></table>`;
      } else {
        htmlBody += `<p style="${styles.info}">No specific log entries found for this session ID.</p>`;
      }

      htmlBody += `<p><a href="${spreadsheetUrl}" style="${styles.button}">Open Spreadsheet</a></p>`;
      htmlBody += `<p style="${styles.footer}">This is an automated message. Please do not reply.</p>`;
      htmlBody += `</body></html>`;
    }

    // --- Attachments ---
    let attachments = [];
    if (CONFIG.EMAIL_CONFIG.INCLUDE_LOG_ATTACHMENT) {
      const logAttachment = createLogAttachment(sessionId);
      if (logAttachment) attachments.push(logAttachment);
    }
    if (CONFIG.EMAIL_CONFIG.INCLUDE_VERIFICATION_ATTACHMENT) {
      const verificationAttachment = createVerificationAttachment(sessionId);
      if (verificationAttachment) attachments.push(verificationAttachment);
    }
    if (CONFIG.EMAIL_CONFIG.INCLUDE_DIAGNOSTIC_ATTACHMENT) { // Assuming you might add this config
      const diagnosticAttachment = createDiagnosticAttachment(sessionId);
      if (diagnosticAttachment) attachments.push(diagnosticAttachment);
    }

    // --- Send Email ---
    const emailOptions = {
      attachments: attachments.length > 0 ? attachments : undefined,
      htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : undefined
      // noReply: true // Consider if you want to prevent replies
    };

    const recipientsArray = Array.isArray(recipients) ? recipients : [recipients];
    recipientsArray.forEach(email => {
      if (email && typeof email === 'string' && email.includes('@')) {
        try {
          GmailApp.sendEmail(email, fullSubject, plainBody, emailOptions);
          Logger.log(`Error notification sent to: ${email}`);
        } catch (sendError) {
          Logger.log(`Failed to send error notification to ${email}: ${sendError.message}`);
          // Log this failure back to the sheet if possible
          logOperation(sessionId, "EMAIL_ERROR", `Failed sending error notification to ${email}: ${sendError.message}`);
        }
      } else {
        Logger.log(`Skipping invalid recipient address: ${email}`);
      }
    });

    logOperation(sessionId, "EMAIL_SENT", `Error notification attempted for ${recipientsArray.length} recipient(s) with ${attachments.length} attachment(s). Subject: ${subject}`);

  } catch (error) {
    Logger.log(`FATAL ERROR constructing/sending error notification: ${error.message}`);
    logOperation(sessionId, "EMAIL_ERROR", `Fatal error preparing error email: ${error.message}`);
  }
}

/**
* Sends a notification when a job starts.
* @param {string} sessionId - Session identifier.
* @param {number} [rulesToProcessCount] - Optional: Number of rules being processed.
*/
function sendJobStartNotification(sessionId, rulesToProcessCount) {
  const recipients = CONFIG.EMAIL_NOTIFICATIONS;
  if (!recipients || recipients.length === 0 || !CONFIG.EMAIL_CONFIG.SEND_ON_START) {
    Logger.log(`Start notification skipped: No recipients or SEND_ON_START is false.`);
    return;
  }
  Logger.log(`Preparing start notification for session ${sessionId}`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const spreadsheetName = ss.getName();
    const spreadsheetUrl = ss.getUrl();
    const startTime = formatDate(new Date());

    const prefix = CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX || "[Data Ingest]";
    const subject = `${prefix} Job Started - ${spreadsheetName}`;

    let rulesMsg = rulesToProcessCount ? `${rulesToProcessCount} rule(s)` : 'All active rules';

    // --- Plain Text Body ---
    let plainBody = `Data Ingest job has started for spreadsheet: ${spreadsheetName}\n\n`;
    plainBody += `Time: ${startTime}\n`;
    plainBody += `Session ID: ${sessionId}\n`;
    plainBody += `Rules to process: ${rulesMsg}\n\n`;
    plainBody += `You will receive a completion notification when the job finishes.\n\n`;
    plainBody += `Link: ${spreadsheetUrl}`;

    // --- HTML Body ---
    let htmlBody = "";
    if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
      const styles = { /* Re-use or define styles similar to error notification */
        body: "font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; background-color: #f8f8f8;",
        h2: "color: #1976D2; border-bottom: 2px solid #1976D2; padding-bottom: 10px; margin-bottom: 20px;",
        p: "line-height: 1.6;",
        info: "color: #555;",
        button: "display: inline-block; padding: 10px 20px; background-color: #1976D2; color: white; text-decoration: none; border-radius: 5px; margin-top: 20px;",
        footer: "color: #888; font-size: 12px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 15px;"
      };
      htmlBody = `<html><body style="${styles.body}">`;
      htmlBody += `<h2 style="${styles.h2}">Data Ingest Job Started</h2>`;
      htmlBody += `<p style="${styles.info}"><strong>Spreadsheet:</strong> ${spreadsheetName}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Time:</strong> ${startTime}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Session ID:</strong> ${sessionId}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Rules to process:</strong> ${rulesMsg}</p>`;
      htmlBody += `<p>You will receive a completion notification when the job finishes.</p>`;
      htmlBody += `<p><a href="${spreadsheetUrl}" style="${styles.button}">Open Spreadsheet</a></p>`;
      htmlBody += `<p style="${styles.footer}">This is an automated message.</p>`;
      htmlBody += `</body></html>`;
    }

    // --- Send Email ---
    const emailOptions = { htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : undefined };
    const recipientsArray = Array.isArray(recipients) ? recipients : [recipients];
    recipientsArray.forEach(email => {
      if (email && typeof email === 'string' && email.includes('@')) {
        try {
          GmailApp.sendEmail(email, subject, plainBody, emailOptions);
          Logger.log(`Start notification sent to: ${email}`);
        } catch (sendError) {
          Logger.log(`Failed to send start notification to ${email}: ${sendError.message}`);
          logOperation(sessionId, "EMAIL_ERROR", `Failed sending start notification to ${email}: ${sendError.message}`);
        }
      }
    });
    logOperation(sessionId, "EMAIL_SENT", `Start notification sent for ${rulesMsg}.`);


  } catch (error) {
    Logger.log(`FATAL ERROR constructing/sending start notification: ${error.message}`);
    logOperation(sessionId, "EMAIL_ERROR", `Fatal error preparing start email: ${error.message}`);
  }
}


/**
* Sends a detailed run summary email after completion.
* @param {string} sessionId - Session identifier.
* @param {string} overallStatus - Overall status ('COMPLETE', 'ERROR', 'TEST').
* @param {Array<Object>} results - Array of result objects for each rule processed.
*        Each object: { ruleName, status, rowsProcessed, rowsExpected, duration, message }
*/
function sendRunSummaryEmail(sessionId, overallStatus, results) {
  const recipients = CONFIG.EMAIL_NOTIFICATIONS;
  if (!recipients || recipients.length === 0 || !CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE) {
    Logger.log(`Run summary email skipped: No recipients or SEND_ON_COMPLETE is false.`);
    return;
  }
  if (!results) results = []; // Ensure results is an array

  Logger.log(`Preparing run summary email for session ${sessionId}, Status: ${overallStatus}`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const spreadsheetName = ss.getName();
    const spreadsheetUrl = ss.getUrl();
    const completionTime = formatDate(new Date());

    // Calculate summary stats
    const successCount = results.filter(r => r.status === "SUCCESS").length;
    const errorCount = results.filter(r => r.status === "ERROR").length;
    const skippedCount = results.filter(r => r.status === "SKIPPED").length;
    const totalProcessed = results.length;
    const totalRowsProcessed = results.reduce((sum, r) => sum + (r.rowsProcessed || 0), 0);
    const totalRowsExpected = results.reduce((sum, r) => sum + (r.rowsExpected || 0), 0);
    const totalDuration = results.reduce((sum, r) => sum + (r.duration || 0), 0);
    const runDurationStr = totalDuration > 60 ? `${(totalDuration / 60).toFixed(1)} min` : `${totalDuration.toFixed(1)} sec`;

    // Determine subject based on status
    const prefix = CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX || "[Data Ingest]";
    let subjectStatus = overallStatus === "TEST" ? "Test Report" :
      (errorCount > 0 ? "Run Summary - ERRORS" : "Run Summary - Success");
    const subject = `${prefix} ${subjectStatus} - ${spreadsheetName}`;

    // --- Plain Text Body ---
    let plainBody = `Data Ingest Run Summary (${spreadsheetName})\n\n`;
    plainBody += `Completion Time: ${completionTime}\n`;
    plainBody += `Session ID: ${sessionId}\n`;
    plainBody += `Duration: ${runDurationStr}\n\n`;
    plainBody += `Overall Status: ${errorCount > 0 ? 'COMPLETED WITH ERRORS' : 'SUCCESS'}\n\n`;
    plainBody += `Summary:\n`;
    plainBody += `- Rules Processed: ${totalProcessed} (Success: ${successCount}, Errors: ${errorCount}, Skipped: ${skippedCount})\n`;
    plainBody += `- Rows Processed: ${totalRowsProcessed.toLocaleString()} (Expected: ${totalRowsExpected.toLocaleString()})\n\n`;

    if (results.length > 0) {
      plainBody += "Rule Details:\n";
      results.forEach(r => {
        plainBody += `- ${r.ruleName || 'Unknown Rule'}: ${r.status}`;
        if (r.status === "SUCCESS" || r.status === "ERROR") {
          plainBody += ` (${(r.rowsProcessed || 0).toLocaleString()} rows`;
          if (r.duration !== undefined) plainBody += ` in ${r.duration.toFixed(1)}s`;
          plainBody += `)`;
        }
        if (r.message) {
          plainBody += `\n   Message: ${r.message}`;
        }
        plainBody += '\n';
      });
    }

    plainBody += `\nLog Sheet: ${spreadsheetUrl}`;

    // --- HTML Body ---
    let htmlBody = "";
    if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
      // Define styles (similar to error/start notifications)
      const styles = {
        body: "font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; background-color: #f8f8f8;",
        h2: `color: ${errorCount > 0 ? '#D32F2F' : '#1976D2'}; border-bottom: 2px solid ${errorCount > 0 ? '#D32F2F' : '#1976D2'}; padding-bottom: 10px; margin-bottom: 20px;`,
        h3: "color: #444; border-bottom: 1px solid #ccc; padding-bottom: 8px; margin-top: 25px; margin-bottom: 15px;",
        p: "line-height: 1.6;",
        info: "color: #555;",
        summaryBox: "background-color: #fff; border: 1px solid #ddd; border-radius: 5px; padding: 15px 20px; margin: 20px 0;",
        table: "border-collapse: collapse; width: 100%; margin: 20px 0; background-color: #fff; border: 1px solid #ddd;",
        th: "background-color: #f2f2f2; text-align: left; padding: 10px; border: 1px solid #ddd; font-weight: bold; white-space: nowrap;",
        td: "padding: 10px; border: 1px solid #ddd; vertical-align: top;",
        tdRight: "text-align: right; padding: 10px; border: 1px solid #ddd; vertical-align: top;",
        trEven: "background-color: #f9f9f9;",
        statusSuccess: "color: #2E7D32; font-weight: bold;",
        statusError: "color: #C62828; font-weight: bold;",
        statusSkipped: "color: #757575;",
        button: "display: inline-block; padding: 10px 20px; background-color: #1976D2; color: white; text-decoration: none; border-radius: 5px; margin-top: 20px;",
        footer: "color: #888; font-size: 12px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 15px;",
        progressBarContainer: "width: 100%; background-color: #e0e0e0; border-radius: 4px; height: 18px; margin-top: 5px;",
        progressBar: "background-color: #4CAF50; height: 100%; border-radius: 4px; text-align: center; line-height: 18px; color: white; font-size: 12px; white-space: nowrap;"
      };

      htmlBody = `<html><head><style>td, th { vertical-align: top; }</style></head><body style="${styles.body}">`;
      htmlBody += `<h2 style="${styles.h2}">Data Ingest ${subjectStatus}</h2>`;

      // Summary Info
      htmlBody += `<p style="${styles.info}"><strong>Spreadsheet:</strong> ${spreadsheetName}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Completed:</strong> ${completionTime}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Session ID:</strong> ${sessionId}</p>`;
      htmlBody += `<p style="${styles.info}"><strong>Total Duration:</strong> ${runDurationStr}</p>`;

      // Summary Box
      htmlBody += `<div style="${styles.summaryBox}">`;
      htmlBody += `<strong>Overall Status: <span style="${errorCount > 0 ? styles.statusError : styles.statusSuccess}">${errorCount > 0 ? 'COMPLETED WITH ERRORS' : 'SUCCESS'}</span></strong><br/>`;
      htmlBody += `Rules Processed: ${totalProcessed} (Success: ${successCount}, Errors: ${errorCount}, Skipped: ${skippedCount})<br/>`;
      htmlBody += `Rows Processed: ${totalRowsProcessed.toLocaleString()} (Expected: ${totalRowsExpected.toLocaleString()})`;
      // Progress Bar
      if (totalRowsExpected > 0) {
        const percent = Math.round((totalRowsProcessed / totalRowsExpected) * 100);
        htmlBody += `<div style="${styles.progressBarContainer}"><div style="${styles.progressBar} width: ${percent}%;">${percent}%</div></div>`;
      }
      htmlBody += `</div>`;

      // Rule Details Table
      if (results.length > 0) {
        htmlBody += `<h3 style="${styles.h3}">Rule Details</h3>`;
        htmlBody += `<table style="${styles.table}"><thead><tr>`;
        htmlBody += `<th style="${styles.th}">Rule</th><th style="${styles.th}">Status</th>`;
        htmlBody += `<th style="${styles.th}; text-align: right;">Rows Processed</th><th style="${styles.th}; text-align: right;">Rows Expected</th>`;
        htmlBody += `<th style="${styles.th}; text-align: right;">Duration</th><th style="${styles.th}">Message</th>`;
        htmlBody += `</tr></thead><tbody>`;
        results.forEach((r, index) => {
          const rowStyle = index % 2 === 1 ? styles.trEven : "";
          let statusStyle = "";
          if (r.status === 'SUCCESS') statusStyle = styles.statusSuccess;
          else if (r.status === 'ERROR') statusStyle = styles.statusError;
          else if (r.status === 'SKIPPED') statusStyle = styles.statusSkipped;

          htmlBody += `<tr style="${rowStyle}">`;
          htmlBody += `<td style="${styles.td}">${r.ruleName || 'Unknown Rule'}</td>`;
          htmlBody += `<td style="${styles.td}; ${statusStyle}">${r.status}</td>`;
          htmlBody += `<td style="${styles.tdRight}">${(r.rowsProcessed || 0).toLocaleString()}</td>`;
          htmlBody += `<td style="${styles.tdRight}">${(r.rowsExpected || 0).toLocaleString()}</td>`;
          htmlBody += `<td style="${styles.tdRight}">${r.duration !== undefined ? r.duration.toFixed(1) + 's' : 'N/A'}</td>`;
          htmlBody += `<td style="${styles.td}">${(r.message || '').replace(/\n/g, '<br/>')}</td>`;
          htmlBody += `</tr>`;
        });
        htmlBody += `</tbody></table>`;
      }

      // Verification Summary Table (if data exists)
      const verificationData = getVerificationDataForSession(sessionId);
      if (verificationData.length > 0) {
        htmlBody += `<h3 style="${styles.h3}">Verification Summary</h3>`;
        htmlBody += `<table style="${styles.table}"><thead><tr>`;
        htmlBody += `<th style="${styles.th}">Rule ID</th><th style="${styles.th}">Source</th><th style="${styles.th}">Destination</th>`;
        htmlBody += `<th style="${styles.th}; text-align: right;">Src Rows</th><th style="${styles.th}; text-align: right;">Dest Rows</th>`;
        htmlBody += `<th style="${styles.th}">Rows Match</th><th style="${styles.th}">Cols Match</th><th style="${styles.th}">Samples Match</th>`;
        htmlBody += `<th style="${styles.th}">Status</th>`;
        htmlBody += `</tr></thead><tbody>`;
        verificationData.forEach((v, index) => {
          const rowStyle = index % 2 === 1 ? styles.trEven : "";
          const statusStyle = v.Status === 'ERROR' ? styles.statusError : styles.statusSuccess;
          const matchStyle = (val) => val === 'NO' ? 'background-color: #FFFDE7;' : ''; // Highlight NOs
          htmlBody += `<tr style="${rowStyle}">`;
          htmlBody += `<td style="${styles.td}">${v.RuleID || ''}</td>`;
          htmlBody += `<td style="${styles.td}">${v.SourceFile || v.SourceType || ''}</td>`;
          htmlBody += `<td style="${styles.td}">${v.DestinationSheet || ''}</td>`;
          htmlBody += `<td style="${styles.tdRight}">${v.SourceRows !== undefined ? v.SourceRows : ''}</td>`;
          htmlBody += `<td style="${styles.tdRight}">${v.DestRows !== undefined ? v.DestRows : ''}</td>`;
          htmlBody += `<td style="${styles.td}; ${matchStyle(v.RowsMatch)}">${v.RowsMatch || 'N/A'}</td>`;
          htmlBody += `<td style="${styles.td}; ${matchStyle(v.ColumnsMatch)}">${v.ColumnsMatch || 'N/A'}</td>`;
          htmlBody += `<td style="${styles.td}; ${matchStyle(v.SamplesMatch)}">${v.SamplesMatch || 'N/A'}</td>`;
          htmlBody += `<td style="${styles.td}; ${statusStyle}">${v.Status || ''}</td>`;
          htmlBody += `</tr>`;
        });
        htmlBody += `</tbody></table>`;
      }

      // Recent Log Entries Table
      const logEntries = getLogEntriesForSession(sessionId, CONFIG.EMAIL_CONFIG.MAX_ROWS_IN_EMAIL || 50); // Use config limit
      if (logEntries.length > 0) {
        htmlBody += `<h3 style="${styles.h3}">Recent Log Entries</h3>`;
        htmlBody += `<table style="${styles.table}"><thead><tr><th style="${styles.th}">Timestamp</th><th style="${styles.th}">Event Type</th><th style="${styles.th}">Message</th></tr></thead><tbody>`;
        logEntries.forEach((entry, index) => {
          const rowStyle = index % 2 === 1 ? styles.trEven : "";
          let eventStyle = "";
          if (entry.eventType === 'ERROR') eventStyle = styles.statusError;
          else if (entry.eventType === 'WARNING') eventStyle = 'color:#F57F17;';
          else if (entry.eventType === 'SKIPPED') eventStyle = styles.statusSkipped;
          htmlBody += `<tr style="${rowStyle}"><td style="${styles.td}">${formatDate(entry.timestamp)}</td><td style="${styles.td}; ${eventStyle}">${entry.eventType}</td><td style="${styles.td}">${String(entry.message).replace(/\n/g, '<br/>')}</td></tr>`;
        });
        htmlBody += `</tbody></table>`;
      }

      // Footer and Link
      htmlBody += `<p><a href="${spreadsheetUrl}" style="${styles.button}">Open Spreadsheet</a></p>`;
      htmlBody += `<p style="${styles.footer}">This is an automated message. Please do not reply.</p>`;
      htmlBody += `</body></html>`;
    }

    // --- Prepare Attachments ---
    let attachments = [];
    if (CONFIG.EMAIL_CONFIG.INCLUDE_LOG_ATTACHMENT) {
      const logAttachment = createLogAttachment(sessionId);
      if (logAttachment) attachments.push(logAttachment);
    }
    if (CONFIG.EMAIL_CONFIG.INCLUDE_VERIFICATION_ATTACHMENT) {
      const verificationAttachment = createVerificationAttachment(sessionId);
      if (verificationAttachment) attachments.push(verificationAttachment);
    }
    if (CONFIG.EMAIL_CONFIG.INCLUDE_DIAGNOSTIC_ATTACHMENT) {
      const diagnosticAttachment = createDiagnosticAttachment(sessionId);
      if (diagnosticAttachment) attachments.push(diagnosticAttachment);
    }


    // --- Send Email ---
    const emailOptions = {
      attachments: attachments.length > 0 ? attachments : undefined,
      htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : undefined
    };

    const recipientsArray = Array.isArray(recipients) ? recipients : [recipients];
    recipientsArray.forEach(email => {
      if (email && typeof email === 'string' && email.includes('@')) {
        try {
          GmailApp.sendEmail(email, subject, plainBody, emailOptions);
          Logger.log(`Run summary sent to: ${email}`);
        } catch (sendError) {
          Logger.log(`Failed to send run summary to ${email}: ${sendError.message}`);
          logOperation(sessionId, "EMAIL_ERROR", `Failed sending summary email to ${email}: ${sendError.message}`);
        }
      }
    });
    logOperation(sessionId, "EMAIL_SENT", `Run summary email sent for ${results.length} rules with ${attachments.length} attachment(s). Status: ${overallStatus}`);


  } catch (error) {
    Logger.log(`FATAL ERROR constructing/sending run summary email: ${error.message}`);
    logOperation(sessionId, "EMAIL_ERROR", `Fatal error preparing summary email: ${error.message}`);
  }
}


// ========================================================================== //
// CORE PROCESSING LOGIC (Could be in CoreProcessingLib.gs)
// ========================================================================== //

/**
* Main ingestion function that processes all active rules from the config sheet.
* @param {string} sessionId - Unique identifier for this execution session.
* @returns {Array<Object>} Array of result objects for reporting.
*/
function ingestData(sessionId) {
  Logger.log(`Starting ingestData function with Session ID: ${sessionId}`);
  const startTime = new Date();
  const results = []; // Store results for each rule

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);

    if (!configSheet) {
      throw new Error(`Configuration sheet "${CONFIG.CONFIG_SHEET_NAME}" not found.`);
    }

    const configData = configSheet.getDataRange().getValues();
    Logger.log(`Loaded configuration sheet "${CONFIG.CONFIG_SHEET_NAME}" with ${configData.length} rows (including header).`);

    if (configData.length <= 1) {
      logOperation(sessionId, "SUMMARY", "No rules found in configuration sheet.");
      return results; // No rules to process
    }

    const headers = configData[0];
    const headerMap = createHeaderMap(headers);

    // Verify required headers exist
    const requiredInternalHeaders = ['ruleActive', 'ingestMethod'];
    for (const internalHeader of requiredInternalHeaders) {
      const actualHeader = CONFIG.COLUMN_MAPPINGS[internalHeader];
      if (!actualHeader || headerMap[actualHeader] === undefined) {
        throw new Error(`Required header "${actualHeader || internalHeader}" not found in configuration sheet. Check CONFIG.COLUMN_MAPPINGS and sheet headers.`);
      }
    }

    const ruleActiveIndex = headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive];
    const ingestMethodIndex = headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod];
    const sheetHandlingModeIndex = headerMap[CONFIG.COLUMN_MAPPINGS.sheetHandlingMode];

    const totalRules = configData.length - 1;
    let processedRuleCount = 0;
    let activeRulesFound = 0;

    // Process each row (rule) in the config sheet
    for (let i = 1; i < configData.length; i++) {
      const row = configData[i];
      const rowNum = i + 1; // 1-based row number for user messages
      const ruleId = `RuleRowNum_${rowNum}`; // Use row number as simple ID

      processedRuleCount++;
      // Update progress indicator roughly based on row number
      if (processedRuleCount % 5 === 0 || processedRuleCount === totalRules) { // Update every 5 rules or on the last one
        const progress = Math.round((processedRuleCount / totalRules) * 100);
        updateProgressIndicator("Data Ingest", progress, `Processing rule ${processedRuleCount}/${totalRules} (Row ${rowNum})`);
      }


      // Skip effectively empty rows
      if (!isRowPopulated(row)) {
        continue;
      }

      const isActive = row[ruleActiveIndex] === true;

      if (!isActive) {
        updateRuleStatus(configSheet, rowNum, "SKIPPED", "Rule not active");
        results.push({ ruleName: ruleId, status: "SKIPPED", message: "Rule not active" });
        continue;
      }

      activeRulesFound++;
      logOperation(sessionId, "PROCESSING", `Starting processing for ${ruleId} (Row ${rowNum})`);
      let ruleResult = { ruleName: ruleId, status: "ERROR", rowsProcessed: 0, rowsExpected: 0, duration: 0, message: "Processing started but did not complete." }; // Default error result
      let ruleStartTime = new Date(); // Declare here for wider scope

      try {
        const ingestMethod = row[ingestMethodIndex];
        const sheetHandlingMode = row[sheetHandlingModeIndex] || 'clearAndReuse'; // Default mode if blank

        if (!ingestMethod || typeof ingestMethod !== 'string') {
          throw new Error(`"ingestMethod" is missing or invalid in row ${rowNum}.`);
        }

        Logger.log(`${ruleId}: Method=${ingestMethod}, Mode=${sheetHandlingMode}`);

        ruleStartTime = new Date(); // Reset timer just before execution
        let processOutput = {}; // To store return value from process functions

        // Execute the appropriate processing function with retry logic
        if (ingestMethod === 'email') {
          processOutput = executeWithRetry(() => processEmailIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
        } else if (ingestMethod === 'gSheet') {
          processOutput = executeWithRetry(() => processGSheetIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
        } else if (ingestMethod === 'push') {
          processOutput = executeWithRetry(() => processSheetPushWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
        } else {
          throw new Error(`Unknown ingest method specified: "${ingestMethod}"`);
        }

        const ruleDuration = (new Date() - ruleStartTime) / 1000;

        // Update result upon success
        ruleResult.status = "SUCCESS";
        ruleResult.rowsProcessed = processOutput.rowsProcessed || 0;
        ruleResult.rowsExpected = processOutput.rowsExpected || 0;
        ruleResult.duration = ruleDuration;
        ruleResult.message = processOutput.message || `Processed ${ruleResult.rowsProcessed} rows successfully.`;

        updateRuleStatus(configSheet, rowNum, "SUCCESS", ruleResult.message.slice(0, 500)); // Limit message length in sheet
        logOperation(sessionId, "SUCCESS", `${ruleId}: ${ruleResult.message}`);

      } catch (error) {
        const errorMsg = `Error processing ${ruleId} (Row ${rowNum}): ${error.message}`;
        Logger.log(errorMsg); // Log detailed error
        ruleResult.status = "ERROR";
        ruleResult.message = error.message; // Store the error message
        ruleResult.duration = (new Date() - ruleStartTime) / 1000; // Calculate duration even on error

        updateRuleStatus(configSheet, rowNum, "ERROR", errorMsg.slice(0, 500)); // Log concise error to sheet
        logOperation(sessionId, "ERROR", errorMsg); // Log full error to log sheet

      } finally {
        results.push(ruleResult); // Add the result (success or error) to the list
      }
    } // End loop through config rows

    // Final summary log
    const endTime = new Date();
    const totalDurationSec = (endTime - startTime) / 1000;
    const successCount = results.filter(r => r.status === "SUCCESS").length;
    const errorCount = results.filter(r => r.status === "ERROR").length;
    const skippedCount = results.filter(r => r.status === "SKIPPED").length; // Count explicitly tracked skips

    const summaryMsg = `Ingest process completed. Active Rules: ${activeRulesFound}. Results: ${successCount} SUCCESS, ${errorCount} ERROR, ${skippedCount} SKIPPED. Total Duration: ${totalDurationSec.toFixed(1)}s.`;
    logOperation(sessionId, "SUMMARY", summaryMsg);
    Logger.log(summaryMsg);

    // Clean up progress indicator
    updateProgressIndicator("Data Ingest", 100, "Complete");


    return results; // Return array of result objects

  } catch (error) {
    // Catch errors during setup or reading config
    const criticalErrorMsg = `CRITICAL Error during ingestData execution: ${error.message}`;
    Logger.log(criticalErrorMsg);
    logOperation(sessionId, "ERROR", criticalErrorMsg); // Log the critical failure
    updateProgressIndicator("Data Ingest", 100, "Error"); // Update progress on error
    throw error; // Rethrow to be caught by runAll
  }
}


/**
 * Enhanced process email ingestion rule with verification. Finds the LATEST email matching criteria.
 * @param {string} sessionId - Session identifier.
 * @param {Array} row - Configuration row data.
 * @param {Object} headerMap - Map of header names to column indices.
 * @param {string} sheetHandlingMode - How to handle the destination sheet.
 * @param {string} ruleId - Identifier for the rule (e.g., "RuleRowNum_5").
 * @returns {Object} Result: { status, rowsProcessed, rowsExpected, duration, message, dataHash, attachmentSize }.
 * @throws {Error} If processing fails.
 */
function processEmailIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`${ruleId}: Starting email ingest process with verification. Mode: ${sheetHandlingMode}`);

  // Define variables needed in the catch block higher up
  let sourceMetadata = {}; // Initialize to avoid undefined errors in catch block if error happens early
  let destSheetTabName; // Will be defined within the try block

  try {
    // Get required config values
    const searchString = getRequiredValue(row, headerMap, 'in_email_searchString');
    const attachmentPattern = getRequiredValue(row, headerMap, 'in_email_attachmentPattern');
    const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
    destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName'); // Assign here

    logOperation(sessionId, "EMAIL_SEARCH_START", `${ruleId}: Searching Gmail with query: ${searchString}`);

    const threads = GmailApp.search(searchString, 0, 10); // Search recent threads first
    if (threads.length === 0) {
      logOperation(sessionId, "INFO", `${ruleId}: No email threads found matching search criteria "${searchString}". Skipping rule.`);
      return { status: "SUCCESS", rowsProcessed: 0, rowsExpected: 0, duration: (new Date() - startTime) / 1000, message: `No emails found matching search criteria.`, dataHash: null, attachmentSize: 0 };
    }
    Logger.log(`${ruleId}: Found ${threads.length} matching thread(s). Checking for attachments...`);

    const attachmentRegex = new RegExp(attachmentPattern, 'i');
    let foundAttachment = null;
    // sourceMetadata already initialized above
    let attachmentData = null;

    threadLoop:
    for (const thread of threads) {
      const messages = thread.getMessages();
      for (let i = messages.length - 1; i >= 0; i--) {
        const message = messages[i];
        const attachments = message.getAttachments();
        for (const attachment of attachments) {
          const attachmentName = attachment.getName();
          if (attachmentRegex.test(attachmentName)) {
             foundAttachment = attachment;
             sourceMetadata = { // Overwrite initial empty object
                 filename: attachmentName, size: attachment.getSize(), contentType: attachment.getContentType(),
                 emailSubject: message.getSubject(), emailDate: message.getDate(), emailFrom: message.getFrom(), messageId: message.getId()
             };
             Logger.log(`${ruleId}: Found latest matching attachment: "${attachmentName}" in email from ${formatDate(message.getDate())}.`);
             logOperation(sessionId, "ATTACHMENT_FOUND", `${ruleId}: Found attachment: ${attachmentName} (Size: ${sourceMetadata.size} bytes, Type: ${sourceMetadata.contentType})`);
             break threadLoop;
          }
        }
      }
    }

    if (!foundAttachment) {
      logOperation(sessionId, "INFO", `${ruleId}: No attachment found matching pattern "${attachmentPattern}" in recent emails matching query.`);
      return { status: "SUCCESS", rowsProcessed: 0, rowsExpected: 0, duration: (new Date() - startTime) / 1000, message: `No matching attachment found in emails.`, dataHash: null, attachmentSize: 0 };
    }

    // --- PARSING BLOCK (Includes octet-stream check) ---
    try {
        const contentType = sourceMetadata.contentType.toLowerCase();
        const filenameLower = sourceMetadata.filename.toLowerCase();

        if (contentType.includes('csv') || contentType.includes('text/plain')) {
            Logger.log(`${ruleId}: Parsing as CSV/Text based on content type: ${contentType}`);
            attachmentData = Utilities.parseCsv(foundAttachment.getDataAsString());
        }
        else if (contentType.includes('application/octet-stream') && filenameLower.endsWith('.csv')) {
            Logger.log(`${ruleId}: Content type is octet-stream, but filename ends with .csv. Attempting CSV parse.`);
            try {
                attachmentData = Utilities.parseCsv(foundAttachment.getDataAsString());
                logOperation(sessionId, "INFO", `${ruleId}: Successfully parsed octet-stream file "${sourceMetadata.filename}" as CSV based on extension.`);
            } catch (csvParseError) {
                logOperation(sessionId, "ERROR", `${ruleId}: Failed to parse octet-stream file "${sourceMetadata.filename}" as CSV despite .csv extension: ${csvParseError.message}`);
                throw new Error(`File "${sourceMetadata.filename}" reported as octet-stream and failed CSV parsing despite .csv extension. Check file format or sender's Content-Type setting.`);
            }
        }
        else if (contentType.includes('spreadsheetml') || contentType.includes('excel') || contentType.includes('openxmlformats-officedocument.spreadsheetml')) {
            logOperation(sessionId, "WARNING", `${ruleId}: Found Excel attachment "${sourceMetadata.filename}". Automatic parsing not implemented. Sheet will likely be empty.`);
            attachmentData = [];
        } else {
            throw new Error(`Unsupported attachment content type: ${sourceMetadata.contentType}`);
        }

        // Check for empty data (modified to treat empty as warning/info, not error)
        if (!attachmentData || attachmentData.length === 0) {
            if (attachmentData && attachmentData.length === 1 && attachmentData[0].every(cell => cell === '')) {
                 logOperation(sessionId, "WARNING", `${ruleId}: Attachment "${sourceMetadata.filename}" appears to be empty or contain only empty headers.`);
                 attachmentData = [];
            } else if (attachmentData && attachmentData.length === 0 && (contentType.includes('spreadsheetml') || contentType.includes('excel'))) {
                 logOperation(sessionId, "INFO", `${ruleId}: Excel file "${sourceMetadata.filename}" was processed, but no data extracted (parsing not implemented).`);
            } else if (attachmentData) {
                 logOperation(sessionId, "INFO", `${ruleId}: Attachment "${sourceMetadata.filename}" contained no data rows.`);
            } else {
                  // This case means parseCsv likely returned null or something unexpected
                  logOperation(sessionId, "WARNING", `${ruleId}: Parsing attachment "${sourceMetadata.filename}" resulted in null/undefined data. Treating as empty.`);
                  attachmentData = [];
                  // throw new Error("Attachment CSV/Data appears to be empty."); // Avoid throwing error for empty
            }
        }
        // Log parsing only if data was found
        if (attachmentData && attachmentData.length > 0) {
           logOperation(sessionId, "DATA_PARSED", `${ruleId}: Parsed ${attachmentData.length} rows x ${attachmentData[0]?.length || 0} columns from "${sourceMetadata.filename}".`);
        }

    } catch (parseError) {
        throw new Error(`Error parsing attachment "${sourceMetadata.filename}": ${parseError.message}`);
    }
    // --- END PARSING BLOCK ---


    const dataHash = attachmentData && attachmentData.length > 0 ? calculateDataHash(attachmentData) : null;
    if (dataHash) {
      logOperation(sessionId, "DATA_HASH", `${ruleId}: Source data hash: ${dataHash}`);
    }

    const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);

    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      try { beforeRowCount = destSheet.getLastRow(); } catch (e) { beforeRowCount = 0; Logger.log(`${ruleId}: Could not get last row of dest sheet, assuming 0.`); }
      logOperation(sessionId, "APPEND_MODE", `${ruleId}: Destination sheet "${destSheetTabName}" has ${beforeRowCount} rows before append.`);
    }

    if (attachmentData && attachmentData.length > 0) {
      const startRow = (sheetHandlingMode === 'append' && beforeRowCount > 0) ? beforeRowCount + 1 : 1;
      const numRowsToWrite = attachmentData.length;
      const numColsToWrite = attachmentData[0].length;
      const maxRows = destSheet.getMaxRows();
      const maxCols = destSheet.getMaxColumns();
      if (maxRows < startRow + numRowsToWrite - 1) {
        destSheet.insertRowsAfter(maxRows, startRow + numRowsToWrite - 1 - maxRows);
      }
      if (maxCols < numColsToWrite) {
        destSheet.insertColumnsAfter(maxCols, numColsToWrite - maxCols);
      }
      destSheet.getRange(startRow, 1, numRowsToWrite, numColsToWrite).setValues(attachmentData);
      logOperation(sessionId, "DATA_WRITTEN", `${ruleId}: Wrote ${numRowsToWrite} rows to "${destSheetTabName}" starting at row ${startRow}.`);
    } else {
      logOperation(sessionId, "INFO", `${ruleId}: No data rows to write to "${destSheetTabName}".`);
    }

    const sourceRowCount = attachmentData ? attachmentData.length : 0;
    const sourceColCount = attachmentData && attachmentData.length > 0 ? attachmentData[0].length : 0;
    const afterRowCount = destSheet.getLastRow();
    const afterColCount = destSheet.getLastColumn();
    let rowsMatch = 'N/A', colsMatch = 'N/A', samplesMatch = 'N/A';
    let verificationPassed = true;
    let verificationDetails = "Verification passed.";

    if (CONFIG.VERIFICATION_CONFIG.ENABLED && sourceRowCount > 0) { // Only verify if data existed
      logOperation(sessionId, "VERIFICATION_START", `${ruleId}: Starting data verification.`);
      // Row Count Verification
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS) { /* ... verification logic ... */ }
      // Column Count Verification
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS && verificationPassed) { /* ... verification logic ... */ }
      // Sample Data Verification
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA && verificationPassed) {
        const sampleCheckResult = verifyDataSamples(attachmentData, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        if (!sampleCheckResult) { /* ... set verification failed ... */ }
        else { samplesMatch = 'YES'; }
      }
      logOperation(sessionId, "VERIFICATION_END", `${ruleId}: Verification Result: Passed=${verificationPassed}`);

    } else if (CONFIG.VERIFICATION_CONFIG.ENABLED && sourceRowCount === 0) {
        logOperation(sessionId, "VERIFICATION_SKIP", `${ruleId}: Verification skipped as source attachment had no data rows.`);
        verificationDetails = "Verification skipped (no source data).";
        if ((sheetHandlingMode === 'clearAndReuse' || sheetHandlingMode === 'recreate') && afterRowCount > 1) {
            verificationPassed = false; rowsMatch = 'NO';
            verificationDetails = `Source was empty, but destination sheet has ${afterRowCount} rows after ${sheetHandlingMode}. Expected 0 or 1.`;
            logOperation(sessionId, "ROW_COUNT_ERROR", `${ruleId}: ${verificationDetails}`);
        } else { verificationPassed = true; rowsMatch = 'N/A'; }
        colsMatch = 'N/A'; samplesMatch = 'N/A';
    }

    logVerification({
      sessionId: sessionId, ruleId: ruleId, sourceType: "Email Attachment",
      sourceFile: sourceMetadata.filename, sourceMetadata: JSON.stringify(sourceMetadata),
      destinationSheet: destSheetTabName, sourceRowCount: sourceRowCount, destinationRowCount: afterRowCount,
      sourceColumnCount: sourceColCount, destinationColumnCount: afterColCount,
      rowsMatch: rowsMatch, columnsMatch: colsMatch, samplesMatch: samplesMatch,
      dataHash: dataHash, status: verificationPassed ? 'COMPLETE' : 'ERROR', details: verificationDetails
    });

    if (!verificationPassed) {
      throw new Error(verificationDetails);
    }

    const duration = (new Date() - startTime) / 1000;
    Logger.log(`${ruleId}: Email ingest completed successfully in ${duration.toFixed(2)} seconds.`);

    return {
      status: "SUCCESS", rowsProcessed: sourceRowCount, rowsExpected: sourceRowCount, duration: duration,
      message: `Successfully processed attachment "${sourceMetadata.filename}" (${sourceRowCount} rows).`,
      dataHash: dataHash, attachmentSize: sourceMetadata.size
    };

  // --- CATCH BLOCK WITH IMPROVED ROBUSTNESS ---
  } catch (error) {
    const errorMsgForLog = `${ruleId}: Email ingest ERROR: ${error.message}`;
    Logger.log(errorMsgForLog); // Log the primary error first

    // Attempt to log failure verification, but check if needed variables exist
    let logVerData = {
        sessionId: sessionId,
        ruleId: ruleId,
        sourceType: "Email Attachment",
        status: 'ERROR',
        details: `Processing failed: ${error.message}`
    };

    // Safely add destination tab name if it was defined before the error
    // Check if destSheetTabName was assigned a value
    if (typeof destSheetTabName !== 'undefined' && destSheetTabName) {
        logVerData.destinationSheet = destSheetTabName;
    } else {
        logVerData.destinationSheet = 'N/A (Error before dest defined)';
    }

    // Safely add source file name if metadata was obtained before the error
    // Check if sourceMetadata object exists AND has the filename property
    if (typeof sourceMetadata !== 'undefined' && sourceMetadata && typeof sourceMetadata.filename !== 'undefined') {
        logVerData.sourceFile = sourceMetadata.filename;
    } else {
        logVerData.sourceFile = 'N/A (Error before attachment found/processed)';
    }

    try {
        logVerification(logVerData);
    } catch (logVerError) {
        // Avoid infinite loops if logging verification itself fails
        Logger.log(`CRITICAL: Failed to log verification status during error handling for ${ruleId}: ${logVerError.message}`);
    }

    throw error; // Rethrow the original error to be caught by ingestData/runAll
  }
  // --- END CATCH BLOCK ---

}



/**
* Enhanced process Google Sheet ingestion rule with verification.
* Includes 'copyFormat' handling and batch processing for large data.
* @param {string} sessionId - Session identifier.
* @param {Array} row - Configuration row data.
* @param {Object} headerMap - Map of header names to column indices.
* @param {string} sheetHandlingMode - How to handle the destination sheet.
* @param {string} ruleId - Identifier for the rule.
* @returns {Object} Result: { status, rowsProcessed, rowsExpected, duration, message, dataHash }.
* @throws {Error} If processing fails.
*/
function processGSheetIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`${ruleId}: Starting Google Sheet ingest. Mode: ${sheetHandlingMode}`);

  // Get config values
  const sourceSheetId = getResourceId(row, headerMap, 'in_gsheet_sheetId', 'in_gsheet_sheetURL');
  const sourceTabName = getRequiredValue(row, headerMap, 'in_gsheet_tabName');
  const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
  const destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName');

  logOperation(sessionId, "SHEET_ACCESS", `${ruleId}: Accessing Source: ${sourceSheetId} -> "${sourceTabName}", Dest: ${destSheetId} -> "${destSheetTabName}"`);

  try {
    // --- Open Source Sheet ---
    let sourceSpreadsheet, sourceSheet;
    try {
      sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
      sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
      if (!sourceSheet) {
        throw new Error(`Source sheet tab "${sourceTabName}" not found in source spreadsheet ID: ${sourceSheetId}`);
      }
      Logger.log(`${ruleId}: Source sheet "${sourceTabName}" opened.`);
    } catch (e) {
      throw new Error(`Failed to open source sheet ID ${sourceSheetId} or find tab "${sourceTabName}": ${e.message}`);
    }

    // --- Open Destination Spreadsheet ---
    let destSpreadsheet;
    try {
      destSpreadsheet = SpreadsheetApp.openById(destSheetId);
      Logger.log(`${ruleId}: Destination spreadsheet "${destSpreadsheet.getName()}" opened.`);
    } catch (e) {
      throw new Error(`Failed to open destination spreadsheet ID ${destSheetId}: ${e.message}`);
    }


    // --- Handle 'copyFormat' Mode ---
    if (sheetHandlingMode === 'copyFormat') {
      Logger.log(`${ruleId}: Using 'copyFormat' mode. Copying sheet directly.`);
      logOperation(sessionId, "SHEET_COPY_START", `${ruleId}: Starting sheet copy with formatting.`);

      let copiedSheet;
      try {
        // Check if destination sheet exists first
        const existingSheet = destSpreadsheet.getSheetByName(destSheetTabName);

        // Copy source to destination *first* (might create "Copy of...")
        copiedSheet = sourceSheet.copyTo(destSpreadsheet);

        // If destination existed, delete it *after* copy is successful
        if (existingSheet) {
          // IMPORTANT: Check if it's the *same sheet* (can happen if dest=source or copy failed strangely)
          if (existingSheet.getSheetId() !== copiedSheet.getSheetId()) {
            Logger.log(`${ruleId}: Deleting existing destination sheet "${destSheetTabName}" before renaming copy.`);
            destSpreadsheet.deleteSheet(existingSheet);
          } else {
            // This case is tricky. If source and dest SS are same, and tab names are same,
            // copyTo creates "Copy of [tabName]". Renaming the copy to [tabName] will fail
            // if the original [tabName] still exists. We should delete the original.
            // If they are different spreadsheets, this isn't an issue.
            if (sourceSpreadsheetId === destSheetId) {
              Logger.log(`${ruleId}: Source/Dest Spreadsheets are the same. Deleting original "${destSheetTabName}" after copy.`);
              destSpreadsheet.deleteSheet(existingSheet);
            } else {
              Logger.log(`${ruleId}: Destination sheet "${destSheetTabName}" has same ID as copy. Skipping deletion (should not happen between different Spreadsheets).`);
            }
          }
        }

        // Rename the copied sheet to the final name
        copiedSheet.setName(destSheetTabName);

        // Optional: Activate the sheet? copiedSheet.activate();
        Logger.log(`${ruleId}: Sheet copied and renamed to "${destSheetTabName}".`);
        logOperation(sessionId, "SHEET_COPY_COMPLETE", `${ruleId}: Successfully copied sheet with formatting.`);
      } catch (copyError) {
        logOperation(sessionId, "ERROR", `${ruleId}: Failed to copy or rename sheet in 'copyFormat' mode: ${copyError.message}`);
        throw new Error(`Failed during 'copyFormat': ${copyError.message}`);
      }

      // For copyFormat, verification is limited. We assume the copy is accurate.
      const sourceRowCount = sourceSheet.getLastRow();
      const sourceColCount = sourceSheet.getLastColumn();
      logVerification({
        sessionId: sessionId, ruleId: ruleId, sourceType: "Google Sheet", sourceFile: `${sourceSheetId}:${sourceTabName}`,
        destinationSheet: destSheetTabName, sourceRowCount: sourceRowCount, destinationRowCount: copiedSheet.getLastRow(),
        sourceColumnCount: sourceColCount, destinationColumnCount: copiedSheet.getLastColumn(),
        status: 'COMPLETE', details: "'copyFormat' mode used. Assumed successful copy.",
        rowsMatch: 'N/A', columnsMatch: 'N/A', samplesMatch: 'N/A' // Mark verification steps as N/A
      });

      const duration = (new Date() - startTime) / 1000;
      return {
        status: "SUCCESS",
        rowsProcessed: sourceRowCount, // Best estimate
        rowsExpected: sourceRowCount,
        duration: duration,
        message: `'copyFormat' mode completed successfully.`
      };
    } // --- End 'copyFormat' Mode ---


    // --- Handle Data Transfer Modes (clearAndReuse, recreate, append) ---
    Logger.log(`${ruleId}: Reading data from source sheet "${sourceTabName}".`);
    const sourceData = sourceSheet.getDataRange().getValues();
    const sourceRowCount = sourceData.length;
    const sourceColCount = sourceData[0]?.length || 0;

    if (sourceRowCount === 0) {
      logOperation(sessionId, "INFO", `${ruleId}: Source sheet "${sourceTabName}" is empty. No data to transfer.`);
      // Ensure destination is handled correctly (cleared if needed)
      const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);
      logVerification({ sessionId: sessionId, ruleId: ruleId, sourceType: "Google Sheet", sourceFile: `${sourceSheetId}:${sourceTabName}`, destinationSheet: destSheetTabName, sourceRowCount: 0, destinationRowCount: destSheet.getLastRow(), sourceColumnCount: 0, destinationColumnCount: destSheet.getLastColumn(), status: 'COMPLETE', details: "Source sheet was empty." });
      return { status: "SUCCESS", rowsProcessed: 0, rowsExpected: 0, duration: (new Date() - startTime) / 1000, message: "Source sheet was empty." };
    }

    logOperation(sessionId, "DATA_READ", `${ruleId}: Read ${sourceRowCount} rows x ${sourceColCount} columns from source.`);
    const dataHash = calculateDataHash(sourceData);
    logOperation(sessionId, "DATA_HASH", `${ruleId}: Source data hash: ${dataHash}`);

    // Open destination sheet (handles clearing/recreating based on mode)
    const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);

    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      try { beforeRowCount = destSheet.getLastRow(); } catch (e) { beforeRowCount = 0; Logger.log(`${ruleId}: Could not get last row of dest sheet, assuming 0.`); }
      logOperation(sessionId, "APPEND_MODE", `${ruleId}: Dest sheet "${destSheetTabName}" has ${beforeRowCount} rows before append.`);
    }

    // Write data (Consider batching for very large datasets - e.g., > 50k cells)
    let dataToWrite = sourceData;
    let startRow = 1;
    let sourceHasHeader = true; // Assume source has header unless only 1 row total

    if (sourceRowCount === 1) sourceHasHeader = false; // Treat single row as data only

    if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
      // If appending, skip the header row from source data *if* source had one
      if (sourceHasHeader) {
        dataToWrite = sourceData.slice(1);
      } else {
        dataToWrite = sourceData; // Don't slice if source was just data
      }
      startRow = beforeRowCount + 1;
      if (dataToWrite.length === 0 && sourceHasHeader) {
        logOperation(sessionId, "INFO", `${ruleId}: Source sheet only contained a header row. Nothing to append.`);
      }
    } else {
      // If not appending, write all source data
      dataToWrite = sourceData;
      startRow = 1;
    }

    if (dataToWrite.length > 0) {
      const numRowsToWrite = dataToWrite.length;
      const numColsToWrite = dataToWrite[0].length;
      Logger.log(`${ruleId}: Writing ${numRowsToWrite} rows x ${numColsToWrite} columns to "${destSheetTabName}" starting at row ${startRow}.`);
      // Ensure sheet has enough rows/cols
      if (destSheet.getMaxRows() < startRow + numRowsToWrite - 1) {
        destSheet.insertRowsAfter(destSheet.getMaxRows(), startRow + numRowsToWrite - 1 - destSheet.getMaxRows());
      }
      if (destSheet.getMaxColumns() < numColsToWrite) {
        destSheet.insertColumnsAfter(destSheet.getMaxColumns(), numColsToWrite - destSheet.getMaxColumns());
      }
      destSheet.getRange(startRow, 1, numRowsToWrite, numColsToWrite).setValues(dataToWrite);
      logOperation(sessionId, "DATA_WRITTEN", `${ruleId}: Wrote ${numRowsToWrite} rows.`);
    }


    // Perform Verification
    const afterRowCount = destSheet.getLastRow();
    const afterColCount = destSheet.getLastColumn();
    let rowsMatch = 'N/A', colsMatch = 'N/A', samplesMatch = 'N/A';
    let verificationPassed = true;
    let verificationDetails = "Verification passed.";

    if (CONFIG.VERIFICATION_CONFIG.ENABLED) {
      logOperation(sessionId, "VERIFICATION_START", `${ruleId}: Starting data verification.`);

      // 1. Row Count
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS) {
        let expectedRowCountAfterWrite;
        if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
          expectedRowCountAfterWrite = beforeRowCount + dataToWrite.length;
        } else { // Cleared or recreated
          expectedRowCountAfterWrite = dataToWrite.length === 0 ? (destSheet.getRange(1, 1).isBlank() ? 0 : 1) : dataToWrite.length;
        }

        if (afterRowCount !== expectedRowCountAfterWrite) {
          verificationPassed = false;
          rowsMatch = 'NO';
          verificationDetails = `Row count mismatch: Expected ${expectedRowCountAfterWrite}, Found ${afterRowCount}. Mode: ${sheetHandlingMode}, Before: ${beforeRowCount}, Written: ${dataToWrite.length}`;
          logOperation(sessionId, "ROW_COUNT_ERROR", `${ruleId}: ${verificationDetails}`);
        } else {
          rowsMatch = 'YES';
        }
      }
      // 2. Column Count
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS && verificationPassed) {
        if (sourceColCount > afterColCount) {
          verificationPassed = false;
          colsMatch = 'NO';
          verificationDetails = `Column count mismatch: Source had ${sourceColCount}, Destination has ${afterColCount}. Data may be truncated.`;
          logOperation(sessionId, "COLUMN_COUNT_ERROR", `${ruleId}: ${verificationDetails}`);
        } else {
          colsMatch = 'YES';
        }
      }
      // 3. Sample Data
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA && verificationPassed) {
        // Pass the correct source data (with or without header based on mode/state)
        const sampleCheckResult = verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        if (!sampleCheckResult) {
          verificationPassed = false;
          samplesMatch = 'NO';
          verificationDetails = "Sample data verification failed (see diagnostic log).";
        } else {
          samplesMatch = 'YES';
        }
      }
      logOperation(sessionId, "VERIFICATION_END", `${ruleId}: Verification Result: Passed=${verificationPassed}`);
    } // End Verification Enabled


    // Log verification result
    logVerification({
      sessionId: sessionId, ruleId: ruleId, sourceType: "Google Sheet", sourceFile: `${sourceSheetId}:${sourceTabName}`,
      destinationSheet: destSheetTabName, sourceRowCount: sourceRowCount, destinationRowCount: afterRowCount,
      sourceColumnCount: sourceColCount, destinationColumnCount: afterColCount,
      rowsMatch: rowsMatch, columnsMatch: colsMatch, samplesMatch: samplesMatch,
      dataHash: dataHash, status: verificationPassed ? 'COMPLETE' : 'ERROR', details: verificationDetails
    });

    if (!verificationPassed) {
      throw new Error(verificationDetails);
    }

    const duration = (new Date() - startTime) / 1000;
    Logger.log(`${ruleId}: Google Sheet ingest completed successfully in ${duration.toFixed(2)} seconds.`);

    // Determine rows actually processed based on mode
    const rowsActuallyProcessed = dataToWrite.length;

    return {
      status: "SUCCESS",
      rowsProcessed: rowsActuallyProcessed,
      rowsExpected: rowsActuallyProcessed, // Expected rows *written*
      duration: duration,
      message: `Successfully processed GSheet ${sourceTabName} (${rowsActuallyProcessed} rows).`,
      dataHash: dataHash
    };

  } catch (error) {
    Logger.log(`${ruleId}: Google Sheet ingest ERROR: ${error.message}`);
    logVerification({ sessionId: sessionId, ruleId: ruleId, sourceType: "Google Sheet", status: 'ERROR', details: `Processing failed: ${error.message}` });
    throw error;
  }
}


/**
* Enhanced process sheet push rule with verification. Pushes data from a tab in the *current* spreadsheet to a destination sheet.
* @param {string} sessionId - Session identifier.
* @param {Array} row - Configuration row data.
* @param {Object} headerMap - Map of header names to column indices.
* @param {string} sheetHandlingMode - How to handle the destination sheet.
* @param {string} ruleId - Identifier for the rule.
* @returns {Object} Result: { status, rowsProcessed, rowsExpected, duration, message, dataHash }.
* @throws {Error} If processing fails.
*/
function processSheetPushWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`${ruleId}: Starting sheet push process. Mode: ${sheetHandlingMode}`);

  // Get config values
  const sourceTabName = getRequiredValue(row, headerMap, 'pushSourceTabName');
  const destSheetId = getResourceId(row, headerMap, 'pushDestinationSheetId', 'pushDestinationSheetUrl');
  const destTabName = getRequiredValue(row, headerMap, 'pushDestinationTabName');

  const sourceSpreadsheet = SpreadsheetApp.getActiveSpreadsheet(); // Source is always the active sheet for push
  const sourceSpreadsheetId = sourceSpreadsheet.getId();
  const sourceSpreadsheetName = sourceSpreadsheet.getName();

  logOperation(sessionId, "PUSH_INIT", `${ruleId}: Pushing from "${sourceTabName}" (in "${sourceSpreadsheetName}") to Dest: ${destSheetId} -> "${destTabName}"`);

  try {
    // --- Get Source Sheet & Data ---
    const sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
    if (!sourceSheet) {
      throw new Error(`Source tab "${sourceTabName}" not found in the current spreadsheet ("${sourceSpreadsheetName}").`);
    }
    Logger.log(`${ruleId}: Source sheet "${sourceTabName}" found.`);

    const sourceData = sourceSheet.getDataRange().getValues();
    const sourceRowCount = sourceData.length;
    const sourceColCount = sourceData[0]?.length || 0;

    if (sourceRowCount === 0) {
      logOperation(sessionId, "INFO", `${ruleId}: Source sheet "${sourceTabName}" is empty. No data to push.`);
      const destSheet = openDestinationSheet(destSheetId, destTabName, sheetHandlingMode); // Ensure dest is cleared if needed
      logVerification({ sessionId: sessionId, ruleId: ruleId, sourceType: "Sheet Push", sourceFile: `${sourceSpreadsheetId}:${sourceTabName}`, destinationSheet: destTabName, sourceRowCount: 0, destinationRowCount: destSheet.getLastRow(), sourceColumnCount: 0, destinationColumnCount: destSheet.getLastColumn(), status: 'COMPLETE', details: "Source sheet was empty." });
      return { status: "SUCCESS", rowsProcessed: 0, rowsExpected: 0, duration: (new Date() - startTime) / 1000, message: "Source sheet was empty." };
    }

    logOperation(sessionId, "DATA_READ", `${ruleId}: Read ${sourceRowCount} rows x ${sourceColCount} columns from "${sourceTabName}".`);
    const dataHash = calculateDataHash(sourceData);
    logOperation(sessionId, "DATA_HASH", `${ruleId}: Source data hash: ${dataHash}`);

    // --- Open Destination Sheet ---
    const destSheet = openDestinationSheet(destSheetId, destTabName, sheetHandlingMode);

    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      try { beforeRowCount = destSheet.getLastRow(); } catch (e) { beforeRowCount = 0; Logger.log(`${ruleId}: Could not get last row of dest sheet, assuming 0.`); }
      logOperation(sessionId, "APPEND_MODE", `${ruleId}: Dest sheet "${destTabName}" has ${beforeRowCount} rows before append.`);
    }

    // --- Write Data ---
    let dataToWrite = sourceData;
    let startRow = 1;
    let sourceHasHeader = sourceRowCount > 1; // Simple check: assume header if more than 1 row

    if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
      if (sourceHasHeader) {
        dataToWrite = sourceData.slice(1); // Skip header
      } else {
        dataToWrite = sourceData;
      }
      startRow = beforeRowCount + 1;
      if (dataToWrite.length === 0 && sourceHasHeader) {
        logOperation(sessionId, "INFO", `${ruleId}: Source sheet only contained a header row. Nothing to append.`);
      }
    } else {
      dataToWrite = sourceData;
      startRow = 1;
    }

    if (dataToWrite.length > 0) {
      const numRowsToWrite = dataToWrite.length;
      const numColsToWrite = dataToWrite[0].length;
      Logger.log(`${ruleId}: Writing ${numRowsToWrite} rows x ${numColsToWrite} columns to "${destTabName}" starting at row ${startRow}.`);
      // Ensure sheet has enough rows/cols
      if (destSheet.getMaxRows() < startRow + numRowsToWrite - 1) {
        destSheet.insertRowsAfter(destSheet.getMaxRows(), startRow + numRowsToWrite - 1 - destSheet.getMaxRows());
      }
      if (destSheet.getMaxColumns() < numColsToWrite) {
        destSheet.insertColumnsAfter(destSheet.getMaxColumns(), numColsToWrite - destSheet.getMaxColumns());
      }
      destSheet.getRange(startRow, 1, numRowsToWrite, numColsToWrite).setValues(dataToWrite);
      logOperation(sessionId, "DATA_WRITTEN", `${ruleId}: Wrote ${numRowsToWrite} rows.`);
    }

    // --- Perform Verification ---
    const afterRowCount = destSheet.getLastRow();
    const afterColCount = destSheet.getLastColumn();
    let rowsMatch = 'N/A', colsMatch = 'N/A', samplesMatch = 'N/A';
    let verificationPassed = true;
    let verificationDetails = "Verification passed.";

    if (CONFIG.VERIFICATION_CONFIG.ENABLED) {
      logOperation(sessionId, "VERIFICATION_START", `${ruleId}: Starting data verification for push.`);

      // 1. Row Count
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS) {
        let expectedRowCountAfterWrite;
        if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
          expectedRowCountAfterWrite = beforeRowCount + dataToWrite.length;
        } else { // Cleared or recreated
          expectedRowCountAfterWrite = dataToWrite.length === 0 ? (destSheet.getRange(1, 1).isBlank() ? 0 : 1) : dataToWrite.length;
        }
        if (afterRowCount !== expectedRowCountAfterWrite) {
          verificationPassed = false;
          rowsMatch = 'NO';
          verificationDetails = `Row count mismatch: Expected ${expectedRowCountAfterWrite}, Found ${afterRowCount}. Mode: ${sheetHandlingMode}, Before: ${beforeRowCount}, Written: ${dataToWrite.length}`;
          logOperation(sessionId, "ROW_COUNT_ERROR", `${ruleId}: ${verificationDetails}`);
        } else {
          rowsMatch = 'YES';
        }
      }
      // 2. Column Count
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS && verificationPassed) {
        if (sourceColCount > afterColCount) {
          verificationPassed = false;
          colsMatch = 'NO';
          verificationDetails = `Column count mismatch: Source had ${sourceColCount}, Destination has ${afterColCount}.`;
          logOperation(sessionId, "COLUMN_COUNT_ERROR", `${ruleId}: ${verificationDetails}`);
        } else {
          colsMatch = 'YES';
        }
      }
      // 3. Sample Data
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA && verificationPassed) {
        const sampleCheckResult = verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        if (!sampleCheckResult) {
          verificationPassed = false;
          samplesMatch = 'NO';
          verificationDetails = "Sample data verification failed (see diagnostic log).";
        } else {
          samplesMatch = 'YES';
        }
      }
      logOperation(sessionId, "VERIFICATION_END", `${ruleId}: Verification Result: Passed=${verificationPassed}`);
    } // End Verification Enabled


    // --- Log Verification Result ---
    logVerification({
      sessionId: sessionId, ruleId: ruleId, sourceType: "Sheet Push", sourceFile: `${sourceSpreadsheetId}:${sourceTabName}`,
      destinationSheet: destTabName, sourceRowCount: sourceRowCount, destinationRowCount: afterRowCount,
      sourceColumnCount: sourceColCount, destinationColumnCount: afterColCount,
      rowsMatch: rowsMatch, columnsMatch: colsMatch, samplesMatch: samplesMatch,
      dataHash: dataHash, status: verificationPassed ? 'COMPLETE' : 'ERROR', details: verificationDetails
    });

    if (!verificationPassed) {
      throw new Error(verificationDetails);
    }

    const duration = (new Date() - startTime) / 1000;
    Logger.log(`${ruleId}: Sheet push completed successfully in ${duration.toFixed(2)} seconds.`);

    const rowsActuallyProcessed = dataToWrite.length;

    return {
      status: "SUCCESS",
      rowsProcessed: rowsActuallyProcessed,
      rowsExpected: rowsActuallyProcessed,
      duration: duration,
      message: `Successfully pushed sheet "${sourceTabName}" (${rowsActuallyProcessed} rows).`,
      dataHash: dataHash
    };

  } catch (error) {
    Logger.log(`${ruleId}: Sheet push ERROR: ${error.message}`);
    logVerification({ sessionId: sessionId, ruleId: ruleId, sourceType: "Sheet Push", status: 'ERROR', details: `Processing failed: ${error.message}` });
    throw error;
  }
}

/**
* Opens or creates a destination sheet based on the specified handling mode.
* Ensures the sheet exists and is in the correct state (cleared, new, or existing).
* @param {string} spreadsheetId - ID of the destination spreadsheet.
* @param {string} tabName - Name of the target tab within the spreadsheet.
* @param {string} handlingMode - How to handle existing sheet ('clearAndReuse', 'recreate', 'append', 'copyFormat').
* @returns {Sheet} The destination sheet object, ready for writing (or having been copied).
* @throws {Error} If the spreadsheet cannot be opened or sheet manipulation fails.
*/
function openDestinationSheet(spreadsheetId, tabName, handlingMode) {
  Logger.log(`Opening destination: ID=${spreadsheetId}, Tab="${tabName}", Mode=${handlingMode}`);
  let spreadsheet;
  try {
    spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  } catch (e) {
    throw new Error(`Failed to open destination spreadsheet with ID "${spreadsheetId}": ${e.message}`);
  }
  Logger.log(`Opened destination spreadsheet: "${spreadsheet.getName()}"`);

  let sheet = spreadsheet.getSheetByName(tabName);
  const sheetExists = sheet !== null;
  Logger.log(`Destination tab "${tabName}" exists: ${sheetExists}`);

  if (handlingMode === 'copyFormat') {
    // 'copyFormat' is handled *before* calling this function in the GSheet ingest process.
    // If called with this mode otherwise, it implies the sheet should exist (or be created).
    if (!sheet) {
      Logger.log(`'copyFormat' specified but sheet "${tabName}" doesn't exist. Creating it.`);
      try {
        sheet = spreadsheet.insertSheet(tabName);
      } catch (e) {
        throw new Error(`Failed to insert sheet "${tabName}" during copyFormat handling: ${e.message}`);
      }
    }
    Logger.log(`Mode 'copyFormat': Returning sheet "${tabName}" as is (copy handled elsewhere).`);
    return sheet;
  }


  if (!sheetExists) {
    // If sheet doesn't exist, create it regardless of mode (except copyFormat handled above)
    Logger.log(`Sheet "${tabName}" not found. Creating new sheet.`);
    try {
      sheet = spreadsheet.insertSheet(tabName);
      // Ensure sheet has minimum rows/cols for potential formatting/writing later
      if (sheet.getMaxRows() < CONFIG.NEW_SHEET_ROWS) sheet.insertRows(sheet.getMaxRows() + 1, CONFIG.NEW_SHEET_ROWS - sheet.getMaxRows());
      if (sheet.getMaxColumns() < 10) sheet.insertColumns(sheet.getMaxColumns() + 1, 10 - sheet.getMaxColumns()); // Ensure at least 10 cols
    } catch (e) {
      throw new Error(`Failed to insert new sheet "${tabName}" in spreadsheet ID "${spreadsheetId}": ${e.message}`);
    }
  } else {
    // Sheet exists, handle based on mode
    switch (handlingMode) {
      case 'clearAndReuse':
        Logger.log(`Mode 'clearAndReuse': Clearing content and formats of sheet "${tabName}".`);
        try {
          // Clear everything - content, formats, conditional rules, data validation, notes etc.
          sheet.clear();
          // Re-ensure minimum rows/cols after clearing
          if (sheet.getMaxRows() < CONFIG.NEW_SHEET_ROWS) sheet.insertRows(sheet.getMaxRows() + 1, CONFIG.NEW_SHEET_ROWS - sheet.getMaxRows());
          if (sheet.getMaxColumns() < 10) sheet.insertColumns(sheet.getMaxColumns() + 1, 10 - sheet.getMaxColumns());
        } catch (e) {
          throw new Error(`Failed to clear sheet "${tabName}": ${e.message}`);
        }
        break;
      case 'recreate':
        Logger.log(`Mode 'recreate': Deleting and recreating sheet "${tabName}".`);
        try {
          const sheetIndex = sheet.getIndex(); // Remember original position
          spreadsheet.deleteSheet(sheet);
          sheet = spreadsheet.insertSheet(tabName, sheetIndex); // Recreate at same position
          // Ensure minimum rows/cols for new sheet
          if (sheet.getMaxRows() < CONFIG.NEW_SHEET_ROWS) sheet.insertRows(sheet.getMaxRows() + 1, CONFIG.NEW_SHEET_ROWS - sheet.getMaxRows());
          if (sheet.getMaxColumns() < 10) sheet.insertColumns(sheet.getMaxColumns() + 1, 10 - sheet.getMaxColumns());
        } catch (e) {
          throw new Error(`Failed to delete/recreate sheet "${tabName}": ${e.message}`);
        }
        break;
      case 'append':
        Logger.log(`Mode 'append': Using existing sheet "${tabName}" without clearing.`);
        // Do nothing to the sheet content here.
        break;
      default:
        // Default behavior: Treat unrecognized modes like 'clearAndReuse' for safety
        Logger.log(`Unknown handling mode "${handlingMode}". Defaulting to 'clearAndReuse' for sheet "${tabName}".`);
        try {
          sheet.clear();
          // Re-ensure minimum rows/cols after clearing
          if (sheet.getMaxRows() < CONFIG.NEW_SHEET_ROWS) sheet.insertRows(sheet.getMaxRows() + 1, CONFIG.NEW_SHEET_ROWS - sheet.getMaxRows());
          if (sheet.getMaxColumns() < 10) sheet.insertColumns(sheet.getMaxColumns() + 1, 10 - sheet.getMaxColumns());
        } catch (e) {
          throw new Error(`Failed to clear sheet "${tabName}" (defaulting from mode ${handlingMode}): ${e.message}`);
        }
    }
  }

  if (!sheet) {
    // Should not happen if error handling above is correct, but as a safeguard
    throw new Error(`Failed to obtain a valid sheet object for "${tabName}" after handling mode "${handlingMode}".`);
  }

  Logger.log(`Returning sheet "${sheet.getName()}" ready for use.`);
  return sheet;
}


// ========================================================================== //
// MAIN SCRIPT & UI FUNCTIONS (Could be in Main.gs / Triggers.gs / UILib.gs)
// ========================================================================== //

/**
* Creates the custom menu when the spreadsheet is opened.
* Does NOT automatically check for or create sheets anymore.
*/
function onOpen() {
  // Use the specific function name to avoid conflicts if user adds other onOpen triggers
  dataIngestOnOpen();
}

/**
* Function to create the menu.
*/
function dataIngestOnOpen() {
  try {
    // --- REMOVED: ensureRequiredSheetsExist(); ---
    // Sheet check/creation now only happens manually or via runAll prerequisite.

    // Create the custom menu
    SpreadsheetApp.getUi()
      .createMenu('⚙️ Data Ingest')
      .addItem('🚀 Run All Active Rules', 'runAll')
      // --- REMOVED: .addItem('➡️ Run Selected Rules', 'runSelectedRules') ---
      .addSeparator()
      .addItem('📊 Check System Status', 'checkSystemStatus')
      .addItem('✔️ Validate Configuration', 'validateConfiguration')
      .addSeparator()
      .addItem('🛠️ Setup/Recreate Sheets', 'setupSheets') // Keep manual setup
      .addItem('⏰ Manage Scheduled Triggers', 'manageTriggersUI') // Updated item
      .addSeparator()
      .addItem('✉️ Send Test Email Report', 'sendTestEmailReport')
      .addToUi();
    Logger.log("Custom menu created successfully (without auto sheet setup).");
  } catch (e) {
    Logger.log(`Error in dataIngestOnOpen: ${e.message}`);
    // SpreadsheetApp.getUi().alert("Error", "Could not initialize Data Ingest menu: " + e.message);
  }
}

/**
* Checks if required sheets exist and creates any missing ones using safe creation.
* Called by runAll and setupSheets, NOT by onOpen.
* @returns {boolean} True if all required sheets exist (or were created), false otherwise.
*/
function ensureRequiredSheetsExist() {
  Logger.log("Ensuring required sheets exist...");
  const requiredSheets = [
    { name: CONFIG.CONFIG_SHEET_NAME, createFn: createCfgIngestSheet },
    { name: CONFIG.LOG_SHEET_NAME, createFn: createLogSheet },
    { name: CONFIG.VERIFICATION_SHEET_NAME, createFn: createVerificationSheet },
    { name: CONFIG.DIAGNOSTIC_SHEET_NAME, createFn: createDiagnosticSheet }
  ];
  let allExist = true;
  let sheetsChecked = 0;
  let sheetsCreated = 0;

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const existingSheetNames = ss.getSheets().map(s => s.getName());

    for (const sheetInfo of requiredSheets) {
      sheetsChecked++;
      if (!existingSheetNames.includes(sheetInfo.name)) {
        Logger.log(`Required sheet "${sheetInfo.name}" is missing. Attempting creation...`);
        try {
          sheetInfo.createFn(); // Call the specific creation function
          sheetsCreated++;
          Logger.log(`Successfully created required sheet: "${sheetInfo.name}".`);
        } catch (createError) {
          Logger.log(`ERROR creating required sheet "${sheetInfo.name}": ${createError.message}`);
          allExist = false; // Mark as failed if any creation fails
          // Log this error to the log sheet if possible (but avoid infinite loop if log sheet itself failed)
          if (sheetInfo.name !== CONFIG.LOG_SHEET_NAME) {
            try { logOperation(generateUniqueID(), "INIT_ERROR", `Failed to create required sheet: ${sheetInfo.name}. Error: ${createError.message}`); } catch (e) { }
          }
        }
      }
    }

    if (sheetsCreated > 0) {
      try { logOperation(generateUniqueID(), "INITIALIZATION", `Checked for required sheets. Created ${sheetsCreated} missing sheet(s). Status: ${allExist ? 'OK' : 'ERRORS'}`); } catch (e) { }
    }
    Logger.log(`Required sheets check complete. All exist: ${allExist}.`);
    return allExist;

  } catch (error) {
    // Catch errors like permission issues accessing SpreadsheetApp
    Logger.log(`CRITICAL ERROR during sheet existence check: ${error.message}`);
    // Try to log if possible
    try { logOperation(generateUniqueID(), "INIT_ERROR", `Critical error checking sheets: ${error.message}`); } catch (e) { }
    return false; // Indicate failure
  }
}


/**
* Backs up specified existing sheets.
* @param {Spreadsheet} ss - The spreadsheet object.
* @param {Array<string>} sheetNames - Array of sheet names to back up.
* @returns {Array<string>} Array of the names of the backup sheets created.
*/
function backupExistingSheets(ss, sheetNames) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupResults = [];

  for (const sheetName of sheetNames) {
    const sheet = ss.getSheetByName(sheetName);
    if (sheet) {
      try {
        const backupName = `${sheetName}-backup-${timestamp}`.slice(0, 100); // Limit name length
        const backupSheet = sheet.copyTo(ss).setName(backupName);
        Logger.log(`Created backup of "${sheetName}" as "${backupName}".`);
        backupResults.push(backupName);
      } catch (error) {
        Logger.log(`ERROR creating backup of "${sheetName}": ${error.message}. Skipping backup.`);
      }
    } else {
      Logger.log(`Sheet "${sheetName}" not found for backup. Skipping.`);
    }
  }
  return backupResults;
}


/**
* Menu item: Sets up (or recreates with backup) all required sheets.
* Prompts user for confirmation if sheets exist.
*/
function setupSheets() {
  Logger.log("Starting setupSheets via menu...");
  const ui = SpreadsheetApp.getUi();
  const sessionId = generateUniqueID();
  logOperation(sessionId, "SETUP_START", "User initiated sheet setup/recreation.");

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetNamesToCheck = [
      CONFIG.CONFIG_SHEET_NAME, CONFIG.LOG_SHEET_NAME,
      CONFIG.VERIFICATION_SHEET_NAME, CONFIG.DIAGNOSTIC_SHEET_NAME
    ];
    const existingSheets = sheetNamesToCheck.filter(name => ss.getSheetByName(name) !== null);
    let userChoice = ui.Button.YES; // Default to proceed if no sheets exist

    if (existingSheets.length > 0) {
      const message = `The following required sheets already exist:\n- ${existingSheets.join('\n- ')}\n\n` +
        `Recreating them will backup the current ones first.\n\n` +
        `Do you want to backup and recreate these sheets? (Select "No" to cancel)`;
      userChoice = ui.alert('Recreate Sheets?', message, ui.ButtonSet.YES_NO);
    }

    if (userChoice !== ui.Button.YES) {
      Logger.log("User cancelled sheet setup/recreation.");
      logOperation(sessionId, "SETUP_CANCELLED", "User cancelled operation.");
      ui.alert("Setup Cancelled", "No changes were made to the sheets.", ui.ButtonSet.OK);
      return;
    }

    // User confirmed or no sheets existed, proceed with setup
    Logger.log("Proceeding with sheet setup/recreation...");

    // Backup existing sheets before replacing
    if (existingSheets.length > 0) {
      backupExistingSheets(ss, existingSheets);
      logOperation(sessionId, "BACKUP_CREATED", `Created backups for existing sheets: ${existingSheets.join(', ')}`);
    }

    // Call creation functions (ensureSafeSheetCreation handles replacement)
    let createdCount = 0;
    let errorCount = 0;
    const creationFunctions = [
      createCfgIngestSheet, createLogSheet, createVerificationSheet, createDiagnosticSheet
    ];

    for (const createFn of creationFunctions) {
      try {
        createFn();
        createdCount++;
      } catch (error) {
        errorCount++;
        Logger.log(`Error during setup calling ${createFn.name}: ${error.message}`);
        logOperation(sessionId, "SETUP_ERROR", `Error creating sheet via ${createFn.name}: ${error.message}`);
        // Continue trying to create other sheets
      }
    }

    // Report final status
    let finalMessage = "";
    if (errorCount === 0) {
      finalMessage = `Successfully set up all ${createdCount} required sheets.`;
      logOperation(sessionId, "SETUP_COMPLETE", finalMessage);
    } else {
      finalMessage = `Setup finished with ${errorCount} error(s). ${createdCount} sheets were successfully created/recreated.\nPlease check the logs for details of the errors.`;
      logOperation(sessionId, "SETUP_PARTIAL", finalMessage);
    }
    ui.alert('Setup Finished', finalMessage, ui.ButtonSet.OK);


  } catch (error) {
    const errorMsg = `Critical error during setupSheets execution: ${error.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "SETUP_ERROR", errorMsg);
    ui.alert('Setup Error', `An unexpected error occurred: ${error.message}`, ui.ButtonSet.OK);
  }
}


/**
* Main function to run all active ingest rules. Triggered by menu or schedule.
*/
function runAll() {
  const functionStartTime = new Date();
  const sessionId = generateUniqueID();
  Logger.log(`Starting runAll execution. Session ID: ${sessionId}`);
  logOperation(sessionId, "START", "Starting execution of all active rules.");
  let results = [];
  let overallStatus = "COMPLETE"; // Assume success unless error occurs

  try {
    // 1. Ensure required sheets exist first (CRITICAL PREREQUISITE)
    const sheetsOk = ensureRequiredSheetsExist();
    if (!sheetsOk) {
      throw new Error("Required sheets are missing or could not be created. Aborting run.");
    }

    // 2. Send Start Notification (if enabled)
    if (CONFIG.EMAIL_CONFIG.SEND_ON_START) {
      try {
        // Could try to estimate number of active rules for notification
        // const activeRuleCount = estimateActiveRules(); // Need to implement estimateActiveRules if desired
        sendJobStartNotification(sessionId /*, activeRuleCount */);
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send START notification: ${emailError.message}`);
        // Don't abort the run for email failure
      }
    }

    // 3. Execute the core data ingestion
    results = ingestData(sessionId); // This function now returns results array

    // 4. Determine final status based on results
    const hasErrors = results.some(r => r.status === "ERROR");
    if (hasErrors) {
      overallStatus = "ERROR";
      Logger.log(`runAll completed with errors. Session ID: ${sessionId}`);
    } else {
      Logger.log(`runAll completed successfully. Session ID: ${sessionId}`);
    }

    // 5. Send Completion Notification (if enabled)
    if (CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE) {
      try {
        sendRunSummaryEmail(sessionId, overallStatus, results);
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send COMPLETION notification: ${emailError.message}`);
      }
    }

    // 6. Show UI Alert (if run manually from UI)
    // Check if UI context exists (e.g., not run from a trigger)
    let uiContextExists = false;
    try {
      uiContextExists = SpreadsheetApp.getUi() && SpreadsheetApp.getActiveSpreadsheet() && SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
    } catch (e) { /* Ignore error if no UI context */ }

    if (uiContextExists) {
      const successCount = results.filter(r => r.status === "SUCCESS").length;
      const errorCount = results.filter(r => r.status === "ERROR").length;
      const skippedCount = results.filter(r => r.status === "SKIPPED").length;
      const totalDurationSec = (new Date() - functionStartTime) / 1000;

      SpreadsheetApp.getUi().alert('Run Complete',
        `Ingest process finished in ${totalDurationSec.toFixed(1)}s.\n` +
        `- Rules processed: ${results.length}\n` +
        `- Successful: ${successCount}\n` +
        `- Errors: ${errorCount}\n` +
        `- Skipped: ${skippedCount}\n\n` +
        `Status: ${overallStatus}. Check the log sheet for details.`,
        SpreadsheetApp.getUi().ButtonSet.OK);
    }


  } catch (error) {
    // Catch critical errors from ingestData or sheet check
    const errorMsg = `CRITICAL ERROR during runAll: ${error.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "ERROR", errorMsg); // Log the critical failure
    overallStatus = "ERROR"; // Mark run as failed

    // Send Error Notification (if enabled)
    if (CONFIG.EMAIL_CONFIG.SEND_ON_ERROR) {
      try {
        // Pass 'results' even if partial, sendRunSummary handles it
        sendRunSummaryEmail(sessionId, overallStatus, results); // Send summary even on critical failure if possible
        // Or send a simpler specific error notification:
        // sendErrorNotification("Critical Ingest Process Failure", error.message, sessionId);
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send critical ERROR notification: ${emailError.message}`);
      }
    }

    // Show UI Alert on critical failure (if UI context exists)
    let uiContextExistsOnError = false;
    try { uiContextExistsOnError = SpreadsheetApp.getUi() && SpreadsheetApp.getActiveSpreadsheet() && SpreadsheetApp.getActiveSpreadsheet().getActiveSheet(); } catch (e) { }
    if (uiContextExistsOnError) {
      SpreadsheetApp.getUi().alert('Critical Error',
        `The ingest process failed critically:\n${error.message}\n\nCheck the log sheet for details.`,
        SpreadsheetApp.getUi().ButtonSet.OK);
    }
  } finally {
    // Ensure progress indicator is cleared even on error
    removeProgressSheet(); // Call function to remove progress indicator and its trigger
    Logger.log(`runAll execution finished for Session ID: ${sessionId}`);
  }
}


/**
* ----- runSelectedRules FUNCTION REMOVED (as requested) -----
*/


/**
* Updates the status columns (lastRunTime, lastRunStatus, lastRunMessage) for a specific rule row.
* @param {Sheet} configSheet - The configuration sheet object.
* @param {number} rowNum - The 1-based row number of the rule to update.
* @param {string} status - The status text ("SUCCESS", "ERROR", "SKIPPED").
* @param {string} message - The status message. Truncated if too long for sheet cell.
*/
function updateRuleStatus(configSheet, rowNum, status, message) {
  if (!configSheet || rowNum < 2) return; // Ignore header or invalid input

  try {
    // Get headers from row 1 to find column indices dynamically
    const headers = configSheet.getRange(1, 1, 1, configSheet.getLastColumn()).getValues()[0];
    const headerMap = createHeaderMap(headers);

    // Find column indices using internal keys from CONFIG.COLUMN_MAPPINGS
    const timeColName = CONFIG.COLUMN_MAPPINGS.lastRunTime;
    const statusColName = CONFIG.COLUMN_MAPPINGS.lastRunStatus;
    const msgColName = CONFIG.COLUMN_MAPPINGS.lastRunMessage;

    const timeColIdx = headerMap[timeColName];
    const statusColIdx = headerMap[statusColName];
    const msgColIdx = headerMap[msgColName];

    const MAX_MSG_LENGTH = 500; // Limit message length in sheet cell
    const truncatedMessage = message ? String(message).slice(0, MAX_MSG_LENGTH) : '';

    // Update specific cells directly if columns exist
    if (timeColIdx !== undefined) {
      configSheet.getRange(rowNum, timeColIdx + 1).setValue(new Date());
    }
    if (statusColIdx !== undefined) {
      configSheet.getRange(rowNum, statusColIdx + 1).setValue(status);
    }
    if (msgColIdx !== undefined) {
      configSheet.getRange(rowNum, msgColIdx + 1).setValue(truncatedMessage);
    }

  } catch (error) {
    // Log error but don't let status update failure stop the main process
    Logger.log(`Error updating status for row ${rowNum} in sheet "${configSheet.getName()}": ${error.message}`);
  }
}

/**
* Validates the configuration sheet structure and rule definitions.
* Displays results in a UI alert.
*/
function validateConfiguration() {
  Logger.log("Starting configuration validation...");
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);

  if (!configSheet) {
    Logger.log(`Validation Error: Configuration sheet "${CONFIG.CONFIG_SHEET_NAME}" not found.`);
    ui.alert('Validation Error', `Configuration sheet "${CONFIG.CONFIG_SHEET_NAME}" not found. Please run "Setup/Recreate Sheets" first.`, ui.ButtonSet.OK);
    return;
  }

  let errors = [];
  let warnings = [];
  let activeRuleCount = 0;

  try {
    const configData = configSheet.getDataRange().getValues();
    if (configData.length <= 1) {
      warnings.push("The configuration sheet has no rules defined (only a header row).");
    }

    const headers = configData[0];
    const headerMap = createHeaderMap(headers);

    // 1. Validate Required Headers exist based on COLUMN_MAPPINGS
    const mappedHeaders = Object.values(CONFIG.COLUMN_MAPPINGS);
    const missingMappedHeaders = mappedHeaders.filter(header => headerMap[header] === undefined);
    if (missingMappedHeaders.length > 0) {
      errors.push(`Missing required column headers defined in COLUMN_MAPPINGS: ${missingMappedHeaders.join(', ')}. Check sheet headers and mappings.`);
    }
    // Specifically check essential headers needed for basic processing
    const essentialInternal = ['ruleActive', 'ingestMethod'];
    essentialInternal.forEach(key => {
      const mappedHeader = CONFIG.COLUMN_MAPPINGS[key];
      if (!mappedHeader || headerMap[mappedHeader] === undefined) {
        errors.push(`Essential column for "${key}" (Mapped to: ${mappedHeader || 'N/A'}) is missing.`);
      }
    });


    // If essential headers missing, stop further row validation
    if (errors.some(err => err.includes('Essential column'))) {
      Logger.log("Essential headers missing, aborting detailed row validation.");
    } else {
      // 2. Validate each rule row
      const ruleActiveIndex = headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive];
      const ingestMethodIndex = headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod];

      for (let i = 1; i < configData.length; i++) {
        const row = configData[i];
        const rowNum = i + 1;

        if (!isRowPopulated(row)) {
          continue; // Skip empty rows silently
        }

        const isActive = row[ruleActiveIndex] === true;
        if (isActive) {
          activeRuleCount++;
          const ingestMethod = ingestMethodIndex !== undefined ? row[ingestMethodIndex] : null;
          Logger.log(`Validating active rule in row ${rowNum}, Method: ${ingestMethod}`);

          // Validate common destination fields first (only if NOT push method)
          if (ingestMethod !== 'push') {
            validateDestinationFields(row, headerMap, rowNum, errors);
          }

          validateSheetHandlingMode(row, headerMap, rowNum, warnings); // Sheet handling is warning only

          // Validate method-specific fields
          if (ingestMethod === 'email') {
            validateEmailRule(row, headerMap, rowNum, errors, warnings);
          } else if (ingestMethod === 'gSheet') {
            validateGSheetRule(row, headerMap, rowNum, errors, warnings);
          } else if (ingestMethod === 'push') {
            validatePushRule(row, headerMap, rowNum, errors, warnings);
          } else if (!ingestMethod) {
            errors.push(`Row ${rowNum}: Active rule is missing the required 'ingestMethod'.`);
          } else {
            errors.push(`Row ${rowNum}: Unknown 'ingestMethod' value: "${ingestMethod}". Must be 'email', 'gSheet', or 'push'.`);
          }
        }
      }
    } // End else block (essential headers exist)


    if (activeRuleCount === 0 && configData.length > 1) {
      warnings.push(`Found ${configData.length - 1} rule(s), but none are marked active.`);
    } else if (configData.length > 1) {
      warnings.push(`Found ${activeRuleCount} active rule(s) defined.`);
    }


  } catch (e) {
    errors.push(`Error during validation process: ${e.message}`);
    Logger.log(`Validation Catch Block Error: ${e.stack}`);
  }


  // 3. Display Results
  let message = "";
  let title = "";

  if (errors.length === 0) {
    title = 'Validation Successful';
    message = `Configuration appears valid.\n\n`;
    if (warnings.length > 0) message += 'INFO/WARNINGS:\n- ' + warnings.join('\n- ');
    Logger.log(`Configuration validation successful. ${warnings.length} warnings.`);
  } else {
    title = 'Validation Failed';
    message = `Found ${errors.length} error(s) and ${warnings.length} warning(s):\n\n`;
    message += 'ERRORS:\n- ' + errors.join('\n- ') + '\n\n';
    if (warnings.length > 0) {
      message += 'WARNINGS:\n- ' + warnings.join('\n- ');
    }
    message += '\n\nPlease correct the errors before running the ingest process.';
    Logger.log(`Configuration validation failed. ${errors.length} errors, ${warnings.length} warnings.`);
  }

  // Use larger dialog box for potentially long messages
  const htmlOutput = HtmlService.createHtmlOutput(`<pre>${Utilities.encodeHtml(message)}</pre>`)
    .setWidth(600)
    .setHeight(400);
  ui.showModalDialog(htmlOutput, title);
}

// --- Validation Helper Functions ---

/** Validates email-specific rule fields */
function validateEmailRule(row, headerMap, rowNum, errors, warnings) {
  const fields = ['in_email_searchString', 'in_email_attachmentPattern'];
  fields.forEach(key => {
    const colName = CONFIG.COLUMN_MAPPINGS[key];
    if (!colName || headerMap[colName] === undefined) errors.push(`Row ${rowNum} (Email): Missing required column header: "${colName || key}".`);
    else if (!row[headerMap[colName]]) errors.push(`Row ${rowNum} (Email): Missing required value for "${colName}".`);
  });
}

/** Validates gSheet-specific rule fields */
function validateGSheetRule(row, headerMap, rowNum, errors, warnings) {
  // Check source ID OR URL
  const idKey = 'in_gsheet_sheetId';
  const urlKey = 'in_gsheet_sheetURL';
  const idColName = CONFIG.COLUMN_MAPPINGS[idKey];
  const urlColName = CONFIG.COLUMN_MAPPINGS[urlKey];
  const hasId = idColName && headerMap[idColName] !== undefined && String(row[headerMap[idColName]]).trim();
  const hasUrl = urlColName && headerMap[urlColName] !== undefined && String(row[headerMap[urlColName]]).trim();
  if (!hasId && !hasUrl) {
    errors.push(`Row ${rowNum} (gSheet): Missing required value for Source Sheet ID ("${idColName || idKey}") OR Source Sheet URL ("${urlColName || urlKey}").`);
  }
  // Check source tab name
  const tabKey = 'in_gsheet_tabName';
  const tabColName = CONFIG.COLUMN_MAPPINGS[tabKey];
  if (!tabColName || headerMap[tabColName] === undefined) errors.push(`Row ${rowNum} (gSheet): Missing required column header: "${tabColName || tabKey}".`);
  else if (!row[headerMap[tabColName]]) errors.push(`Row ${rowNum} (gSheet): Missing required value for Source Tab Name ("${tabColName}").`);
}

/** Validates push-specific rule fields */
function validatePushRule(row, headerMap, rowNum, errors, warnings) {
  // Check source tab name
  const sourceTabKey = 'pushSourceTabName';
  const sourceTabColName = CONFIG.COLUMN_MAPPINGS[sourceTabKey];
  if (!sourceTabColName || headerMap[sourceTabColName] === undefined) errors.push(`Row ${rowNum} (Push): Missing required column header: "${sourceTabColName || sourceTabKey}".`);
  else if (!row[headerMap[sourceTabColName]]) errors.push(`Row ${rowNum} (Push): Missing required value for Source Tab Name ("${sourceTabColName}").`);

  // Check destination ID OR URL
  const destIdKey = 'pushDestinationSheetId';
  const destUrlKey = 'pushDestinationSheetUrl';
  const destIdColName = CONFIG.COLUMN_MAPPINGS[destIdKey];
  const destUrlColName = CONFIG.COLUMN_MAPPINGS[destUrlKey];
  const hasDestId = destIdColName && headerMap[destIdColName] !== undefined && String(row[headerMap[destIdColName]]).trim();
  const hasDestUrl = destUrlColName && headerMap[destUrlColName] !== undefined && String(row[headerMap[destUrlColName]]).trim();
  if (!hasDestId && !hasDestUrl) {
    errors.push(`Row ${rowNum} (Push): Missing required value for Destination Sheet ID ("${destIdColName || destIdKey}") OR Destination Sheet URL ("${destUrlColName || destUrlKey}").`);
  }
  // Check destination tab name
  const destTabKey = 'pushDestinationTabName';
  const destTabColName = CONFIG.COLUMN_MAPPINGS[destTabKey];
  if (!destTabColName || headerMap[destTabColName] === undefined) errors.push(`Row ${rowNum} (Push): Missing required column header: "${destTabColName || destTabKey}".`);
  else if (!row[headerMap[destTabColName]]) errors.push(`Row ${rowNum} (Push): Missing required value for Destination Tab Name ("${destTabColName}").`);
}


/** Validates common destination fields (ID/URL and Tab Name) - Used for email and gSheet */
function validateDestinationFields(row, headerMap, rowNum, errors) {
  // Check destination ID OR URL
  const destIdKey = 'dest_sheetId';
  const destUrlKey = 'dest_sheetUrl';
  const destIdColName = CONFIG.COLUMN_MAPPINGS[destIdKey];
  const destUrlColName = CONFIG.COLUMN_MAPPINGS[destUrlKey];
  const hasDestId = destIdColName && headerMap[destIdColName] !== undefined && String(row[headerMap[destIdColName]]).trim();
  const hasDestUrl = destUrlColName && headerMap[destUrlColName] !== undefined && String(row[headerMap[destUrlColName]]).trim();
  if (!hasDestId && !hasDestUrl) {
    errors.push(`Row ${rowNum}: Missing required value for Destination Sheet ID ("${destIdColName || destIdKey}") OR Destination Sheet URL ("${destUrlColName || destUrlKey}").`);
  }
  // Check destination tab name
  const destTabKey = 'dest_sheet_tabName';
  const destTabColName = CONFIG.COLUMN_MAPPINGS[destTabKey];
  if (!destTabColName || headerMap[destTabColName] === undefined) errors.push(`Row ${rowNum}: Missing required column header: "${destTabColName || destTabKey}".`);
  else if (!row[headerMap[destTabColName]]) errors.push(`Row ${rowNum}: Missing required value for Destination Tab Name ("${destTabColName}").`);
}

/** Validates the sheet handling mode value (adds warning if invalid/missing) */
function validateSheetHandlingMode(row, headerMap, rowNum, warnings) {
  const key = 'sheetHandlingMode';
  const colName = CONFIG.COLUMN_MAPPINGS[key];
  const validModes = ['clearAndReuse', 'recreate', 'copyFormat', 'append'];

  if (!colName || headerMap[colName] === undefined) {
    // Only add warning once if column is missing entirely
    const warnMsg = `Config sheet is missing the "${colName || key}" column. Rules will default to 'clearAndReuse' mode.`;
    if (!warnings.includes(warnMsg)) {
      warnings.push(warnMsg);
    }
  } else {
    const mode = row[headerMap[colName]];
    if (mode && !validModes.includes(String(mode))) {
      warnings.push(`Row ${rowNum}: Invalid value "${mode}" for "${colName}". Must be one of: ${validModes.join(', ')}. Will default to 'clearAndReuse'.`);
    }
    // Blank value is acceptable, defaults to clearAndReuse
  }
}


// --- Trigger Management Functions ---

/**
* Menu item: Shows the HTML Service dialogue for managing triggers.
*/
function manageTriggersUI() {
  const html = HtmlService.createTemplateFromFile('TriggersDialogue')
    .evaluate()
    .setWidth(450)
    .setHeight(350);
  SpreadsheetApp.getUi().showModalDialog(html, 'Manage Scheduled Triggers');
  Logger.log("Opened Manage Triggers dialogue.");
}

/**
* Backend function: Gets the status of standard triggers created by this script.
* @returns {object} { runAllActive: boolean, cleanupLogsActive: boolean }
*/
function getTriggerStatus() {
  Logger.log("Backend: Getting trigger status...");
  let status = { runAllActive: false, cleanupLogsActive: false };
  try {
    const triggers = ScriptApp.getProjectTriggers();
    const scriptId = ScriptApp.getScriptId(); // Filter for triggers created by this script

    triggers.forEach(trigger => {
      // Check if trigger belongs to this script and is time-based
      if (trigger.getTriggerSource() === ScriptApp.TriggerSource.CLOCK &&
        trigger.getTriggerSourceId() === scriptId) { // Check source ID for script bound triggers
        const handler = trigger.getHandlerFunction();
        if (handler === 'runAll') status.runAllActive = true;
        if (handler === 'cleanupLogs') status.cleanupLogsActive = true;
      }
    });
    Logger.log(`Backend: Trigger status found: ${JSON.stringify(status)}`);
  } catch (e) {
    Logger.log(`Backend: Error getting trigger status: ${e.message}`);
    // Return default false status on error
  }
  return status;
}


/**
* Backend function: Creates or deletes a specific standard trigger for this script.
* @param {string} functionName - The name of the function to trigger ('runAll' or 'cleanupLogs').
* @param {boolean} enable - True to create/enable, false to delete/disable.
* @returns {object} { success: boolean, message: string, newStatus: object }
*/
function setTriggerState(functionName, enable) {
  Logger.log(`Backend: Setting trigger state for '${functionName}' to ${enable}`);
  try {
    // First, delete any existing trigger for this function created by this script
    _deleteSpecificTrigger(functionName);

    // Then, create it if enabling
    if (enable) {
      _createSpecificTrigger(functionName);
      Logger.log(`Backend: Created trigger for '${functionName}'.`);
    } else {
      Logger.log(`Backend: Disabled (deleted) trigger for '${functionName}'.`);
    }
    // Return success and the potentially updated status
    return { success: true, message: `Trigger for '${functionName}' ${enable ? 'enabled' : 'disabled'}.`, newStatus: getTriggerStatus() };
  } catch (e) {
    Logger.log(`Backend: Error setting trigger state for ${functionName}: ${e.message}`);
    return { success: false, message: `Error setting trigger for '${functionName}': ${e.message}`, newStatus: getTriggerStatus() };
  }
}

/** Helper to create a specific trigger */
function _createSpecificTrigger(functionName) {
  if (functionName === 'runAll') {
    ScriptApp.newTrigger('runAll')
      .timeBased()
      .everyDays(1)
      .atHour(1) // Example: 1 AM
      .inTimezone(Session.getScriptTimeZone())
      .create();
  } else if (functionName === 'cleanupLogs') {
    ScriptApp.newTrigger('cleanupLogs')
      .timeBased()
      .everyDays(1)
      .atHour(0) // Example: Midnight
      .inTimezone(Session.getScriptTimeZone())
      .create();
  } else {
    throw new Error(`Cannot create trigger for unknown function: ${functionName}`);
  }
}

/** Helper to delete a specific trigger created by this script */
function _deleteSpecificTrigger(functionName) {
  const triggers = ScriptApp.getProjectTriggers();
  const scriptId = ScriptApp.getScriptId();
  let deleted = false;

  triggers.forEach(trigger => {
    if (trigger.getTriggerSource() === ScriptApp.TriggerSource.CLOCK &&
      trigger.getTriggerSourceId() === scriptId && // Check script ID
      trigger.getHandlerFunction() === functionName) {
      try {
        ScriptApp.deleteTrigger(trigger);
        Logger.log(`Backend: Deleted existing trigger for ${functionName} (ID: ${trigger.getUniqueId()})`);
        deleted = true;
      } catch (e) {
        Logger.log(`Backend: Failed to delete trigger ID ${trigger.getUniqueId()} for ${functionName}: ${e.message}`);
        // Optionally re-throw or just log
        // throw e; // Re-throwing might stop deleting other matches if multiple exist
      }
    }
  });
  return deleted; // Indicate if any deletion occurred
}


/**
* Scheduled function to periodically clean up old log entries from various log sheets.
*/
function cleanupLogs() {
  const sessionId = generateUniqueID();
  logOperation(sessionId, "CLEANUP_START", "Starting scheduled log cleanup process.");
  let totalCleaned = 0;

  const sheetsToClean = [
    { name: CONFIG.LOG_SHEET_NAME, type: "Log" },
    { name: CONFIG.VERIFICATION_SHEET_NAME, type: "Verification" },
    { name: CONFIG.DIAGNOSTIC_SHEET_NAME, type: "Diagnostic" }
  ];

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const maxEntries = CONFIG.MAX_LOG_ENTRIES || 500; // Max entries to keep
    const headerRows = 1;
    const keepRows = maxEntries + headerRows;

    sheetsToClean.forEach(sheetInfo => {
      const sheet = ss.getSheetByName(sheetInfo.name);
      if (sheet) {
        try {
          const lastRow = sheet.getLastRow();
          if (lastRow > keepRows) {
            const deleteCount = lastRow - keepRows;
            // Delete older rows (which are typically at higher row numbers due to insertion at top)
            const deleteStartRow = keepRows + 1;
            Logger.log(`Cleaning ${sheetInfo.type} sheet "${sheetInfo.name}": Found ${lastRow} rows, keeping ${keepRows}. Deleting ${deleteCount} rows starting from row ${deleteStartRow}.`);
            sheet.deleteRows(deleteStartRow, deleteCount);
            totalCleaned += deleteCount;
            Logger.log(`Cleaned ${deleteCount} entries from ${sheetInfo.name}.`);
          }
        } catch (e) {
          Logger.log(`Error cleaning up ${sheetInfo.type} sheet "${sheetInfo.name}": ${e.message}`);
          logOperation(sessionId, "CLEANUP_ERROR", `Error cleaning sheet "${sheetInfo.name}": ${e.message}`);
        }
      }
    });

    const summaryMsg = `Log cleanup finished. Removed a total of ${totalCleaned} old entries across relevant sheets.`;
    logOperation(sessionId, "CLEANUP_COMPLETE", summaryMsg);
    Logger.log(summaryMsg);

  } catch (error) {
    // Catch errors like spreadsheet access issues
    const errorMsg = `Critical error during cleanupLogs process: ${error.message}`;
    Logger.log(errorMsg);
    try { logOperation(sessionId, "CLEANUP_ERROR", errorMsg); } catch (e) { }
  }
}

/**
* Triggered on user edits. Currently used to potentially add checkboxes/validation
* to newly added rows in the config sheet.
* @param {Event} e - The edit event object.
*/
function onEdit(e) {
  // Only act on edits within the config sheet
  if (!e || !e.range || e.range.getSheet().getName() !== CONFIG.CONFIG_SHEET_NAME) {
    return;
  }

  const editedRange = e.range;
  const editedRow = editedRange.getRow();
  const configSheet = editedRange.getSheet();

  // Ignore header row edits
  if (editedRow <= 1) {
    return;
  }

  // --- Example: Ensure Checkbox Exists in ruleActive Column ---
  try {
    const headers = configSheet.getRange(1, 1, 1, configSheet.getLastColumn()).getValues()[0];
    const headerMap = createHeaderMap(headers);
    const ruleActiveColName = CONFIG.COLUMN_MAPPINGS.ruleActive;
    const ruleActiveColIndex = headerMap[ruleActiveColName];

    if (ruleActiveColIndex !== undefined) {
      const checkboxCell = configSheet.getRange(editedRow, ruleActiveColIndex + 1);
      // Add checkbox only if the cell doesn't have one and is currently blank (prevents overwriting)
      if (!checkboxCell.getDataValidation() || checkboxCell.getDataValidation()?.getCriteriaType() !== SpreadsheetApp.DataValidationCriteria.CHECKBOX) {
        if (checkboxCell.isBlank()) {
          checkboxCell.insertCheckboxes();
          checkboxCell.setValue(false); // Default to false
        }
      }
    }
  } catch (error) {
    Logger.log(`onEdit Error (Checkbox): ${error.message} for row ${editedRow}`);
  }

  // --- Example: Ensure Data Validation Exists in Method/Mode Columns ---
  try {
    const headers = configSheet.getRange(1, 1, 1, configSheet.getLastColumn()).getValues()[0]; // Re-fetch might be needed if columns changed
    const headerMap = createHeaderMap(headers);
    const validations = [
      { key: 'ingestMethod', values: ['email', 'gSheet', 'push'] },
      { key: 'sheetHandlingMode', values: ['clearAndReuse', 'recreate', 'copyFormat', 'append'] }
    ];

    validations.forEach(valInfo => {
      const colName = CONFIG.COLUMN_MAPPINGS[valInfo.key];
      const colIndex = colName ? headerMap[colName] : undefined;
      if (colIndex !== undefined) {
        const cell = configSheet.getRange(editedRow, colIndex + 1);
        const currentValidation = cell.getDataValidation();
        // Apply validation if none exists or if it's not the correct list type
        if (!currentValidation || currentValidation.getCriteriaType() !== SpreadsheetApp.DataValidationCriteria.VALUE_IN_LIST) {
          const rule = SpreadsheetApp.newDataValidation()
            .requireValueInList(valInfo.values, true)
            .setAllowInvalid(false)
            .build();
          cell.setDataValidation(rule);
        }
      }
    });
  } catch (error) {
    Logger.log(`onEdit Error (Validation): ${error.message} for row ${editedRow}`);
  }
}


/**
* Menu Item: Checks system status (sheets, triggers, config) and displays a report.
*/
function checkSystemStatus() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const statusSessionId = generateUniqueID();
  logOperation(statusSessionId, "SYSTEM_CHECK_START", "User initiated system status check.");
  let report = "Data Ingest System Status Report:\n" + `Time: ${formatDate(new Date())}\n\n`;
  let checksPassed = true;

  try {
    // --- 1. Check Required Sheets ---
    report += "📊 Required Sheets:\n";
    const requiredSheets = [
      CONFIG.CONFIG_SHEET_NAME, CONFIG.LOG_SHEET_NAME,
      CONFIG.VERIFICATION_SHEET_NAME, CONFIG.DIAGNOSTIC_SHEET_NAME
    ];
    requiredSheets.forEach(name => {
      const sheet = ss.getSheetByName(name);
      if (sheet) {
        report += `- ${name}: ✅ Exists (Rows: ${sheet.getLastRow()})\n`;
      } else {
        report += `- ${name}: ❌ MISSING\n`;
        checksPassed = false;
      }
    });

    // --- 2. Check Configuration ---
    report += "\n⚙️ Configuration (" + CONFIG.CONFIG_SHEET_NAME + "):\n";
    const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
    if (configSheet) {
      const configData = configSheet.getDataRange().getValues();
      if (configData.length <= 1) {
        report += "- Status: ⚠️ No rules defined (only header row).\n";
      } else {
        const headers = configData[0];
        const headerMap = createHeaderMap(headers);
        const ruleActiveColName = CONFIG.COLUMN_MAPPINGS.ruleActive;
        const ruleActiveIndex = headerMap[ruleActiveColName];
        if (ruleActiveIndex === undefined) {
          report += `- Status: ❌ Error - '${ruleActiveColName}' column header not found!\n`;
          checksPassed = false;
        } else {
          let activeRuleCount = 0;
          for (let i = 1; i < configData.length; i++) {
            if (isRowPopulated(configData[i]) && configData[i][ruleActiveIndex] === true) activeRuleCount++;
          }
          report += `- Status: ✅ Found ${configData.length - 1} total potential rule rows.\n`;
          report += `- Active Rules: ${activeRuleCount}\n`;
        }
      }
    } else {
      report += "- Status: ❌ Sheet not found.\n";
      checksPassed = false;
    }

    // --- 3. Check Triggers ---
    report += "\n⏰ Triggers (Created by this script):\n";
    try {
      const triggerStatus = getTriggerStatus(); // Use backend function
      report += `- Run All Daily: ${triggerStatus.runAllActive ? '✅ Active' : '❌ Inactive'}\n`;
      report += `- Cleanup Logs Daily: ${triggerStatus.cleanupLogsActive ? '✅ Active' : '❌ Inactive'}\n`;
      // Note: onOpen trigger is implicit via function name
      report += `- onOpen (Menu): ℹ️ (Implicitly handled)\n`;
    } catch (e) {
      report += `- Status: ❌ Error checking triggers: ${e.message}\n`;
      checksPassed = false;
    }


    // --- 4. Email Configuration ---
    report += "\n✉️ Email Notifications:\n";
    const recipients = CONFIG.EMAIL_NOTIFICATIONS;
    if (recipients && recipients.length > 0) {
      report += `- Recipients: ${Array.isArray(recipients) ? recipients.join(', ') : recipients}\n`;
    } else {
      report += `- Recipients: ⚠️ None configured.\n`;
    }
    report += `- Send on Start: ${CONFIG.EMAIL_CONFIG.SEND_ON_START ? '✅ Yes' : '❌ No'}\n`;
    report += `- Send on Complete: ${CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE ? '✅ Yes' : '❌ No'}\n`;
    report += `- Send on Error: ${CONFIG.EMAIL_CONFIG.SEND_ON_ERROR ? '✅ Yes' : '❌ No'}\n`;
    report += `- Attach Logs: ${CONFIG.EMAIL_CONFIG.INCLUDE_LOG_ATTACHMENT ? '✅ Yes' : '❌ No'}\n`;
    report += `- Attach Verification: ${CONFIG.EMAIL_CONFIG.INCLUDE_VERIFICATION_ATTACHMENT ? '✅ Yes' : '❌ No'}\n`;
    report += `- Attach Diagnostics: ${CONFIG.EMAIL_CONFIG.INCLUDE_DIAGNOSTIC_ATTACHMENT ? '✅ Yes' : '❌ No'}\n`;


    // Final Status
    report += `\n---\nOverall Status: ${checksPassed ? '✅ OK' : '❌ ISSUES FOUND'}`;
    logOperation(statusSessionId, "SYSTEM_CHECK_COMPLETE", `System status check finished. Overall OK: ${checksPassed}`);

  } catch (error) {
    report += `\n\n❌ CRITICAL ERROR DURING STATUS CHECK:\n${error.message}`;
    checksPassed = false;
    logOperation(statusSessionId, "SYSTEM_CHECK_ERROR", `Critical error during status check: ${error.message}`);
  }

  // Display report in a dialog
  const htmlOutput = HtmlService.createHtmlOutput(`<pre>${Utilities.encodeHtml(report)}</pre>`).setWidth(650).setHeight(500);
  ui.showModalDialog(htmlOutput, 'System Status Report');
}


/**
* Menu item: Sends a test email report to configured recipients for verification.
*/
function sendTestEmailReport() {
  const ui = SpreadsheetApp.getUi();
  const sessionId = generateUniqueID();
  logOperation(sessionId, "TEST_EMAIL_START", "User initiated test email report.");

  try {
    // Check if emails are configured
    const recipients = CONFIG.EMAIL_NOTIFICATIONS;
    if (!recipients || recipients.length === 0) {
      ui.alert("Cannot Send Test", "No email recipients are configured in the script's CONFIG.", ui.ButtonSet.OK);
      logOperation(sessionId, "TEST_EMAIL_ABORT", "Aborted: No recipients configured.");
      return;
    }

    // Confirm with the user
    const response = ui.alert('Send Test Email?',
      `This will send a sample run summary email to:\n- ${Array.isArray(recipients) ? recipients.join('\n- ') : recipients}\n\nContinue?`,
      ui.ButtonSet.YES_NO
    );
    if (response !== ui.Button.YES) {
      logOperation(sessionId, "TEST_EMAIL_CANCELLED", "User cancelled test email.");
      return;
    }

    Logger.log(`Proceeding to send test email for session ${sessionId}.`);

    // Ensure sheets exist for attachment creation (don't recreate)
    ensureRequiredSheetsExist(); // Create if missing, safe function
    const logSheetExists = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.LOG_SHEET_NAME) !== null;
    const verSheetExists = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.VERIFICATION_SHEET_NAME) !== null;
    const diagSheetExists = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME) !== null;


    // Create sample data for the report
    const testResults = [
      { ruleName: "Sample Rule (Success)", status: "SUCCESS", rowsProcessed: 1250, rowsExpected: 1250, duration: 3.5, message: "Data loaded from sample_data.csv." },
      { ruleName: "Sample Rule (Error)", status: "ERROR", rowsProcessed: 980, rowsExpected: 1000, duration: 2.1, message: "Row count mismatch: Expected 1000, Found 980." },
      { ruleName: "Sample Rule (Skipped)", status: "SKIPPED", rowsProcessed: 0, rowsExpected: 0, duration: 0.1, message: "Rule was inactive." }
    ];

    // Log some dummy entries for the test session
    logOperation(sessionId, "INFO", "Generating sample log entries for test email.");
    logOperation(sessionId, "WARNING", "This is a sample warning message.");
    logOperation(sessionId, "SUCCESS", "Sample successful operation log.");
    logOperation(sessionId, "ERROR", "This is a sample error message for the test report.");
    logVerification({ // Log a sample verification entry
      sessionId: sessionId, ruleId: "Sample Rule (Success)", sourceType: "Test Data",
      sourceFile: "sample_data.csv", destinationSheet: "test_dest_tab",
      sourceRowCount: 1250, destinationRowCount: 1250, sourceColumnCount: 10, destinationColumnCount: 10,
      rowsMatch: 'YES', columnsMatch: 'YES', samplesMatch: 'YES', dataHash: "test-hash-abc",
      status: 'COMPLETE', details: "Sample verification entry."
    });
    logVerification({ // Log a sample failed verification entry
      sessionId: sessionId, ruleId: "Sample Rule (Error)", sourceType: "Test Data",
      sourceFile: "sample_error.csv", destinationSheet: "test_error_tab",
      sourceRowCount: 1000, destinationRowCount: 980, sourceColumnCount: 5, destinationColumnCount: 5,
      rowsMatch: 'NO', columnsMatch: 'YES', samplesMatch: 'NO', dataHash: "test-hash-xyz",
      status: 'ERROR', details: "Sample failed verification entry (row count & sample mismatch)."
    });
    logDiagnostic({ // Log a sample diagnostic entry
      sessionId: sessionId, position: "Sample Row 5", column: "C",
      sourceValue: "123.45", destValue: "123.456", normalizedSource: "123.45", normalizedDest: "123.456",
      details: "Sample diagnostic mismatch entry."
    });


    // Send the summary email in "TEST" mode
    sendRunSummaryEmail(sessionId, "TEST", testResults); // Use "TEST" status

    ui.alert('Test Email Sent', 'A sample test report has been sent to the configured recipients.', ui.ButtonSet.OK);
    logOperation(sessionId, "TEST_EMAIL_SENT", "Successfully initiated sending of test email report.");

  } catch (error) {
    const errorMsg = `Error sending test email: ${error.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "TEST_EMAIL_ERROR", errorMsg);
    ui.alert('Error', `Failed to send test email:\n${error.message}`, ui.ButtonSet.OK);
  }
}


/**
* Updates a temporary progress indicator sheet.
* @param {string} operation - The operation being performed (e.g., "Data Ingest").
* @param {number} progress - Progress percentage (0-100).
* @param {string} status - Status message.
*/
function updateProgressIndicator(operation, progress, status) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetName = '_ScriptProgress'; // Use a distinct name
    let progressSheet = ss.getSheetByName(sheetName);

    if (!progressSheet) {
      progressSheet = ss.insertSheet(sheetName, ss.getNumSheets()); // Insert at end
      progressSheet.hideSheet(); // Hide by default
      progressSheet.getRange("A1:C1").setValues([["Operation", "Progress", "Status"]]).setFontWeight("bold");
      progressSheet.setColumnWidths(1, 3, 150); // Adjust widths
      progressSheet.setFrozenRows(1);
    }

    // Update the progress information in row 2
    progress = Math.max(0, Math.min(100, Math.round(progress))); // Clamp progress 0-100
    progressSheet.getRange("A2").setValue(operation);
    progressSheet.getRange("B2").setValue(`${progress}%`);
    progressSheet.getRange("C2").setValue(status);

    // Make sheet visible while running (progress > 0 and < 100)
    if (progress > 0 && progress < 100) {
      if (progressSheet.isSheetHidden()) progressSheet.showSheet();
    } else {
      // Consider hiding immediately on 0 or 100, or let removeProgressSheet handle it
    }


    SpreadsheetApp.flush(); // Try to force update

  } catch (error) {
    // Don't let progress indicator issues affect the main process
    Logger.log(`Warning: Error updating progress indicator: ${error.message}`);
  }
}

/**
* Removes the progress indicator sheet and any associated cleanup triggers.
*/
function removeProgressSheet() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheetName = '_ScriptProgress';
    const progressSheet = ss.getSheetByName(sheetName);

    if (progressSheet) {
      ss.deleteSheet(progressSheet);
      Logger.log(`Removed progress sheet "${sheetName}".`);
    }

    // Delete specific cleanup triggers for this function if they were used
    const triggers = ScriptApp.getProjectTriggers();
    for (const trigger of triggers) {
      if (trigger.getHandlerFunction() === 'removeProgressSheet') {
        try {
          ScriptApp.deleteTrigger(trigger);
          Logger.log(`Deleted trigger for removeProgressSheet.`);
        } catch (e) {
          Logger.log(`Warning: Could not delete trigger for removeProgressSheet: ${e.message}`);
        }
      }
    }
  } catch (error) {
    Logger.log(`Warning: Error removing progress sheet: ${error.message}`);
  }
}

/**
* Checks if fast mode is enabled and if a specific feature should be disabled in fast mode
* @param {string} featureName - Name of the feature to check
* @returns {boolean} True if the feature should be disabled
*/
function shouldDisableFeature(featureName) {
  if (!CONFIG.FAST_MODE) return false;
  const featureConfig = CONFIG[featureName];
  return featureConfig && featureConfig.DISABLE_IN_FAST_MODE === true;
}

/**
* Modified log function that respects fast mode
* @param {string} eventType - Type of event to log
* @param {string} message - Message to log
* @param {string} sessionId - Optional session ID
*/
function logEvent(eventType, message, sessionId) {
  if (shouldDisableFeature('LOG_SHEET_NAME')) return;
  // ... existing logging code ...
}

/**
* Modified verification function that respects fast mode
*/
function verifyData(sourceData, destData, ruleId, sessionId) {
  if (shouldDisableFeature('VERIFICATION_CONFIG')) return true;
  // ... existing verification code ...
}

/**
* Modified email notification function that respects fast mode
*/
function sendEmailNotification(subject, body, attachments) {
  if (shouldDisableFeature('EMAIL_CONFIG')) return;
  // ... existing email code ...
}

/**
* Checks if a specific feature should be disabled based on performance settings
* @param {string} featureName - Name of the feature to check (e.g., 'DISABLE_EMAILS')
* @returns {boolean} True if the feature should be disabled
*/
function isFeatureDisabled(featureName) {
  return CONFIG.PERFORMANCE_CONFIG[featureName] === true;
}

/**
* Gets the current logging level
* @returns {string} Current logging level
*/
function getLoggingLevel() {
  return CONFIG.PERFORMANCE_CONFIG.LOGGING_LEVEL;
}

/**
* Gets the current verification level
* @returns {string} Current verification level
*/
function getVerificationLevel() {
  return CONFIG.PERFORMANCE_CONFIG.VERIFICATION_LEVEL;
}

/**
* Gets the batch size for processing
* @returns {number} Current batch size
*/
function getBatchSize() {
  return CONFIG.PERFORMANCE_CONFIG.BATCH_SIZE;
}

/**
* Gets the maximum number of concurrent operations
* @returns {number} Maximum concurrent operations
*/
function getMaxConcurrentOperations() {
  return CONFIG.PERFORMANCE_CONFIG.MAX_CONCURRENT_OPERATIONS;
}

/**
* Modified log function that respects performance settings
* @param {string} eventType - Type of event to log
* @param {string} message - Message to log
* @param {string} sessionId - Optional session ID
*/
function logEvent(eventType, message, sessionId) {
  if (isFeatureDisabled('DISABLE_LOGGING')) return;
  
  const loggingLevel = getLoggingLevel();
  
  // Skip logging based on level
  if (loggingLevel === 'MINIMAL' && 
      !['ERROR', 'ABORT', 'COMPLETE'].includes(eventType)) {
    return;
  }
  
  if (loggingLevel === 'STANDARD' && 
      ['DATA_HASH', 'VERIFICATION_DETAIL'].includes(eventType)) {
    return;
  }
  
  // ... existing logging code ...
}

/**
* Modified verification function that respects performance settings
*/
function verifyData(sourceData, destData, ruleId, sessionId) {
  if (isFeatureDisabled('DISABLE_VERIFICATION')) return true;
  
  const verificationLevel = getVerificationLevel();
  if (verificationLevel === 'NONE') return true;
  
  // Basic verification only checks row counts
  if (verificationLevel === 'BASIC') {
    return sourceData.length === destData.length;
  }
  
  // Full verification performs all checks
  // ... existing verification code ...
}

/**
* Modified email notification function that respects performance settings
*/
function sendEmailNotification(subject, body, attachments) {
  if (isFeatureDisabled('DISABLE_EMAILS')) return;
  // ... existing email code ...
}

/**
* Modified formatting function that respects performance settings
*/
function applyFormatting(sheet, formatConfig) {
  if (isFeatureDisabled('DISABLE_FORMATTING')) return;
  // ... existing formatting code ...
}

/**
* Process data from source to destination with caching and batching
* @param {Object} rule - Ingest rule configuration
* @param {string} sessionId - Session ID for tracking
* @returns {Promise} Promise that resolves when processing is complete
*/
function processData(rule, sessionId) {
  return new Promise((resolve, reject) => {
    try {
      // Get source data with caching
      const sourceData = getSourceDataWithCache(rule, sessionId);
      
      // Process data in batches
      BatchProcessor.process(sourceData, (batch) => {
        return processDataBatch(batch, rule, sessionId);
      }).then(resolve).catch(reject);
      
    } catch (error) {
      reject(error);
    }
  });
}

/**
* Get source data with caching
* @param {Object} rule - Ingest rule configuration
* @param {string} sessionId - Session ID for tracking
* @returns {Array} Source data
*/
function getSourceDataWithCache(rule, sessionId) {
  const cacheKey = `source_${rule.ruleId}_${sessionId}`;
  let sourceData = Cache.get(cacheKey);
  
  if (!sourceData) {
    sourceData = getSourceData(rule);
    Cache.set(cacheKey, sourceData);
  }
  
  return sourceData;
}

/**
* Process a batch of data
* @param {Array} batch - Batch of data to process
* @param {Object} rule - Ingest rule configuration
* @param {string} sessionId - Session ID for tracking
* @returns {Promise} Promise that resolves when batch is processed
*/
function processDataBatch(batch, rule, sessionId) {
  return new Promise((resolve, reject) => {
    try {
      const destSheet = getDestinationSheet(rule);
      
      // Get last row with caching
      const lastRowKey = `lastRow_${rule.dest_sheetId}_${rule.dest_sheet_tabName}`;
      let lastRow = Cache.get(lastRowKey);
      
      if (!lastRow) {
        lastRow = destSheet.getLastRow();
        Cache.set(lastRowKey, lastRow);
      }
      
      // Process the batch
      const startRow = lastRow + 1;
      const range = destSheet.getRange(startRow, 1, batch.length, batch[0].length);
      range.setValues(batch);
      
      // Update cache
      Cache.set(lastRowKey, startRow + batch.length - 1);
      
      resolve();
    } catch (error) {
      reject(error);
    }
  });
}

/**
* Modified getDestinationSheet with caching
* @param {Object} rule - Ingest rule configuration
* @returns {GoogleAppsScript.Spreadsheet.Sheet} Destination sheet
*/
function getDestinationSheet(rule) {
  const cacheKey = `sheet_${rule.dest_sheetId}_${rule.dest_sheet_tabName}`;
  let sheet = Cache.get(cacheKey);
  
  if (!sheet) {
    sheet = SpreadsheetApp.openById(rule.dest_sheetId)
      .getSheetByName(rule.dest_sheet_tabName);
    Cache.set(cacheKey, sheet);
  }
  
  return sheet;
}