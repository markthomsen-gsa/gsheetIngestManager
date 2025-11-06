/**
 * Shared utilities and validation functions
 * Common functions used across all components
 */

/**
 * Validate Google Sheet ID format
 * Checks if string matches Google Sheets ID format (44 characters)
 * @param {string} sheetId - Sheet ID to validate
 * @returns {boolean} True if valid sheet ID format
 */
function isValidSheetId(sheetId) {
  // Google Sheet ID format: 44 characters, alphanumeric + hyphens/underscores
  return /^[a-zA-Z0-9_-]{44}$/.test(sheetId);
}

/**
 * Validate Google Sheets URL format
 * Checks if URL matches Google Sheets pattern
 * @param {string} url - URL to validate
 * @returns {boolean} True if valid Google Sheets URL
 */
function isValidSheetUrl(url) {
  // Check if URL matches Google Sheets pattern
  return /https:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9_-]{44}\//.test(url);
}

/**
 * Extract Sheet ID from Google Sheets URL
 * Extracts 44-character sheet ID from Google Sheets URL
 * @param {string} url - Google Sheets URL
 * @returns {string|null} Sheet ID if found, null otherwise
 */
function extractSheetIdFromUrl(url) {
  // Match: /spreadsheets/d/[SHEET_ID]/
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]{44})\//);
  return match ? match[1] : null;
}

/**
 * Parse Google Sheets URL to extract sheet ID and optional GID
 * Supports multiple URL formats:
 * - /spreadsheets/d/SHEET_ID/edit
 * - /spreadsheets/d/SHEET_ID/edit#gid=123456789
 * - /spreadsheets/d/SHEET_ID/edit?param=value#gid=123456789
 *
 * @param {string} url - Google Sheets URL
 * @returns {Object} { sheetId: string, gid: string|null }
 * @throws {Error} If URL format is invalid
 */
function parseSheetUrl(url) {
  if (!url || typeof url !== 'string') {
    throw new Error('Invalid URL: must be a non-empty string');
  }

  // Extract sheet ID
  const sheetIdMatch = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]{44})\//);
  if (!sheetIdMatch) {
    throw new Error('Invalid Google Sheets URL: sheet ID not found');
  }
  const sheetId = sheetIdMatch[1];

  // Extract GID if present (after #gid=)
  let gid = null;
  const gidMatch = url.match(/#gid=([0-9]+)/);
  if (gidMatch) {
    gid = gidMatch[1];
  }

  return { sheetId, gid };
}

/**
 * Parse destination field - handles both URLs and IDs
 * Converts destination input to sheet ID, with fallback to current spreadsheet
 * @param {string} destination - Destination URL, ID, or empty string
 * @returns {string} Sheet ID for destination
 * @throws {Error} If destination format is invalid
 */
function parseDestination(destination) {
  // Auto-default to current spreadsheet if destination is empty
  if (!destination || destination.trim() === '') {
    return SpreadsheetApp.getActiveSpreadsheet().getId();
  }

  // If it looks like a URL, extract the ID
  if (destination.includes('docs.google.com/spreadsheets')) {
    const sheetId = extractSheetIdFromUrl(destination);
    if (!sheetId) {
      throw new Error('Invalid Google Sheets URL format');
    }
    return sheetId;
  }

  // Otherwise treat as Sheet ID
  if (!isValidSheetId(destination)) {
    throw new Error('Invalid Sheet ID format (must be 44 characters)');
  }

  return destination;
}

/**
 * Validate email address format
 * Checks if string matches valid email format
 * @param {string} email - Email address to validate
 * @returns {boolean} True if valid email format
 */
function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

/**
 * Validate regex pattern
 * Tests if string is a valid regular expression
 * @param {string} pattern - Regex pattern to validate
 * @returns {boolean} True if valid regex pattern
 */
function isValidRegex(pattern) {
  try {
    new RegExp(pattern);
    return true;
  } catch (e) {
    return false;
  }
}

/**
 * Detect column positions from spreadsheet headers
 * Maps field names to column indices using flexible header matching
 * @param {Array<string>} headers - Array of header strings
 * @returns {Object} Column mapping object
 * @returns {number} returns.id - Column index for Rule ID (-1 if not found)
 * @returns {number} returns.active - Column index for Active (-1 if not found)
 * @returns {number} returns.method - Column index for Method (-1 if not found)
 * @returns {number} returns.sourceQuery - Column index for Source Query (-1 if not found)
 * @returns {number} returns.attachmentPattern - Column index for Attachment Pattern (-1 if not found)
 * @returns {number} returns.sourceTab - Column index for Source Tab (-1 if not found)
 * @returns {number} returns.destination - Column index for Destination (-1 if not found)
 * @returns {number} returns.destinationTab - Column index for Destination Tab (-1 if not found)
 * @returns {number} returns.mode - Column index for Mode (-1 if not found)
 * @returns {number} returns.emailRecipients - Column index for Email Recipients (-1 if not found)
 */
function detectColumnPositions(headers) {
  const columnMap = {};

  // Define possible header variations for each field
  const headerPatterns = {
    active: ['Active', 'active'],  // Active is now first
    id: ['Rule ID', 'rule id', 'id'],
    method: ['Method', 'method'],
    sourceQuery: ['Source Query', 'source query', 'query'],
    attachmentPattern: ['Attachment Pattern', 'attachment pattern', 'pattern'],
    sourceTab: ['Source Tab', 'source tab', 'Source Tab Name', 'source'],
    destination: ['Destination (URL, ID, or empty for current)', 'Destination (URL or ID)', 'Destination', 'destination'],
    destinationTab: ['Destination Tab', 'destination tab', 'tab'],
    mode: ['Mode', 'mode'],
    lastSuccessDimensions: ['Last Success Dimensions', 'last success dimensions', 'dimensions', 'last dimensions'],
    lastRunResult: ['Last Run Result', 'last run result', 'result'],
    daysSinceLastSuccess: ['Days Since Last Success', 'days since last success', 'days since success', 'days'],
    lastRunTimestamp: ['Last Run Timestamp', 'last run timestamp', 'last run', 'timestamp'],
    emailRecipients: ['Email Recipients', 'email recipients', 'recipients', 'emails'],
    validationFormula: ['Validation Formula', 'validation formula', 'validation', 'formula']
  };

  // Find each field's position
  Object.keys(headerPatterns).forEach(field => {
    columnMap[field] = -1; // Default to not found

    for (let i = 0; i < headers.length; i++) {
      const header = headers[i] ? headers[i].toString().trim() : '';
      if (headerPatterns[field].some(pattern =>
        header.toLowerCase() === pattern.toLowerCase())) {
        columnMap[field] = i;
        break;
      }
    }
  });

  return columnMap;
}

/**
 * Safely extract value from row using column mapping
 * Gets value from row at specified column index with bounds checking
 * @param {Array} row - Row data array
 * @param {Object} columnMap - Column mapping object
 * @param {string} field - Field name to extract
 * @returns {*} Value at column position or undefined if not found
 */
function getValueFromRow(row, columnMap, field) {
  const colIndex = columnMap[field];
  if (colIndex === -1 || colIndex >= row.length) {
    return undefined;
  }
  return row[colIndex];
}

/**
 * Parse rule from row data using dynamic column detection
 * Converts spreadsheet row data to rule object using column mapping
 * @param {Array} row - Row data array
 * @param {Object} columnMap - Column mapping object
 * @returns {Object} Parsed rule object
 * @returns {string} returns.id - Rule ID
 * @returns {boolean} returns.active - Active status
 * @returns {string} returns.method - Processing method
 * @returns {string} returns.sourceQuery - Source query
 * @returns {string} [returns.attachmentPattern] - Attachment pattern
 * @returns {string} [returns.sourceTab] - Source tab name
 * @returns {string} returns.destination - Destination
 * @returns {string} [returns.destinationTab] - Destination tab
 * @returns {string} returns.mode - Processing mode
 * @returns {string} [returns.emailRecipients] - Email recipients
 */
function parseRuleFromRow(row, columnMap) {
  return {
    id: getValueFromRow(row, columnMap, 'id'),
    active: getValueFromRow(row, columnMap, 'active'),
    method: getValueFromRow(row, columnMap, 'method'),
    sourceQuery: getValueFromRow(row, columnMap, 'sourceQuery'),
    attachmentPattern: getValueFromRow(row, columnMap, 'attachmentPattern'),
    sourceTab: getValueFromRow(row, columnMap, 'sourceTab'),
    destination: getValueFromRow(row, columnMap, 'destination'),
    destinationTab: getValueFromRow(row, columnMap, 'destinationTab'),
    mode: getValueFromRow(row, columnMap, 'mode'),
    lastRunTimestamp: getValueFromRow(row, columnMap, 'lastRunTimestamp'),
    lastRunResult: getValueFromRow(row, columnMap, 'lastRunResult'),
    lastSuccessDimensions: getValueFromRow(row, columnMap, 'lastSuccessDimensions'),
    emailRecipients: getValueFromRow(row, columnMap, 'emailRecipients'),
    validationFormula: getValueFromRow(row, columnMap, 'validationFormula')
  };
}

/**
 * Validate individual rule configuration
 * Performs comprehensive validation of rule configuration
 * @param {Object} rule - Rule object to validate
 * @returns {Array<string>} Array of validation error messages
 */
function validateRule(rule) {
  const errors = [];

  // Required fields validation
  if (!rule.id) errors.push('Rule ID is required');
  if (!rule.method || !VALID_METHODS.includes(rule.method)) {
    errors.push(`Method must be one of: ${VALID_METHODS.join(', ')}`);
  }
  if (!rule.mode || !VALID_MODES.includes(rule.mode)) {
    errors.push(`Mode must be one of: ${VALID_MODES.join(', ')}`);
  }

  // Method-specific validation
  if (rule.method === 'email') {
    if (!rule.sourceQuery) {
      errors.push('Email method requires source query (e.g., "from:sender@domain.com")');
    }
    if (!rule.attachmentPattern) {
      errors.push('Email method requires attachment pattern (e.g., ".*\\.csv$")');
    } else if (!isValidRegex(rule.attachmentPattern)) {
      errors.push('Invalid regex pattern in attachmentPattern');
    }
  }

  if (rule.method === 'gSheet') {
    if (!rule.sourceQuery) {
      errors.push('gSheet method requires source sheet ID or URL');
    } else {
      // Check if it's a URL or sheet ID
      const sourceQuery = rule.sourceQuery.trim();
      if (sourceQuery.includes('docs.google.com/spreadsheets')) {
        // Validate URL format
        try {
          parseSheetUrl(sourceQuery);
        } catch (error) {
          errors.push(`Invalid source URL: ${error.message}`);
        }
      } else {
        // Validate sheet ID format
        if (!isValidSheetId(sourceQuery)) {
          errors.push('Invalid source sheet ID format (must be 44 characters or valid URL)');
        }
      }
    }

    // Source Tab is optional - no validation needed (any string is valid)
  }

  // Destination validation - support both URLs and IDs
  if (rule.destination) {
    try {
      parseDestination(rule.destination);
    } catch (error) {
      errors.push(`Destination error: ${error.message}`);
    }
  }

  // Email recipients validation
  if (rule.emailRecipients && rule.emailRecipients.trim()) {
    const emails = rule.emailRecipients.split(',').map(e => e.trim());
    emails.forEach(email => {
      if (!isValidEmail(email)) {
        errors.push(`Invalid email address: ${email}`);
      }
    });
  }

  return errors;
}

/**
 * Validate all rules in configuration sheet
 * Validates all rules in the rules sheet and reports errors
 * @returns {boolean} True if all rules are valid
 * @throws {Error} If validation fails with detailed error messages
 */
function validateAllRules() {
  try {
    const rulesSheet = getSheet('rules');
    const data = rulesSheet.getDataRange().getValues();

    if (data.length < 2) {
      throw new Error('No rules found in configuration sheet');
    }

    const headers = data[0];
    const columnMap = detectColumnPositions(headers);
    const rules = data.slice(1);

    let allValid = true;
    const validationErrors = [];

    rules.forEach((row, index) => {
      const rule = parseRuleFromRow(row, columnMap);

      // Skip empty rows (check if rule has minimal data)
      if (!rule.id) return;

      const errors = validateRule(rule);
      if (errors.length > 0) {
        allValid = false;
        validationErrors.push({
          ruleId: rule.id || `Row ${index + 2}`,
          errors: errors
        });
      }
    });

    if (!allValid) {
      const errorMessage = validationErrors.map(v =>
        `${v.ruleId}: ${v.errors.join(', ')}`
      ).join('\n');
      throw new Error(`Configuration validation failed:\n${errorMessage}`);
    }

    return true;
  } catch (error) {
    console.error('Validation error:', error.message);
    throw error;
  }
}

/**
 * Execute function with retry logic
 * Wraps function execution with exponential backoff retry
 * @param {Function} operation - Function to execute
 * @param {number} [maxRetries=RETRY_ATTEMPTS] - Maximum retry attempts
 * @returns {*} Result of operation
 * @throws {Error} If operation fails after all retries
 */
function executeWithRetry(operation, maxRetries = RETRY_ATTEMPTS) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return operation();
    } catch (error) {
      if (!isRetryableError(error) || attempt === maxRetries) {
        throw error;
      }

      // Exponential backoff: 1s, 2s, 4s
      const delay = RETRY_DELAY_BASE_MS * Math.pow(2, attempt - 1);
      Utilities.sleep(delay);

      console.log(`Retry attempt ${attempt}/${maxRetries} after ${delay}ms delay`);
    }
  }
}

/**
 * Determine if error is retryable
 * Checks if error message indicates a transient failure
 * @param {Error} error - Error object to check
 * @returns {boolean} True if error should be retried
 */
function isRetryableError(error) {
  const retryablePatterns = [
    /timeout/i,
    /rate limit/i,
    /service unavailable/i,
    /temporary/i,
    /quota/i
  ];

  return retryablePatterns.some(pattern => pattern.test(error.message));
}

/**
 * Create rules configuration sheet
 * Creates rules sheet with headers, examples, and data validation
 * @returns {Sheet} Created rules sheet
 */
function createRulesSheet() {
  const sheet = createSheet('rules');

  // Set up headers if sheet is empty
  if (sheet.getLastRow() === 0) {
    const headers = [
      'Active',                // Active column is now first
      'Rule ID',               // Rule ID moved to second
      'Validation Formula',   // Validation Formula (between ID and Method)
      'Method',
      'Source Query',
      'Attachment Pattern',
      'Source Tab',
      'Destination (URL, ID, or empty for current)',
      'Destination Tab',
      'Mode',
      'Last Success Dimensions',
      'Last Run Result',
      'Days Since Last Success',
      'Last Run Timestamp',
      'Email Recipients'
    ];

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#E8F0FE');
    headerRange.setVerticalAlignment('middle');

    // Add example rule for email method
    const exampleRule = [
      true,                     // Active (now first)
      'example-rule',           // Rule ID (now second)
      '',                       // Validation Formula (users can enter formulas manually)
      'email',
      'from:reports@company.com subject:Daily',
      'sales-.*\\.csv$',
      '',  // Source Tab (not used for email)
      'https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID_HERE/edit',
      'Data Import',
      'clearAndReuse',
      '',  // Last Success Dimensions
      '',  // Last Run Result
      '',  // Days Since Last Success (formula will be applied)
      '',  // Last Run Timestamp
      'mark.thomsen@gsa.gov'
    ];

    sheet.getRange(2, 1, 1, exampleRule.length).setValues([exampleRule]);

    // Add gSheet example rule
    const gSheetExampleRule = [
      false,  // Active (now first) - Inactive by default
      'gsheet-example',         // Rule ID (now second)
      '',                       // Validation Formula (users can enter formulas manually)
      'gSheet',
      'https://docs.google.com/spreadsheets/d/SOURCE_SHEET_ID_HERE/edit#gid=0',
      '',  // Not used for gSheet
      'Sales Data',  // Source tab name
      '',  // Empty = current spreadsheet
      'Monthly Import',
      'clearAndReuse',
      '',  // Last Success Dimensions
      '',  // Last Run Result
      '',  // Days Since Last Success (formula will be applied)
      '',  // Last Run Timestamp
      ''
    ];

    sheet.getRange(3, 1, 1, gSheetExampleRule.length).setValues([gSheetExampleRule]);

    // Clean up FIRST: Remove extra rows and columns before applying validation
    cleanupSheetRowsAndColumns(sheet, headers.length, 10);
    
    // Set vertical alignment for all cells in the sheet
    const lastRow = sheet.getLastRow();
    const lastCol = headers.length;
    const allDataRange = sheet.getRange(1, 1, lastRow, lastCol);
    allDataRange.setVerticalAlignment('middle');
    
    // Add data validation for specific columns (only to existing rows)
    addRuleValidation(sheet);
    
    // Apply color coding to validation formula column
    applyValidationColorCoding(sheet);
  }

  return sheet;
}

/**
 * Clean up sheet by removing extra rows and columns
 * Keeps only the specified number of rows and columns
 * @param {Sheet} sheet - Sheet to clean up
 * @param {number} maxColumns - Maximum number of columns to keep
 * @param {number} maxRows - Maximum number of rows to keep
 */
function cleanupSheetRowsAndColumns(sheet, maxColumns, maxRows) {
  // Get the actual maximum rows and columns in the sheet
  const maxSheetRows = sheet.getMaxRows();
  const maxSheetCols = sheet.getMaxColumns();
  
  // Delete extra rows (keep only maxRows rows)
  if (maxSheetRows > maxRows) {
    const rowsToDelete = maxSheetRows - maxRows;
    sheet.deleteRows(maxRows + 1, rowsToDelete);
  }
  
  // Delete extra columns (keep only maxColumns columns)
  if (maxSheetCols > maxColumns) {
    const colsToDelete = maxSheetCols - maxColumns;
    sheet.deleteColumns(maxColumns + 1, colsToDelete);
  }
}

/**
 * Add data validation to rules sheet
 * Adds dropdown validation for method, mode, and active columns
 * @param {Sheet} sheet - Rules sheet to add validation to
 */
function addRuleValidation(sheet) {
  // Only apply validation to rows that actually exist (don't create new rows)
  const actualLastRow = sheet.getLastRow();
  const maxRows = sheet.getMaxRows();
  const lastRow = Math.min(Math.max(actualLastRow, 2), maxRows); // At least row 2 (header), but not beyond max
  
  // Only apply validation if there are data rows (beyond header)
  if (lastRow < 2) {
    return; // No data rows to validate
  }

  // Method column validation
  const methodRange = sheet.getRange(2, RULE_COLUMNS.METHOD + 1, lastRow - 1, 1);
  const methodValidation = SpreadsheetApp.newDataValidation()
    .requireValueInList(VALID_METHODS)
    .build();
  methodRange.setDataValidation(methodValidation);

  // Mode column validation
  const modeRange = sheet.getRange(2, RULE_COLUMNS.MODE + 1, lastRow - 1, 1);
  const modeValidation = SpreadsheetApp.newDataValidation()
    .requireValueInList(VALID_MODES)
    .build();
  modeRange.setDataValidation(modeValidation);

  // Active column validation - use checkboxes
  const activeRange = sheet.getRange(2, RULE_COLUMNS.ACTIVE + 1, lastRow - 1, 1);
  const activeValidation = SpreadsheetApp.newDataValidation()
    .requireCheckbox()
    .build();
  activeRange.setDataValidation(activeValidation);

  // Set number format for Days Since Last Success column (integer, no decimals)
  if (lastRow > 1) {
    const daysCol = RULE_COLUMNS.DAYS_SINCE_LAST_SUCCESS + 1;
    const daysColumnRange = sheet.getRange(2, daysCol, lastRow - 1, 1);
    daysColumnRange.setNumberFormat('0');
  }

  // Add note to Source Tab column header
  const sourceTabHeader = sheet.getRange(1, RULE_COLUMNS.SOURCE_TAB + 1);
  sourceTabHeader.setNote(
    'Optional: Specify tab name to read from source sheet (gSheet method only).\n' +
    'Leave empty to use first tab.\n' +
    'Not used for email or push methods.'
  );
  
  // Apply color coding to validation formula column
  applyValidationColorCoding(sheet);
}

/**
 * Apply color coding to validation formula column
 * Applies conditional formatting rules to validation formula column based on cell value
 * Color rules: TRUE=green, FALSE=red, blank=no color, anything else=yellow
 * @param {Sheet} sheet - Rules sheet to apply color coding to
 */
function applyValidationColorCoding(sheet) {
  // Get validation formula column index
  const validationCol = RULE_COLUMNS.VALIDATION_FORMULA + 1; // Convert to 1-based
  
  // Get data range for validation column (skip header row)
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return; // No data rows to format
  }
  
  const validationRange = sheet.getRange(2, validationCol, lastRow - 1, 1);
  
  // Get column letter for formula reference
  const colLetter = columnIndexToLetter(validationCol);
  
  // Color constants
  const TRUE_COLOR = '#C6EFCE';  // Green
  const FALSE_COLOR = '#FFC7CE'; // Red
  const OTHER_COLOR = '#FFEB9C'; // Yellow
  
  // Get all existing conditional format rules from the sheet
  let allRules = sheet.getConditionalFormatRules();
  
  // Filter out any existing rules that apply to our validation range
  // We'll replace them with our new rules
  const validationRangeA1 = validationRange.getA1Notation();
  allRules = allRules.filter(rule => {
    // Check if this rule applies to our validation range
    const ruleRanges = rule.getRanges();
    return !ruleRanges.some(range => range.getA1Notation() === validationRangeA1);
  });
  
  // Create conditional formatting rules
  // In Google Sheets conditional formatting, we need to use relative references
  // Using INDIRECT with ROW() to reference the current cell being evaluated
  const newRules = [];
  
  // Rule 1: TRUE = Green
  const trueRule = SpreadsheetApp.newConditionalFormatRule()
    .setRanges([validationRange])
    .whenFormulaSatisfied(`=INDIRECT("${colLetter}"&ROW())=TRUE`)
    .setBackground(TRUE_COLOR)
    .build();
  newRules.push(trueRule);
  
  // Rule 2: FALSE = Red
  // Also check for text "FALSE" in case user typed it as text
  // IMPORTANT: Exclude blank cells - they should have no color
  const falseRule = SpreadsheetApp.newConditionalFormatRule()
    .setRanges([validationRange])
    .whenFormulaSatisfied(`=AND(INDIRECT("${colLetter}"&ROW())<>"",OR(INDIRECT("${colLetter}"&ROW())=FALSE,INDIRECT("${colLetter}"&ROW())="FALSE"))`)
    .setBackground(FALSE_COLOR)
    .build();
  newRules.push(falseRule);
  
  // Rule 3: Anything else (not TRUE, not FALSE, not blank) = Yellow
  const otherRule = SpreadsheetApp.newConditionalFormatRule()
    .setRanges([validationRange])
    .whenFormulaSatisfied(`=AND(INDIRECT("${colLetter}"&ROW())<>TRUE,INDIRECT("${colLetter}"&ROW())<>FALSE,INDIRECT("${colLetter}"&ROW())<>"FALSE",INDIRECT("${colLetter}"&ROW())<>"")`)
    .setBackground(OTHER_COLOR)
    .build();
  newRules.push(otherRule);
  
  // Add our new rules to the existing rules
  allRules = allRules.concat(newRules);
  
  // Apply all rules to the sheet (this is a Sheet method, not Range method)
  sheet.setConditionalFormatRules(allRules);
  
  // Force a recalculation
  SpreadsheetApp.flush();
}

/**
 * Test function to manually apply validation color coding
 * Call this function to test or re-apply color coding to validation column
 * @function testValidationColorCoding
 */
function testValidationColorCoding() {
  try {
    const sheet = getSheet('rules');
    applyValidationColorCoding(sheet);
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Validation color coding applied',
      'Success',
      3000
    );
  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Error: ${error.message}`,
      'Error',
      5000
    );
    console.error('Error applying validation color coding:', error);
  }
}

/**
 * Get spreadsheet by ID with error handling
 * Opens spreadsheet by ID with comprehensive error handling
 * @param {string} sheetId - Google Sheets ID
 * @returns {Spreadsheet} Google Sheets Spreadsheet object
 * @throws {Error} If sheet cannot be accessed
 */
function getSpreadsheetById(sheetId) {
  try {
    return SpreadsheetApp.openById(sheetId);
  } catch (error) {
    throw new Error(`Cannot access sheet with ID: ${sheetId}. Check permissions and ID format.`);
  }
}

/**
 * Get existing sheet or create new one if it doesn't exist
 * Finds sheet by name or creates it if missing
 * @param {Spreadsheet} spreadsheet - Parent spreadsheet
 * @param {string} tabName - Name of tab to find/create
 * @param {string} [sessionId] - Session ID for logging
 * @param {string} [ruleId] - Rule ID for logging
 * @returns {Sheet} Google Sheets Sheet object
 */
function getOrCreateSheet(spreadsheet, tabName, sessionId = null, ruleId = null) {
  // If no specific tab requested, use first sheet
  if (!tabName || !tabName.trim()) {
    const sheets = spreadsheet.getSheets();
    if (sheets.length === 0) {
      // Edge case: spreadsheet has no sheets, create default one
      const newSheet = spreadsheet.insertSheet('Sheet1');
      if (sessionId && ruleId) {
        logEntry(sessionId, ruleId, 'INFO', 'Created default tab: Sheet1');
      }
      console.log('Created default tab: Sheet1');
      return newSheet;
    }
    return sheets[0];
  }

  const trimmedName = tabName.trim();

  // Try to find existing tab
  let sheet = spreadsheet.getSheetByName(trimmedName);

  if (!sheet) {
    // Create the tab if it doesn't exist
    sheet = spreadsheet.insertSheet(trimmedName);
    if (sessionId && ruleId) {
      logEntry(sessionId, ruleId, 'INFO', `Created new tab: ${trimmedName}`);
    }
    console.log(`Created new tab: ${trimmedName}`);
  }

  return sheet;
}

/**
 * Format timestamp for display
 * Formats date using script timezone
 * @param {Date} date - Date to format
 * @returns {string} Formatted timestamp string
 */
function formatTimestamp(date) {
  return Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

/**
 * Format last run timestamp for rules sheet
 * Returns Date object for formula compatibility
 * Display format is set via number format in the cell: 'ddd, mmm d, yyyy h:mm AM/PM'
 * @param {Date} date - Date to format
 * @returns {Date} Date object (formatted via number format)
 */
function formatLastRunTimestamp(date) {
  // Return the date object - Google Sheets will format it via number format
  // This allows formulas to work with the date value
  // Format: 'ddd, mmm d, yyyy h:mm AM/PM' (e.g., "Mon, Nov 3, 2025 2:30 PM")
  return date;
}

/**
 * Get ordinal suffix for a number (1st, 2nd, 3rd, 4th, etc.)
 * @param {number} n - Number to get ordinal for
 * @returns {string} Ordinal suffix
 */
function getOrdinalSuffix(n) {
  const j = n % 10;
  const k = n % 100;
  if (j === 1 && k !== 11) return 'st';
  if (j === 2 && k !== 12) return 'nd';
  if (j === 3 && k !== 13) return 'rd';
  return 'th';
}

/**
 * Get timezone abbreviation for a date
 * @param {Date} date - Date to get timezone for
 * @param {string} timezone - Timezone string (e.g., "America/New_York")
 * @returns {string} Timezone abbreviation (e.g., "EST", "EDT", "PST")
 */
function getTimezoneAbbreviation(date, timezone) {
  try {
    // Format date with timezone to get abbreviation
    const formatted = Utilities.formatDate(date, timezone, 'z');
    // Extract abbreviation (e.g., "EST", "EDT", "PST")
    return formatted;
  } catch (e) {
    // Fallback to timezone string if abbreviation can't be determined
    return timezone.split('/').pop().substring(0, 3).toUpperCase();
  }
}

/**
 * Convert column index (1-based) to column letter (A, B, C, ..., Z, AA, AB, ...)
 * Converts numeric column index to Google Sheets column letter notation
 * @param {number} columnIndex - 1-based column index
 * @returns {string} Column letter(s) (e.g., "A", "B", "Z", "AA", "AB")
 */
function columnIndexToLetter(columnIndex) {
  let result = '';
  while (columnIndex > 0) {
    columnIndex--;
    result = String.fromCharCode(65 + (columnIndex % 26)) + result;
    columnIndex = Math.floor(columnIndex / 26);
  }
  return result;
}

/**
 * Get color for dimensions cell based on row and column counts
 * Returns red for 0x0 and header-only (1x{N}, {N}x1, etc.), green for actual data
 * @param {number} rows - Number of rows processed
 * @param {number|undefined} columns - Number of columns processed (undefined if unknown)
 * @returns {string} Hex color code
 */
function getDimensionsColor(rows, columns) {
  // Color constants
  const EMPTY_OR_HEADER_COLOR = '#FFC7CE'; // Red for 0x0 and header row only
  const DATA_COLOR = '#C6EFCE'; // Green for actual data
  
  // If columns is undefined, we can't determine if it's header-only or data
  // Default to red (header-only) if rows is 1, otherwise green
  if (columns === undefined) {
    return rows === 1 ? EMPTY_OR_HEADER_COLOR : DATA_COLOR;
  }
  
  // 0x0 = no data (red)
  if (rows === 0 && columns === 0) {
    return EMPTY_OR_HEADER_COLOR;
  }
  
  // Header row only: 1x{N}, {N}x1, 1x1, 1x0, 0x1 (red)
  if (rows === 1 || columns === 1) {
    return EMPTY_OR_HEADER_COLOR;
  }
  
  // Actual data (green)
  return DATA_COLOR;
}

/**
 * Parse CSV file size estimation
 * Estimates CSV file size in MB for validation
 * @param {Array<Array<string>>|string} csvData - CSV data to estimate
 * @returns {number} Estimated size in MB
 */
function estimateCSVFileSize(csvData) {
  // Rough estimation: each character ≈ 1 byte
  const dataString = Array.isArray(csvData) ?
    csvData.map(row => row.join(',')).join('\n') :
    csvData.toString();

  return Math.ceil(dataString.length / (1024 * 1024)); // Size in MB
}

/**
 * Check if CSV file exceeds size limits
 * Validates CSV against size and row count limits
 * @param {Array<Array<string>>} csvData - CSV data to validate
 * @returns {boolean} True if CSV is within limits
 * @throws {Error} If CSV exceeds limits
 */
function validateCSVSize(csvData) {
  const estimatedSize = estimateCSVFileSize(csvData);

  if (estimatedSize > MAX_FILE_SIZE_MB) {
    throw new Error(`CSV file too large: ${estimatedSize}MB exceeds ${MAX_FILE_SIZE_MB}MB limit`);
  }

  if (Array.isArray(csvData) && csvData.length > MAX_ROWS_PER_FILE) {
    throw new Error(`CSV file too many rows: ${csvData.length} exceeds ${MAX_ROWS_PER_FILE} limit`);
  }

  return true;
}