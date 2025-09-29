/**
 * Shared utilities and validation functions
 * Common functions used across all components
 */

/**
 * Validate Google Sheet ID format
 */
function isValidSheetId(sheetId) {
  // Google Sheet ID format: 44 characters, alphanumeric + hyphens/underscores
  return /^[a-zA-Z0-9_-]{44}$/.test(sheetId);
}

/**
 * Validate Google Sheets URL format
 */
function isValidSheetUrl(url) {
  // Check if URL matches Google Sheets pattern
  return /https:\/\/docs\.google\.com\/spreadsheets\/d\/[a-zA-Z0-9_-]{44}\//.test(url);
}

/**
 * Extract Sheet ID from Google Sheets URL
 */
function extractSheetIdFromUrl(url) {
  // Match: /spreadsheets/d/[SHEET_ID]/
  const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]{44})\//);
  return match ? match[1] : null;
}

/**
 * Parse destination field - handles both URLs and IDs
 */
function parseDestination(destination) {
  if (!destination) {
    throw new Error('Destination is required');
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
 */
function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

/**
 * Validate regex pattern
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
 * Validate individual rule configuration
 */
function validateRule(rule) {
  const errors = [];

  // Required fields validation
  if (!rule.id) errors.push('Rule ID is required');
  if (!rule.method || !VALID_METHODS.includes(rule.method)) {
    errors.push(`Method must be one of: ${VALID_METHODS.join(', ')}`);
  }
  if (!rule.destination) errors.push('Destination is required');
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
      errors.push('gSheet method requires source sheet ID');
    } else if (!isValidSheetId(rule.sourceQuery)) {
      errors.push('Invalid source sheet ID format');
    }
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
 */
function validateAllRules() {
  try {
    const rulesSheet = getSheet('rules');
    const data = rulesSheet.getDataRange().getValues();

    if (data.length < 2) {
      throw new Error('No rules found in configuration sheet');
    }

    const headers = data[0];
    const rules = data.slice(1);

    let allValid = true;
    const validationErrors = [];

    rules.forEach((row, index) => {
      // Skip empty rows
      if (!row[RULE_COLUMNS.ID]) return;

      const rule = {
        id: row[RULE_COLUMNS.ID],
        active: row[RULE_COLUMNS.ACTIVE],
        method: row[RULE_COLUMNS.METHOD],
        sourceQuery: row[RULE_COLUMNS.SOURCE_QUERY],
        attachmentPattern: row[RULE_COLUMNS.ATTACHMENT_PATTERN],
        destination: row[RULE_COLUMNS.DESTINATION],
        destinationTab: row[RULE_COLUMNS.DESTINATION_TAB],
        mode: row[RULE_COLUMNS.MODE],
        emailRecipients: row[RULE_COLUMNS.EMAIL_RECIPIENTS]
      };

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
 */
function createRulesSheet() {
  const sheet = createSheet('rules');

  // Set up headers if sheet is empty
  if (sheet.getLastRow() === 0) {
    const headers = [
      'Rule ID',
      'Active',
      'Method',
      'Source Query',
      'Attachment Pattern',
      'Destination (URL or ID)',
      'Destination Tab',
      'Mode',
      'Email Recipients'
    ];

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#E8F0FE');

    // Add example rule
    const exampleRule = [
      'example-rule',
      true,
      'email',
      'from:reports@company.com subject:Daily',
      'sales-.*\\.csv$',
      'https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID_HERE/edit',
      'Data Import',
      'clearAndReuse',
      'admin@company.com'
    ];

    sheet.getRange(2, 1, 1, exampleRule.length).setValues([exampleRule]);

    // Add data validation for specific columns
    addRuleValidation(sheet);
  }

  return sheet;
}

/**
 * Add data validation to rules sheet
 */
function addRuleValidation(sheet) {
  const lastRow = Math.max(sheet.getLastRow(), 100); // Ensure validation for future rows

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

  // Active column validation
  const activeRange = sheet.getRange(2, RULE_COLUMNS.ACTIVE + 1, lastRow - 1, 1);
  const activeValidation = SpreadsheetApp.newDataValidation()
    .requireValueInList([true, false])
    .build();
  activeRange.setDataValidation(activeValidation);
}

/**
 * Get spreadsheet by ID with error handling
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
 */
function formatTimestamp(date) {
  return Utilities.formatDate(date, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

/**
 * Parse CSV file size estimation
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