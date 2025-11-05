/**
 * CSV email attachment processing
 * Handles email search, attachment filtering, and CSV processing
 */

/**
 * Process email rule to find and import CSV attachments
 * Searches Gmail for emails matching the rule's query and processes CSV attachments
 * @param {Object} rule - Email rule configuration
 * @param {string} rule.id - Rule identifier
 * @param {string} rule.sourceQuery - Gmail search query
 * @param {string} rule.attachmentPattern - Regex pattern for attachment filtering
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result
 * @returns {number} returns.rowsProcessed - Number of rows processed from CSV
 * @returns {Object} [returns.senderInfo] - Email sender information
 * @returns {string} [returns.gmailSearchUrl] - Gmail search URL
 * @returns {string} [returns.filename] - Processed CSV filename
 */
function processEmailRule(rule, sessionId) {
  logEntry(sessionId, rule.id, 'START', `Searching emails: ${rule.sourceQuery}`);

  try {
    // Search Gmail for matching emails
    const emails = GmailApp.search(rule.sourceQuery);

    if (emails.length === 0) {
      logEntry(sessionId, rule.id, 'INFO', 'No matching emails found');
      return { rowsProcessed: 0 };
    }

    // Process recent emails (limit to prevent timeout)
    for (const thread of emails.slice(0, 5)) {
      const messages = thread.getMessages();

      // Log how many messages are in this thread
      logEntry(sessionId, rule.id, 'INFO', `Thread ID: ${thread.getId()} contains ${messages.length} message(s)`);

      for (const [i, message] of messages.entries()) {
        const sender = message.getFrom();
        const subject = message.getSubject();
        const date = message.getDate();
        // Convert date to EST (Eastern Standard Time / America/New_York)
        const estDate = Utilities.formatDate(date, 'America/New_York', 'yyyy-MM-dd HH:mm:ss');
        // Construct the Gmail URL for this message
        const threadId = message.getThread().getId();
        const messageId = message.getId();
        const gmailUrl = `https://mail.google.com/mail/u/0/#inbox/${threadId}/${messageId}`;

        logEntry(
          sessionId,
          rule.id,
          'INFO',
          `Message #${i + 1} in thread:\nSender: ${sender}\nDate Received (EST): ${estDate}\nSubject: ${subject}\nURL: ${gmailUrl}`
        );
      }

      for (const message of messages) {
        const result = processCsvAttachments(message, rule, sessionId);
        if (result.rowsProcessed > 0) {
          // Log sender information for successful processing
          if (result.senderInfo) {
            logEntry(sessionId, rule.id, 'INFO',
              `Processed email from: ${result.senderInfo.name} <${result.senderInfo.email}>`);
          }
          return result; // Return on first successful processing
        }
      }
    }

    logEntry(sessionId, rule.id, 'INFO', 'No matching CSV attachments found');
    return { rowsProcessed: 0, columnsProcessed: 0 };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR', `Email processing failed: ${error.message}`);
    throw error;
  }
}

/**
 * Process CSV attachments from a single email message
 * Filters attachments by pattern and processes the first matching CSV
 * @param {GmailMessage} message - Gmail message object
 * @param {Object} rule - Rule configuration
 * @param {string} rule.id - Rule identifier
 * @param {string} rule.attachmentPattern - Regex pattern for attachment filtering
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result
 * @returns {number} returns.rowsProcessed - Number of rows processed
 * @returns {Object} [returns.senderInfo] - Email sender information
 * @returns {string} [returns.gmailSearchUrl] - Gmail search URL
 * @returns {string} [returns.filename] - Processed filename
 */
function processCsvAttachments(message, rule, sessionId) {
  const attachments = message.getAttachments();

  // Filter for CSV files matching the pattern
  const csvAttachments = attachments.filter(attachment => {
    const fileName = attachment.getName().toLowerCase();

    // Must be CSV file
    if (!fileName.endsWith('.csv')) {
      return false;
    }

    // Must match attachment pattern if specified
    if (rule.attachmentPattern) {
      return new RegExp(rule.attachmentPattern).test(attachment.getName());
    }

    return true;
  });

  if (csvAttachments.length === 0) {
    return { rowsProcessed: 0, columnsProcessed: 0 };
  }

  // Process first matching CSV attachment only
  const attachment = csvAttachments[0];
  logEntry(sessionId, rule.id, 'INFO', `Processing CSV: ${attachment.getName()}`);

  // Extract sender information from the message
  const senderInfo = extractSenderInfo(message);

  // Process CSV and add sender info to result
  const result = processCSVWithRetry(attachment, rule, sessionId);

  // Add sender info and search URL to result
  if (result.rowsProcessed > 0) {
    result.senderInfo = senderInfo;
    result.gmailSearchUrl = createGmailSearchUrl(rule.sourceQuery);
    result.filename = attachment.getName();
  }

  return result;
}

/**
 * Core CSV processing function
 * Handles CSV parsing, validation, and sheet operations
 * @param {GmailAttachment} attachment - CSV attachment from Gmail
 * @param {Object} rule - Rule configuration
 * @param {string} rule.id - Rule identifier
 * @param {string} rule.destination - Destination sheet ID/URL
 * @param {string} rule.destinationTab - Destination tab name
 * @param {string} rule.mode - Processing mode
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result
 * @returns {number} returns.rowsProcessed - Number of rows written to sheet
 * @throws {Error} If CSV parsing or sheet operations fail
 */
function processCsvAttachment(attachment, rule, sessionId) {
  const fileName = attachment.getName();

  try {
    // Get CSV data as string
    const csvString = attachment.getDataAsString();

    if (!csvString || csvString.trim().length === 0) {
      throw new Error('CSV file is empty');
    }

    // Parse CSV using Google's built-in parser
    const csvData = parseCSVData(csvString);

    // Validate CSV size constraints
    validateCSVSize(csvData);

    // Get destination sheet
    const destSheet = getDestinationSheet(rule, sessionId);

    // Apply processing mode
    const rowsWritten = applyCsvToSheet(csvData, destSheet, rule.mode);
    
    // Calculate column count from CSV data
    const columnsProcessed = csvData.length > 0 ? csvData[0].length : 0;

    logEntry(sessionId, rule.id, 'SUCCESS',
      `${fileName}: ${rowsWritten} rows imported`);

    return { 
      rowsProcessed: rowsWritten,
      columnsProcessed: columnsProcessed
    };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR',
      `${fileName}: ${error.message}`);
    throw error;
  }
}

/**
 * Parse CSV with error handling and validation
 * Uses Google's optimized CSV parser with comprehensive error handling
 * @param {string} csvString - Raw CSV content as string
 * @returns {Array<Array<string>>} Parsed CSV data as 2D array
 * @throws {Error} If CSV parsing fails or results in invalid data
 */
function parseCSVData(csvString) {
  try {
    // Use Google's optimized CSV parser
    const csvData = Utilities.parseCsv(csvString);

    if (!Array.isArray(csvData) || csvData.length === 0) {
      throw new Error('CSV parsing resulted in no data');
    }

    // Validate data structure
    validateCSVStructure(csvData);

    return csvData;

  } catch (error) {
    if (error.message.includes('CSV')) {
      throw error; // Re-throw CSV-specific errors
    } else {
      throw new Error(`CSV parsing failed: ${error.message}`);
    }
  }
}

/**
 * Validate CSV data structure and constraints
 * Checks row count limits, column consistency, and minimum requirements
 * @param {Array<Array<string>>} csvData - Parsed CSV data
 * @throws {Error} If CSV structure is invalid or exceeds limits
 */
function validateCSVStructure(csvData) {
  // Check row count limit
  if (csvData.length > MAX_ROWS_PER_FILE) {
    throw new Error(
      `CSV file exceeds maximum rows: ${csvData.length} > ${MAX_ROWS_PER_FILE}`
    );
  }

  // Check for consistent column count
  if (csvData.length > 1) {
    const headerColumnCount = csvData[0].length;
    const inconsistentRows = [];

    for (let i = 1; i < csvData.length; i++) {
      if (csvData[i].length !== headerColumnCount) {
        inconsistentRows.push(i + 1); // 1-based row number
      }
    }

    if (inconsistentRows.length > 0) {
      throw new Error(
        `CSV has inconsistent column counts. Header: ${headerColumnCount} columns. ` +
        `Inconsistent rows: ${inconsistentRows.slice(0, 5).join(', ')}` +
        (inconsistentRows.length > 5 ? ` (and ${inconsistentRows.length - 5} more)` : '')
      );
    }
  }

  // Check for minimum data requirements
  if (csvData.length < 2) {
    throw new Error('CSV file must contain at least header and one data row');
  }

  if (csvData[0].length === 0) {
    throw new Error('CSV file must contain at least one column');
  }
}

/**
 * Get destination sheet with error handling
 * Supports both URLs and IDs, and specific tab names
 * Auto-creates tabs if they don't exist
 * @param {Object} rule - Rule configuration
 * @param {string} rule.destination - Destination sheet ID/URL
 * @param {string} rule.destinationTab - Destination tab name
 * @param {string} [sessionId] - Session identifier for logging
 * @returns {Sheet} Google Sheets Sheet object
 * @throws {Error} If sheet cannot be accessed or created
 */
function getDestinationSheet(rule, sessionId = null) {
  try {
    // Parse destination to get Sheet ID (handles URLs and IDs)
    const sheetId = parseDestination(rule.destination);
    const destSpreadsheet = SpreadsheetApp.openById(sheetId);

    // Get or create the specified tab using helper function
    return getOrCreateSheet(destSpreadsheet, rule.destinationTab, sessionId, rule.id);

  } catch (error) {
    throw new Error(`Cannot access destination sheet: ${error.message}`);
  }
}

/**
 * Apply CSV data to sheet based on processing mode
 * Routes data application to appropriate mode handler
 * @param {Array<Array<string>>} csvData - CSV data to apply
 * @param {Sheet} sheet - Target Google Sheets object
 * @param {string} mode - Processing mode ('clearAndReuse', 'append', 'recreate')
 * @returns {number} Number of rows written to sheet
 * @throws {Error} If mode is invalid or sheet operations fail
 */
function applyCsvToSheet(csvData, sheet, mode) {
  switch (mode.toLowerCase()) {
    case 'clearandreuse':
      return clearAndReuse(csvData, sheet);

    case 'append':
      return appendData(csvData, sheet);

    case 'recreate':
      return recreateSheet(csvData, sheet);

    default:
      throw new Error(`Unknown processing mode: ${mode}`);
  }
}

/**
 * Clear sheet and write new data
 * Clears all existing content and writes CSV data starting from A1
 * @param {Array<Array<string>>} csvData - CSV data to write
 * @param {Sheet} sheet - Target Google Sheets object
 * @returns {number} Number of rows written
 * @throws {Error} If sheet operations fail
 */
function clearAndReuse(csvData, sheet) {
  try {
    // Clear existing content
    sheet.clear();

    // Write CSV data starting from A1
    const range = sheet.getRange(1, 1, csvData.length, csvData[0].length);
    range.setValues(csvData);

    return csvData.length;

  } catch (error) {
    throw new Error(`Clear and reuse failed: ${error.message}`);
  }
}

/**
 * Append data to existing sheet
 * Appends data rows while preserving existing content and headers
 * @param {Array<Array<string>>} csvData - CSV data to append
 * @param {Sheet} sheet - Target Google Sheets object
 * @returns {number} Number of data rows appended (excludes header)
 * @throws {Error} If sheet operations fail
 */
function appendData(csvData, sheet) {
  try {
    const lastRow = sheet.getLastRow();

    // If sheet is empty (new tab or cleared), write all data including headers
    if (lastRow === 0) {
      const range = sheet.getRange(1, 1, csvData.length, csvData[0].length);
      range.setValues(csvData);
      return csvData.length;
    }

    // Skip header row when appending to existing data
    const dataToAppend = csvData.slice(1);

    if (dataToAppend.length === 0) {
      return 0; // No data rows to append
    }

    // Find next empty row
    const startRow = lastRow + 1;

    // Write data
    const range = sheet.getRange(startRow, 1, dataToAppend.length, dataToAppend[0].length);
    range.setValues(dataToAppend);

    return dataToAppend.length;

  } catch (error) {
    throw new Error(`Append data failed: ${error.message}`);
  }
}

/**
 * Recreate sheet with new data
 * Deletes existing sheet and creates new one with same name
 * @param {Array<Array<string>>} csvData - CSV data to write
 * @param {Sheet} sheet - Target Google Sheets object
 * @returns {number} Number of rows written
 * @throws {Error} If sheet operations fail
 */
function recreateSheet(csvData, sheet) {
  try {
    const spreadsheet = sheet.getParent();
    const sheetName = sheet.getName();

    // Delete existing sheet
    spreadsheet.deleteSheet(sheet);

    // Create new sheet with same name
    const newSheet = spreadsheet.insertSheet(sheetName);

    // Write CSV data
    const range = newSheet.getRange(1, 1, csvData.length, csvData[0].length);
    range.setValues(csvData);

    return csvData.length;

  } catch (error) {
    throw new Error(`Recreate sheet failed: ${error.message}`);
  }
}

/**
 * CSV processing with retry logic
 * Wraps CSV processing with automatic retry for transient failures
 * @param {GmailAttachment} attachment - CSV attachment
 * @param {Object} rule - Rule configuration
 * @param {string} sessionId - Session identifier
 * @returns {Object} Processing result with retry handling
 */
function processCSVWithRetry(attachment, rule, sessionId) {
  return executeWithRetry(() => {
    return processCsvAttachment(attachment, rule, sessionId);
  }, RETRY_ATTEMPTS);
}

/**
 * Extract sender information from Gmail message
 * Parses sender name and email from Gmail message headers
 * @param {GmailMessage} message - Gmail message object
 * @returns {Object} Sender information
 * @returns {string} returns.full - Full sender string from Gmail
 * @returns {string} returns.email - Extracted email address
 * @returns {string} returns.name - Extracted display name
 * @returns {string} returns.subject - Message subject
 * @returns {Date} returns.date - Message date
 */
function extractSenderInfo(message) {
  try {
    const fromHeader = message.getFrom(); // Format: "Display Name <email@domain.com>" or "email@domain.com"

    // Extract email address using regex
    const emailMatch = fromHeader.match(/[^@<\s]+@[^@\s>]+/);
    const emailAddress = emailMatch ? emailMatch[0] : '';

    // Extract display name (text before < or the whole string if no < found)
    let displayName = '';
    if (fromHeader.includes('<')) {
      displayName = fromHeader.split('<')[0].trim();
      // Remove quotes if present
      displayName = displayName.replace(/^["']|["']$/g, '');
    } else {
      // If no < found, assume it's just an email address
      displayName = emailAddress;
    }

    return {
      full: fromHeader,
      email: emailAddress,
      name: displayName || emailAddress,
      subject: message.getSubject(),
      date: message.getDate()
    };

  } catch (error) {
    console.error('Error extracting sender info:', error.message);
    return {
      full: 'Unknown Sender',
      email: 'unknown@unknown.com',
      name: 'Unknown',
      subject: 'Unknown Subject',
      date: new Date()
    };
  }
}

/**
 * Create clickable Gmail search URL from search query
 * Generates URL-encoded Gmail search link for easy access
 * @param {string} searchQuery - Gmail search query
 * @returns {string} Gmail search URL
 */
function createGmailSearchUrl(searchQuery) {
  try {
    // URL encode the search query
    const encodedQuery = encodeURIComponent(searchQuery);

    // Gmail search URL format
    return `https://mail.google.com/mail/u/0/#search/${encodedQuery}`;

  } catch (error) {
    console.error('Error creating Gmail search URL:', error.message);
    return 'https://mail.google.com/mail/u/0/'; // Fallback to Gmail inbox
  }
}