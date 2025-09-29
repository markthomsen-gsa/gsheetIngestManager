/**
 * CSV email attachment processing
 * Handles email search, attachment filtering, and CSV processing
 */

/**
 * Process email rule to find and import CSV attachments
 * Returns number of rows processed
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
    for (const thread of emails.slice(0, 10)) {
      const messages = thread.getMessages();

      for (const message of messages) {
        const result = processCsvAttachments(message, rule, sessionId);
        if (result.rowsProcessed > 0) {
          return result; // Return on first successful processing
        }
      }
    }

    logEntry(sessionId, rule.id, 'INFO', 'No matching CSV attachments found');
    return { rowsProcessed: 0 };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR', `Email processing failed: ${error.message}`);
    throw error;
  }
}

/**
 * Process CSV attachments from a single email message
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
    return { rowsProcessed: 0 };
  }

  // Process first matching CSV attachment only
  const attachment = csvAttachments[0];
  logEntry(sessionId, rule.id, 'INFO', `Processing CSV: ${attachment.getName()}`);

  return processCSVWithRetry(attachment, rule, sessionId);
}

/**
 * Core CSV processing function
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
    const destSheet = getDestinationSheet(rule);

    // Apply processing mode
    const rowsWritten = applyCsvToSheet(csvData, destSheet, rule.mode);

    logEntry(sessionId, rule.id, 'SUCCESS',
      `${fileName}: ${rowsWritten} rows imported`);

    return { rowsProcessed: rowsWritten };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR',
      `${fileName}: ${error.message}`);
    throw error;
  }
}

/**
 * Parse CSV with error handling and validation
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
 */
function getDestinationSheet(rule) {
  try {
    const destSpreadsheet = SpreadsheetApp.openById(rule.destination);

    // Use first sheet if no specific sheet name provided
    const sheet = destSpreadsheet.getSheets()[0];

    if (!sheet) {
      throw new Error('Destination spreadsheet has no sheets');
    }

    return sheet;

  } catch (error) {
    throw new Error(`Cannot access destination sheet: ${error.message}`);
  }
}

/**
 * Apply CSV data to sheet based on processing mode
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
 */
function appendData(csvData, sheet) {
  try {
    // Skip header row when appending
    const dataToAppend = csvData.slice(1);

    if (dataToAppend.length === 0) {
      return 0; // No data rows to append
    }

    // Find next empty row
    const lastRow = sheet.getLastRow();
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
 */
function processCSVWithRetry(attachment, rule, sessionId) {
  return executeWithRetry(() => {
    return processCsvAttachment(attachment, rule, sessionId);
  }, RETRY_ATTEMPTS);
}