/**
 * Google Sheets data import operations
 * Handles sheet-to-sheet data transfer and processing
 */

/**
 * Process Google Sheets rule for data import
 */
function processSheetRule(rule, sessionId) {
  logEntry(sessionId, rule.id, 'START', `Importing from sheet: ${rule.sourceQuery}`);

  try {
    // Open source spreadsheet
    const sourceSpreadsheet = SpreadsheetApp.openById(rule.sourceQuery);
    const sourceSheet = sourceSpreadsheet.getSheets()[0]; // Use first sheet

    // Get all data from source
    const sourceData = sourceSheet.getDataRange().getValues();

    if (sourceData.length === 0) {
      logEntry(sessionId, rule.id, 'INFO', 'Source sheet is empty');
      return { rowsProcessed: 0 };
    }

    // Get destination sheet
    const destSheet = getDestinationSheet(rule, sessionId);

    // Apply data to destination
    const rowsWritten = applyDataToSheet(sourceData, destSheet, rule.mode);

    logEntry(sessionId, rule.id, 'SUCCESS',
      `Sheet import: ${rowsWritten} rows transferred`);

    return { rowsProcessed: rowsWritten };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR',
      `Sheet import failed: ${error.message}`);
    throw error;
  }
}

/**
 * Apply sheet data with mode handling
 */
function applyDataToSheet(data, sheet, mode) {
  // Validate data size
  if (data.length > MAX_ROWS_PER_FILE) {
    throw new Error(`Source data exceeds maximum rows: ${data.length} > ${MAX_ROWS_PER_FILE}`);
  }

  return applyCsvToSheet(data, sheet, mode);
}