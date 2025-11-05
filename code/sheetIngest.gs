/**
 * Google Sheets data import operations
 * Handles sheet-to-sheet data transfer and processing
 */

/**
 * Resolve source spreadsheet and tab from rule configuration
 * Handles multiple input formats:
 * - Sheet ID only (44 chars)
 * - Full URL without GID
 * - Full URL with GID
 * - Optional Source Tab override
 *
 * @param {Object} rule - Rule configuration
 * @param {string} sessionId - Session ID for logging
 * @returns {Object} Google Sheets Sheet object
 * @throws {Error} If sheet or tab cannot be accessed
 */
function resolveSourceSheet(rule, sessionId) {
  let sheetId;
  let gid = null;

  // Parse source query - could be ID or URL
  const sourceQuery = rule.sourceQuery.trim();

  if (sourceQuery.includes('docs.google.com/spreadsheets')) {
    // URL format - extract ID and GID
    try {
      const parsed = parseSheetUrl(sourceQuery);
      sheetId = parsed.sheetId;
      gid = parsed.gid;

      if (gid) {
        logEntry(sessionId, rule.id, 'INFO', `Detected GID ${gid} from URL`);
      }
    } catch (error) {
      throw new Error(`Failed to parse source URL: ${error.message}`);
    }
  } else {
    // Assume it's a sheet ID
    if (!isValidSheetId(sourceQuery)) {
      throw new Error(`Invalid source sheet ID format: ${sourceQuery}`);
    }
    sheetId = sourceQuery;
  }

  // Open spreadsheet
  let sourceSpreadsheet;
  try {
    sourceSpreadsheet = SpreadsheetApp.openById(sheetId);
  } catch (error) {
    throw new Error(`Cannot access source sheet ${sheetId}: ${error.message}`);
  }

  // Determine which tab to use
  let targetSheet;
  const sourceTabName = rule.sourceTab ? rule.sourceTab.trim() : '';

  if (sourceTabName) {
    // Source Tab specified - takes precedence over GID
    if (gid) {
      logEntry(sessionId, rule.id, 'WARNING',
        `Both GID and Source Tab specified; using Source Tab "${sourceTabName}"`);
    }

    targetSheet = sourceSpreadsheet.getSheetByName(sourceTabName);

    if (!targetSheet) {
      // Provide helpful error with available tabs
      const availableTabs = sourceSpreadsheet.getSheets().map(s => s.getName());
      throw new Error(
        `Tab "${sourceTabName}" not found in source sheet. ` +
        `Available tabs: [${availableTabs.join(', ')}]`
      );
    }

    logEntry(sessionId, rule.id, 'INFO', `Using source tab: ${sourceTabName}`);

  } else if (gid) {
    // GID specified in URL - resolve to tab
    try {
      const allSheets = sourceSpreadsheet.getSheets();
      targetSheet = allSheets.find(sheet => sheet.getSheetId().toString() === gid);

      if (!targetSheet) {
        logEntry(sessionId, rule.id, 'WARNING',
          `GID ${gid} not found; falling back to first sheet`);
        targetSheet = allSheets[0];
      } else {
        logEntry(sessionId, rule.id, 'INFO',
          `Resolved GID ${gid} to tab: ${targetSheet.getName()}`);
      }
    } catch (error) {
      logEntry(sessionId, rule.id, 'WARNING',
        `Failed to resolve GID ${gid}; using first sheet`);
      targetSheet = sourceSpreadsheet.getSheets()[0];
    }

  } else {
    // No tab specified - use first sheet (default behavior)
    targetSheet = sourceSpreadsheet.getSheets()[0];
    logEntry(sessionId, rule.id, 'INFO',
      `No source tab specified; using first sheet: ${targetSheet.getName()}`);
  }

  // Validate we have a sheet
  if (!targetSheet) {
    throw new Error('Source spreadsheet contains no sheets');
  }

  return targetSheet;
}

/**
 * Process Google Sheets rule for data import
 * Imports data from source sheet to destination sheet
 * @param {Object} rule - Sheet rule configuration
 * @param {string} rule.id - Rule identifier
 * @param {string} rule.sourceQuery - Source sheet ID or URL
 * @param {string} [rule.sourceTab] - Source tab name (optional)
 * @param {string} rule.destination - Destination sheet ID/URL
 * @param {string} [rule.destinationTab] - Destination tab name
 * @param {string} rule.mode - Processing mode
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result
 * @returns {number} returns.rowsProcessed - Number of rows transferred
 * @returns {string} returns.sourceSheetName - Name of source sheet
 * @returns {number} returns.sourceSheetGid - GID of source sheet
 */
function processSheetRule(rule, sessionId) {
  logEntry(sessionId, rule.id, 'START', `Importing from sheet: ${rule.sourceQuery}`);

  try {
    // Resolve source sheet and tab
    const sourceSheet = resolveSourceSheet(rule, sessionId);

    // Get all data from source
    const sourceData = sourceSheet.getDataRange().getValues();

    if (sourceData.length === 0) {
      logEntry(sessionId, rule.id, 'INFO', 'Source sheet is empty');
      return {
        rowsProcessed: 0,
        sourceSheetName: sourceSheet.getName(),
        sourceSheetGid: sourceSheet.getSheetId()
      };
    }

    // Get destination sheet
    const destSheet = getDestinationSheet(rule, sessionId);

    // Apply data to destination
    const rowsWritten = applyDataToSheet(sourceData, destSheet, rule.mode);

    logEntry(sessionId, rule.id, 'SUCCESS',
      `Sheet import: ${rowsWritten} rows transferred from tab: ${sourceSheet.getName()}`);

    return {
      rowsProcessed: rowsWritten,
      sourceSheetName: sourceSheet.getName(),
      sourceSheetGid: sourceSheet.getSheetId()
    };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR',
      `Sheet import failed: ${error.message}`);
    throw error;
  }
}

/**
 * Apply sheet data with mode handling
 * Validates data size and applies data using specified mode
 * @param {Array<Array<string>>} data - Sheet data to apply
 * @param {Sheet} sheet - Target Google Sheets object
 * @param {string} mode - Processing mode
 * @returns {number} Number of rows written
 * @throws {Error} If data exceeds size limits
 */
function applyDataToSheet(data, sheet, mode) {
  // Validate data size
  if (data.length > MAX_ROWS_PER_FILE) {
    throw new Error(`Source data exceeds maximum rows: ${data.length} > ${MAX_ROWS_PER_FILE}`);
  }

  return applyCsvToSheet(data, sheet, mode);
}