/**
 * Data push to external sheets
 * Handles pushing data from current sheet to destination sheets
 */

/**
 * Process push rule to send data to external sheet
 * Pushes data from current active sheet to destination sheet
 * @param {Object} rule - Push rule configuration
 * @param {string} rule.id - Rule identifier
 * @param {string} [rule.destination] - Destination sheet ID/URL (empty = current)
 * @param {string} [rule.destinationTab] - Destination tab name
 * @param {string} rule.mode - Processing mode
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result
 * @returns {number} returns.rowsProcessed - Number of rows transferred
 */
function processPushRule(rule, sessionId) {
  const destDisplay = rule.destination || '(current spreadsheet)';
  logEntry(sessionId, rule.id, 'START', `Pushing data to: ${destDisplay}`);

  try {
    // Get current sheet data
    const currentSheet = SpreadsheetApp.getActiveSheet();
    const currentData = currentSheet.getDataRange().getValues();

    if (currentData.length === 0) {
      logEntry(sessionId, rule.id, 'INFO', 'Current sheet is empty');
      return { rowsProcessed: 0 };
    }

    // Get destination sheet
    const destSheet = getDestinationSheet(rule, sessionId);

    // Push data to destination
    const rowsWritten = applyDataToSheet(currentData, destSheet, rule.mode);

    logEntry(sessionId, rule.id, 'SUCCESS',
      `Data push: ${rowsWritten} rows transferred`);

    return { rowsProcessed: rowsWritten };

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR',
      `Data push failed: ${error.message}`);
    throw error;
  }
}