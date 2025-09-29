/**
 * Data push to external sheets
 * Handles pushing data from current sheet to destination sheets
 */

/**
 * Process push rule to send data to external sheet
 */
function processPushRule(rule, sessionId) {
  logEntry(sessionId, rule.id, 'START', `Pushing data to: ${rule.destination}`);

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