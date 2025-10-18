/**
 * Logging and session management
 * Handles all logging operations with session correlation
 */

/**
 * Generate unique session ID
 */
function generateSessionId() {
  const timestamp = new Date().getTime();
  const random = Math.floor(Math.random() * 1000);
  return `S${timestamp.toString().slice(-8)}${random.toString().padStart(3, '0')}`;
}

/**
 * Log session start
 */
function logSessionStart(sessionId) {
  const timestamp = new Date();
  logEntry(sessionId, 'SESSION', LOG_STATUS.START, 'Data ingest session started');
}

/**
 * Log session completion
 */
function logSessionComplete(sessionId, successCount, errorCount) {
  const message = `Session completed: ${successCount} successful, ${errorCount} failed`;
  logEntry(sessionId, 'SESSION', LOG_STATUS.SUCCESS, message);
}

/**
 * Log entry to logs sheet
 */
function logEntry(sessionId, ruleId, status, message, rowsProcessed = 0) {
  try {
    const logsSheet = getSheet('logs');
    const timestamp = new Date();

    logsSheet.appendRow([
      sessionId,
      timestamp,
      ruleId,
      status,
      message,
      rowsProcessed
    ]);

    // Auto-scroll to the latest entry for real-time monitoring
    try {
      const lastRow = logsSheet.getLastRow();
      const activeSheet = SpreadsheetApp.getActiveSheet();

      // Only auto-scroll if we're currently on the logs sheet
      if (activeSheet && activeSheet.getName() === logsSheet.getName()) {
        // Set active range to the newly added row to ensure it's visible
        logsSheet.setActiveRange(logsSheet.getRange(lastRow, 1, 1, 6));

        // Flush pending spreadsheet changes to ensure immediate visual update
        SpreadsheetApp.flush();
      }
    } catch (scrollError) {
      // Auto-scroll is nice-to-have, don't fail logging if it doesn't work
      console.log(`Auto-scroll failed (non-critical): ${scrollError.message}`);
    }

    // Also log to console for debugging
    console.log(`[${sessionId}] ${ruleId}: ${status} - ${message}`);

  } catch (error) {
    // Fallback to console if sheet logging fails
    console.error(`Logging failed: ${error.message}`);
    console.log(`[${sessionId}] ${ruleId}: ${status} - ${message}`);
  }
}

/**
 * Create logs sheet with headers
 */
function createLogsSheet() {
  const sheet = createSheet('logs');

  // Set up headers if sheet is empty
  if (sheet.getLastRow() === 0) {
    const headers = [
      'Session ID',
      'Timestamp',
      'Rule ID',
      'Status',
      'Message',
      'Rows Processed'
    ];

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#E8F0FE');
    
    // Freeze the top row (header row)
    sheet.setFrozenRows(1);
  }

  return sheet;
}

/**
 * Get logs for specific session
 */
function getSessionLogs(sessionId) {
  const logsSheet = getSheet('logs');
  const data = logsSheet.getDataRange().getValues();
  const logs = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (row[LOG_COLUMNS.SESSION_ID] === sessionId) {
      logs.push({
        sessionId: row[LOG_COLUMNS.SESSION_ID],
        timestamp: row[LOG_COLUMNS.TIMESTAMP],
        ruleId: row[LOG_COLUMNS.RULE_ID],
        status: row[LOG_COLUMNS.STATUS],
        message: row[LOG_COLUMNS.MESSAGE],
        rowsProcessed: row[LOG_COLUMNS.ROWS_PROCESSED]
      });
    }
  }

  return logs;
}

/**
 * Clean up old log entries
 */
function cleanupLogs() {
  try {
    const logsSheet = getSheet('logs');
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - LOG_RETENTION_DAYS);

    const data = logsSheet.getDataRange().getValues();
    const rowsToDelete = [];

    // Find rows older than cutoff date (skip header row)
    for (let i = 1; i < data.length; i++) {
      const timestamp = new Date(data[i][LOG_COLUMNS.TIMESTAMP]);
      if (timestamp < cutoffDate) {
        rowsToDelete.push(i + 1); // +1 for 1-based row numbering
      }
    }

    // Delete rows in reverse order to maintain correct row numbers
    for (let i = rowsToDelete.length - 1; i >= 0; i--) {
      logsSheet.deleteRow(rowsToDelete[i]);
    }

    console.log(`Cleaned up ${rowsToDelete.length} old log entries`);
    return rowsToDelete.length;

  } catch (error) {
    console.error(`Log cleanup failed: ${error.message}`);
    throw error;
  }
}

/**
 * Get recent session summaries
 */
function getRecentSessions(limit = 10) {
  const logsSheet = getSheet('logs');
  const data = logsSheet.getDataRange().getValues();
  const sessions = new Map();

  // Process logs to build session summaries
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const sessionId = row[LOG_COLUMNS.SESSION_ID];

    if (!sessions.has(sessionId)) {
      sessions.set(sessionId, {
        sessionId: sessionId,
        startTime: row[LOG_COLUMNS.TIMESTAMP],
        endTime: null,
        status: 'IN_PROGRESS',
        ruleCount: 0,
        successCount: 0,
        errorCount: 0
      });
    }

    const session = sessions.get(sessionId);
    const status = row[LOG_COLUMNS.STATUS];

    // Update session based on log entry
    if (status === LOG_STATUS.START && row[LOG_COLUMNS.RULE_ID] !== 'SESSION') {
      session.ruleCount++;
    } else if (status === LOG_STATUS.SUCCESS && row[LOG_COLUMNS.RULE_ID] !== 'SESSION') {
      session.successCount++;
    } else if (status === LOG_STATUS.ERROR) {
      session.errorCount++;
    }

    // Check for session completion
    if (row[LOG_COLUMNS.RULE_ID] === 'SESSION' && status === LOG_STATUS.SUCCESS) {
      session.endTime = row[LOG_COLUMNS.TIMESTAMP];
      session.status = 'COMPLETED';
    }
  }

  // Convert to array and sort by start time (most recent first)
  return Array.from(sessions.values())
    .sort((a, b) => new Date(b.startTime) - new Date(a.startTime))
    .slice(0, limit);
}

/**
 * Clear all log entries (keeping headers)
 */
function clearLogs() {
  try {
    const logsSheet = getSheet('logs');
    const lastRow = logsSheet.getLastRow();
    
    // If there are no data rows (only headers), nothing to clear
    if (lastRow <= 1) {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        'No logs to clear',
        'Clear Logs',
        TOAST_DURATION_MS
      );
      return;
    }

    // Clear all data rows (keep header row)
    const dataRange = logsSheet.getRange(2, 1, lastRow - 1, 6);
    dataRange.clearContent();

    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Cleared ${lastRow - 1} log entries`,
      'Clear Logs',
      TOAST_DURATION_MS
    );

    // Position at the header row
    logsSheet.setActiveRange(logsSheet.getRange(1, 1, 1, 6));
    SpreadsheetApp.flush();

  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Failed to clear logs: ${error.message}`,
      'Clear Logs Error',
      TOAST_LONG_DURATION_MS
    );
  }
}