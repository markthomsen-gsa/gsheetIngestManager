/**
 * Main entry point and orchestration for Data Ingestion System
 * Handles menu creation, user interactions, and overall execution flow
 */

/**
 * Creates the main menu when spreadsheet opens
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('📊 Data Ingest')
    .addItem('🚀 Ingest Data', 'runAll')
    .addItem('⚙️ Initialize System', 'setupSheets')
    .addItem('📋 View Logs', 'navigateToLogs')
    .addItem('📝 View Rules', 'navigateToRules')
    .addToUi();
}

/**
 * Main execution function - processes all active rules
 */
function runAll() {
  const sessionId = generateSessionId();
  const startTime = new Date(); // Track session start time

  // Get spreadsheet information for notifications
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const spreadsheetName = spreadsheet.getName();
  const spreadsheetUrl = spreadsheet.getUrl();

  try {
    // Show start notification
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `🚀 Starting data ingest session ${sessionId}`,
      'Session Started',
      TOAST_DURATION_MS
    );

    // Log session start
    logSessionStart(sessionId);

    // Navigate to logs sheet for real-time monitoring
    try {
      const logsSheet = getSheet('logs');
      logsSheet.activate();

      // Position user near the area where new session logs will appear
      const lastRow = logsSheet.getLastRow();
      if (lastRow > 1) {
        // Set active range to a visible area near where new logs will be added
        // Show the last few entries plus some empty rows below for context
        const startRow = Math.max(lastRow - 5, 2); // Show last 5 entries or start from row 2
        logsSheet.setActiveRange(logsSheet.getRange(startRow, 1, 1, 6));
      } else {
        // If sheet is empty except headers, position at row 2
        logsSheet.setActiveRange(logsSheet.getRange(2, 1, 1, 6));
      }

      // Flush to ensure immediate visual positioning
      SpreadsheetApp.flush();

    } catch (error) {
      // Fail silently - navigation not critical and may not work in headless mode
      console.log(`Navigation to logs failed (likely headless execution): ${error.message}`);
    }

    // Validate all rules before processing
    validateAllRules();

    // Get active rules
    const rules = getActiveRules();

    if (rules.length === 0) {
      SpreadsheetApp.getActiveSpreadsheet().toast(
        'No active rules found',
        'Warning',
        TOAST_DURATION_MS
      );
      return;
    }

    // Process each rule
    let successCount = 0;
    let errorCount = 0;
    let totalRows = 0; // Track total rows processed across all rules

    for (const rule of rules) {
      try {
        const result = processRule(rule, sessionId);
        if (result.success) {
          successCount++;
          // Accumulate rows from successful rule processing
          if (result.rowsProcessed) {
            totalRows += result.rowsProcessed;
          }
        } else {
          errorCount++;
        }
      } catch (error) {
        errorCount++;
        logEntry(sessionId, rule.id, 'ERROR', error.message);
      }
    }

    // Calculate execution time
    const endTime = new Date();
    const executionTimeMs = endTime - startTime;
    const executionTimeSeconds = (executionTimeMs / 1000).toFixed(1);
    const executionTime = `${executionTimeSeconds} seconds`;

    // Log session completion
    logSessionComplete(sessionId, successCount, errorCount);

    // Determine notification type based on results
    let notificationType;
    let message;

    if (errorCount === 0) {
      // All rules succeeded
      notificationType = 'success';
      message = `✅ Session ${sessionId} completed: ${successCount}/${rules.length} rules successful`;
    } else if (successCount === 0) {
      // All rules failed
      notificationType = 'error';
      message = `❌ Session ${sessionId} failed: 0/${rules.length} rules successful`;
    } else {
      // Partial success
      notificationType = 'partial';
      message = `⚠️ Session ${sessionId} partial success: ${successCount}/${rules.length} rules successful`;
    }

    // Show completion notification
    SpreadsheetApp.getActiveSpreadsheet().toast(message, 'Session Complete', TOAST_DURATION_MS);

    // Send email notification if configured
    sendSessionNotification(notificationType, sessionId, {
      ruleCount: rules.length,
      successCount: successCount,
      errorCount: errorCount,
      totalRows: totalRows,
      executionTime: executionTime,
      spreadsheetName: spreadsheetName,
      spreadsheetUrl: spreadsheetUrl
    });

  } catch (error) {
    // Calculate execution time even for system errors
    const endTime = new Date();
    const executionTimeMs = endTime - startTime;
    const executionTimeSeconds = (executionTimeMs / 1000).toFixed(1);
    const executionTime = `${executionTimeSeconds} seconds`;

    // Handle system-level errors
    logEntry(sessionId, 'SYSTEM', 'ERROR', error.message);

    SpreadsheetApp.getActiveSpreadsheet().toast(
      `❌ Session ${sessionId} failed: ${error.message}`,
      'Session Failed',
      TOAST_LONG_DURATION_MS
    );

    sendSessionNotification('error', sessionId, {
      errorMessage: error.message,
      ruleCount: 0,
      successCount: 0,
      errorCount: 1,
      totalRows: 0,
      executionTime: executionTime,
      spreadsheetName: spreadsheetName,
      spreadsheetUrl: spreadsheetUrl
    });
  }
}

/**
 * Process a single rule based on its method
 */
function processRule(rule, sessionId) {
  logEntry(sessionId, rule.id, 'START', `Processing rule: ${rule.method}`);

  try {
    let result;

    switch (rule.method) {
      case 'email':
        result = processEmailRule(rule, sessionId);
        break;
      case 'gSheet':
        result = processSheetRule(rule, sessionId);
        break;
      case 'push':
        result = processPushRule(rule, sessionId);
        break;
      default:
        throw new Error(`Unknown method: ${rule.method}`);
    }

    if (result.rowsProcessed > 0) {
      logEntry(sessionId, rule.id, 'SUCCESS',
        `Processed ${result.rowsProcessed} rows`);
      return { success: true, rowsProcessed: result.rowsProcessed };
    } else {
      logEntry(sessionId, rule.id, 'INFO', 'No data to process');
      return { success: true, rowsProcessed: 0 };
    }

  } catch (error) {
    logEntry(sessionId, rule.id, 'ERROR', error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Get all active rules from configuration sheet
 */
function getActiveRules() {
  const rulesSheet = getSheet('rules');
  const data = rulesSheet.getDataRange().getValues();
  const headers = data[0];
  const rules = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    if (row[1] === true) { // Active column
      rules.push({
        id: row[0],
        active: row[1],
        method: row[2],
        sourceQuery: row[3],
        attachmentPattern: row[4],
        destination: row[5],
        destinationTab: row[6],
        mode: row[7],
        emailRecipients: row[8]
      });
    }
  }

  return rules;
}

/**
 * Initialize system sheets
 */
function setupSheets() {
  try {
    createRulesSheet();
    createLogsSheet();

    SpreadsheetApp.getActiveSpreadsheet().toast(
      'System sheets created successfully',
      'Setup Complete',
      TOAST_DURATION_MS
    );

  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Setup failed: ${error.message}`,
      'Setup Error',
      TOAST_LONG_DURATION_MS
    );
  }
}

/**
 * Navigate to logs sheet
 */
function navigateToLogs() {
  try {
    const sheet = getSheet('logs');
    sheet.activate();

    // Position user at the most recent activity
    const lastRow = sheet.getLastRow();
    if (lastRow > 1) {
      // Show recent entries for context
      const startRow = Math.max(lastRow - 10, 2); // Show last 10 entries
      sheet.setActiveRange(sheet.getRange(startRow, 1, 1, 6));
    } else {
      // Position at the first data row if sheet is empty
      sheet.setActiveRange(sheet.getRange(2, 1, 1, 6));
    }

    SpreadsheetApp.flush();

    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Navigated to logs sheet',
      'Navigation',
      3000
    );
  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      error.message,
      'Error',
      TOAST_LONG_DURATION_MS
    );
  }
}

/**
 * Navigate to rules configuration sheet
 */
function navigateToRules() {
  try {
    const sheet = getSheet('rules');
    sheet.activate();
    SpreadsheetApp.getActiveSpreadsheet().toast(
      'Navigated to rules configuration',
      'Navigation',
      3000
    );
  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      error.message,
      'Error',
      TOAST_LONG_DURATION_MS
    );
  }
}