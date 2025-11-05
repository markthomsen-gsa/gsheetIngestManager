/**
 * Main entry point and orchestration for Data Ingestion System
 * Handles menu creation, user interactions, and overall execution flow
 */

/**
 * Creates the main menu when spreadsheet opens
 * Automatically called by Google Apps Script when the spreadsheet is opened
 * @function onOpen
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu('📊 Data Ingest')
    .addItem('🚀 Ingest Data', 'runAll')
    .addSubMenu(
      ui.createMenu('🛠️ Maintenance')
        .addItem('⚙️ Initialize System', 'setupSheets')
        .addItem('📋 View Logs', 'navigateToLogs')
        .addItem('🗑️ Clear Logs', 'clearLogs')
        .addItem('📝 View Rules', 'navigateToRules')
    )
    .addToUi();
}

/**
 * Main execution function - processes all active rules
 * Orchestrates the entire data ingestion process including validation, processing, and notifications
 * @function runAll
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

    // Send session start notification if configured
    sendSessionNotification('start', sessionId, {
      ruleCount: rules.length,
      spreadsheetName: spreadsheetName,
      spreadsheetUrl: spreadsheetUrl,
      logsSheetUrl: getLogsSheetUrl(),
      rulesSheetUrl: getRulesSheetUrl(),
      rulesScheduled: formatRulesForStart(rules)
    });

    // Process each rule
    let successCount = 0;
    let errorCount = 0;
    let totalRows = 0; // Track total rows processed across all rules
    const ruleResults = []; // Store detailed results for each rule

    for (const rule of rules) {
      try {
        const result = processRule(rule, sessionId);
        if (result.success) {
          successCount++;
          // Accumulate rows from successful rule processing
          if (result.rowsProcessed) {
            totalRows += result.rowsProcessed;
          }

          // Store rule result with all details
          ruleResults.push({
            rule: rule,
            result: result,
            status: 'success'
          });
        } else {
          errorCount++;
          ruleResults.push({
            rule: rule,
            result: result,
            status: 'error'
          });
        }
      } catch (error) {
        errorCount++;
        logEntry(sessionId, rule.id, 'ERROR', error.message);
        ruleResults.push({
          rule: rule,
          result: { error: error.message },
          status: 'error'
        });
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
      spreadsheetUrl: spreadsheetUrl,
      logsSheetUrl: getLogsSheetUrl(),
      rulesSheetUrl: getRulesSheetUrl(),
      rulesExecuted: formatRulesResults(ruleResults)
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
      spreadsheetUrl: spreadsheetUrl,
      logsSheetUrl: getLogsSheetUrl(),
      rulesSheetUrl: getRulesSheetUrl(),
      rulesExecuted: 'System error occurred before rule processing could begin.'
    });
  }
}

/**
 * Process a single rule based on its method
 * Routes rule processing to appropriate handler based on method type
 * @param {Object} rule - Rule configuration object
 * @param {string} rule.id - Unique rule identifier
 * @param {string} rule.method - Processing method ('email', 'gSheet', 'push')
 * @param {string} sessionId - Session identifier for logging
 * @returns {Object} Processing result with success status and row count
 * @returns {boolean} returns.success - Whether processing succeeded
 * @returns {number} returns.rowsProcessed - Number of rows processed
 * @returns {Object} [returns.senderInfo] - Email sender information (email method only)
 * @returns {string} [returns.gmailSearchUrl] - Gmail search URL (email method only)
 * @returns {string} [returns.filename] - Processed filename (email method only)
 * @returns {string} [returns.error] - Error message if processing failed
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

      // Return all result data including email sender info and search URLs
      return {
        success: true,
        rowsProcessed: result.rowsProcessed,
        senderInfo: result.senderInfo,
        gmailSearchUrl: result.gmailSearchUrl,
        filename: result.filename
      };
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
 * Reads rules from the rules sheet and filters for active ones
 * @returns {Array<Object>} Array of active rule objects
 * @returns {string} returns[].id - Rule identifier
 * @returns {boolean} returns[].active - Whether rule is active
 * @returns {string} returns[].method - Processing method
 * @returns {string} returns[].sourceQuery - Source query/URL
 * @returns {string} [returns[].attachmentPattern] - Attachment pattern regex
 * @returns {string} [returns[].sourceTab] - Source tab name
 * @returns {string} returns[].destination - Destination sheet ID/URL
 * @returns {string} [returns[].destinationTab] - Destination tab name
 * @returns {string} returns[].mode - Processing mode
 * @returns {string} [returns[].emailRecipients] - Email notification recipients
 */
function getActiveRules() {
  const rulesSheet = getSheet('rules');
  const data = rulesSheet.getDataRange().getValues();

  if (data.length < 2) {
    return []; // No data rows
  }

  const headers = data[0];
  const columnMap = detectColumnPositions(headers);
  const rules = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const rule = parseRuleFromRow(row, columnMap);

    // Check if rule is active (handle missing active column gracefully)
    if (rule.active === true) {
      rules.push(rule);
    }
  }

  return rules;
}

/**
 * Initialize system sheets
 * Creates the rules and logs sheets with proper headers and formatting
 * @function setupSheets
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
 * Switches to logs sheet and positions cursor for monitoring
 * @function navigateToLogs
 */
function navigateToLogs() {
  try {
    const sheet = getSheet('logs');
    sheet.activate();

    // Jump to the last row with data
    const lastRow = sheet.getLastRow();
    if (lastRow > 1) {
      sheet.setActiveRange(sheet.getRange(lastRow + 10, 1, 1, 6));
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
 * Switches to rules sheet for configuration management
 * @function navigateToRules
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