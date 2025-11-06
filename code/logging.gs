/**
 * Logging and session management
 * Handles all logging operations with session correlation
 */

/**
 * Generate unique session ID
 * Creates timestamp-based session identifier with random suffix
 * @returns {string} Unique session ID in format S-{timestamp}-{random}
 */
function generateSessionId() {
  const epoch = Date.now();
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let randomCode = '';
  for (let i = 0; i < 4; i++) {
    randomCode += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return `S-${epoch}-${randomCode}`;
}

/**
 * Log session start
 * Records session start event in logs
 * @param {string} sessionId - Session identifier
 */
function logSessionStart(sessionId) {
  logEntryInternal(sessionId, 'SESSION', LOG_LEVEL.START, LOG_STATUS.START, 'Data ingest session started');
}

/**
 * Log session completion
 * Records session completion with success/error counts
 * @param {string} sessionId - Session identifier
 * @param {number} successCount - Number of successful rules
 * @param {number} errorCount - Number of failed rules
 */
function logSessionComplete(sessionId, successCount, errorCount) {
  const message = `Session completed: ${successCount} successful, ${errorCount} failed`;
  logEntryInternal(sessionId, 'SESSION', LOG_LEVEL.SUCCESS, LOG_STATUS.SUCCESS, message);
}

/**
 * Check if log level should be logged based on configuration
 * @param {string} level - Log level to check
 * @returns {boolean} True if log should be written
 */
function shouldLog(level) {
  const levelHierarchy = {
    TRACE: 0,
    DEBUG: 1,
    INFO: 2,
    WARNING: 3,
    ERROR: 4,
    FATAL: 5
  };
  
  // START and SUCCESS are always logged
  if (level === LOG_LEVEL.START || level === LOG_LEVEL.SUCCESS) {
    return true;
  }
  
  const minLevel = levelHierarchy[LOGGING_CONFIG.MIN_LOG_LEVEL] || 2;
  const currentLevel = levelHierarchy[level] || 2;
  
  // Check specific flags
  if (level === LOG_LEVEL.DEBUG && !LOGGING_CONFIG.ENABLE_DEBUG_LOGS) {
    return false;
  }
  if (level === LOG_LEVEL.TRACE && !LOGGING_CONFIG.ENABLE_TRACE_LOGS) {
    return false;
  }
  
  return currentLevel >= minLevel;
}

/**
 * Categorize error and extract error code and type
 * @param {Error} error - Error object
 * @returns {Object} { code: string, type: string }
 */
function categorizeError(error) {
  const errorMessage = error.message || '';
  const errorStack = error.stack || '';
  const fullError = (errorMessage + ' ' + errorStack).toLowerCase();
  
  // Validation errors
  if (fullError.includes('validation') || fullError.includes('invalid') || 
      fullError.includes('required') || fullError.includes('format')) {
    if (fullError.includes('rule id')) return { code: ERROR_CODES.VALIDATION_RULE_ID_MISSING, type: ERROR_TYPES.VALIDATION };
    if (fullError.includes('method')) return { code: ERROR_CODES.VALIDATION_METHOD_INVALID, type: ERROR_TYPES.VALIDATION };
    if (fullError.includes('sheet id') || fullError.includes('44 character')) return { code: ERROR_CODES.VALIDATION_SHEET_ID_INVALID, type: ERROR_TYPES.VALIDATION };
    if (fullError.includes('email')) return { code: ERROR_CODES.VALIDATION_EMAIL_INVALID, type: ERROR_TYPES.VALIDATION };
    if (fullError.includes('regex') || fullError.includes('pattern')) return { code: ERROR_CODES.VALIDATION_REGEX_INVALID, type: ERROR_TYPES.VALIDATION };
    if (fullError.includes('mode')) return { code: ERROR_CODES.VALIDATION_MODE_INVALID, type: ERROR_TYPES.VALIDATION };
    return { code: ERROR_CODES.VALIDATION_RULE_ID_MISSING, type: ERROR_TYPES.VALIDATION };
  }
  
  // Source errors
  if (fullError.includes('source') || fullError.includes('cannot access') || 
      fullError.includes('not found') || fullError.includes('access denied')) {
    if (fullError.includes('tab') && fullError.includes('not found')) return { code: ERROR_CODES.SOURCE_TAB_NOT_FOUND, type: ERROR_TYPES.SOURCE };
    if (fullError.includes('email') && fullError.includes('not found')) return { code: ERROR_CODES.SOURCE_EMAIL_NOT_FOUND, type: ERROR_TYPES.SOURCE };
    if (fullError.includes('attachment') && fullError.includes('not found')) return { code: ERROR_CODES.SOURCE_ATTACHMENT_NOT_FOUND, type: ERROR_TYPES.SOURCE };
    if (fullError.includes('access denied') || fullError.includes('permission')) return { code: ERROR_CODES.SOURCE_SHEET_ACCESS_DENIED, type: ERROR_TYPES.PERMISSION };
    if (fullError.includes('sheet') && fullError.includes('not found')) return { code: ERROR_CODES.SOURCE_SHEET_NOT_FOUND, type: ERROR_TYPES.SOURCE };
    return { code: ERROR_CODES.SOURCE_QUERY_INVALID, type: ERROR_TYPES.SOURCE };
  }
  
  // Processing errors
  if (fullError.includes('csv') || fullError.includes('parse') || 
      fullError.includes('file') || fullError.includes('too large') ||
      fullError.includes('too many rows') || fullError.includes('timeout')) {
    if (fullError.includes('csv') && (fullError.includes('parse') || fullError.includes('parsing'))) return { code: ERROR_CODES.PROCESSING_CSV_PARSE_ERROR, type: ERROR_TYPES.PROCESSING };
    if (fullError.includes('too large') || fullError.includes('exceeds')) return { code: ERROR_CODES.PROCESSING_FILE_TOO_LARGE, type: ERROR_TYPES.PROCESSING };
    if (fullError.includes('too many rows') || fullError.includes('exceeds') && fullError.includes('rows')) return { code: ERROR_CODES.PROCESSING_TOO_MANY_ROWS, type: ERROR_TYPES.PROCESSING };
    if (fullError.includes('timeout')) return { code: ERROR_CODES.PROCESSING_TIMEOUT, type: ERROR_TYPES.PROCESSING };
    if (fullError.includes('column') && (fullError.includes('mismatch') || fullError.includes('inconsistent'))) return { code: ERROR_CODES.PROCESSING_COLUMN_MISMATCH, type: ERROR_TYPES.PROCESSING };
    return { code: ERROR_CODES.PROCESSING_CSV_PARSE_ERROR, type: ERROR_TYPES.PROCESSING };
  }
  
  // Destination errors
  if (fullError.includes('destination') || fullError.includes('cannot access destination') ||
      fullError.includes('tab create') || fullError.includes('write failed')) {
    if (fullError.includes('tab') && fullError.includes('create')) return { code: ERROR_CODES.DEST_TAB_CREATE_FAILED, type: ERROR_TYPES.DESTINATION };
    if (fullError.includes('write') || fullError.includes('failed')) return { code: ERROR_CODES.DEST_WRITE_FAILED, type: ERROR_TYPES.DESTINATION };
    if (fullError.includes('access denied') || fullError.includes('permission')) return { code: ERROR_CODES.DEST_SHEET_ACCESS_DENIED, type: ERROR_TYPES.PERMISSION };
    return { code: ERROR_CODES.DEST_SHEET_NOT_FOUND, type: ERROR_TYPES.DESTINATION };
  }
  
  // System errors
  if (fullError.includes('timeout') && !fullError.includes('processing')) return { code: ERROR_CODES.SYSTEM_TIMEOUT, type: ERROR_TYPES.SYSTEM };
  if (fullError.includes('memory') || fullError.includes('quota')) return { code: ERROR_CODES.SYSTEM_MEMORY_LIMIT, type: ERROR_TYPES.SYSTEM };
  if (fullError.includes('rate limit')) return { code: ERROR_CODES.SYSTEM_RATE_LIMIT, type: ERROR_TYPES.SYSTEM };
  
  // Default
  return { code: ERROR_CODES.SYSTEM_UNKNOWN_ERROR, type: ERROR_TYPES.SYSTEM };
}

/**
 * Map status to log level for convenience
 * @param {string} status - Status string
 * @returns {string} Corresponding log level
 */
function statusToLevel(status) {
  const statusMap = {
    'START': LOG_LEVEL.START,
    'SUCCESS': LOG_LEVEL.SUCCESS,
    'ERROR': LOG_LEVEL.ERROR,
    'WARNING': LOG_LEVEL.WARNING,
    'INFO': LOG_LEVEL.INFO
  };
  return statusMap[status] || LOG_LEVEL.INFO;
}

/**
 * Log entry with comprehensive statistics
 * Adds log entry with all available metadata and statistics
 * 
 * Signature 1 (full): logEntry(sessionId, ruleId, level, status, message, stats)
 * Signature 2 (convenience): logEntry(sessionId, ruleId, status, message, rowsProcessed)
 * 
 * @param {string} sessionId - Session identifier
 * @param {string} ruleId - Rule identifier
 * @param {string} levelOrStatus - Log level (TRACE, DEBUG, INFO, WARNING, ERROR, FATAL, SUCCESS, START) OR Status (START, SUCCESS, ERROR, INFO, WARNING)
 * @param {string} statusOrMessage - Status (if level provided) OR Message (if status provided)
 * @param {string|Object} messageOrStats - Message (if status provided) OR Stats object (if level provided)
 * @param {number|Object} [rowsProcessedOrStats] - Rows processed (if using convenience signature) OR Stats (if using full signature)
 * @param {Object} [stats] - Statistics object (only if using full signature with 6+ params)
 */
function logEntry(sessionId, ruleId, levelOrStatus, statusOrMessage, messageOrStats, rowsProcessedOrStats) {
  // Determine which signature is being used
  let level, status, message, stats;
  
  // Check if 4th param is a status string (convenience signature)
  if (typeof statusOrMessage === 'string' && 
      ['START', 'SUCCESS', 'ERROR', 'INFO', 'WARNING'].includes(statusOrMessage) &&
      typeof messageOrStats === 'string') {
    // Convenience signature: logEntry(sessionId, ruleId, status, message, rowsProcessed)
    status = levelOrStatus;
    message = statusOrMessage;
    level = statusToLevel(status);
    stats = {
      rowsProcessed: rowsProcessedOrStats || 0
    };
  } else {
    // Full signature: logEntry(sessionId, ruleId, level, status, message, stats)
    level = levelOrStatus;
    status = statusOrMessage;
    message = messageOrStats;
    stats = rowsProcessedOrStats || {};
  }
  
  // Continue with enhanced logging
  logEntryInternal(sessionId, ruleId, level, status, message, stats);
}

/**
 * Internal log entry implementation with comprehensive statistics
 * @param {string} sessionId - Session identifier
 * @param {string} ruleId - Rule identifier
 * @param {string} level - Log level
 * @param {string} status - Status
 * @param {string} message - Log message
 * @param {Object} stats - Statistics object
 */
function logEntryInternal(sessionId, ruleId, level, status, message, stats = {}) {
  // Check if we should log this level
  if (!shouldLog(level)) {
    return;
  }
  
  try {
    const logsSheet = getSheet('logs');
    const timestamp = new Date();
    
    // Extract error information if error object provided
    let errorCode = stats.errorCode || '';
    let errorType = stats.errorType || '';
    if (stats.error && LOGGING_CONFIG.ENABLE_ERROR_CATEGORIZATION) {
      const categorized = categorizeError(stats.error);
      errorCode = categorized.code;
      errorType = categorized.type;
    }
    
    // Build metadata JSON
    let metadataJson = '';
    if (LOGGING_CONFIG.ENABLE_METADATA_LOGGING && stats.metadata) {
      try {
        // Calculate performance metrics
        const metadata = { ...stats.metadata };
        if (stats.rowsProcessed && stats.executionTimeMs && stats.executionTimeMs > 0) {
          metadata.rowsPerSecond = Math.round((stats.rowsProcessed / stats.executionTimeMs) * 1000);
        }
        
        // Add error stack trace if enabled
        if (stats.error && LOGGING_CONFIG.LOG_ERROR_STACK_TRACES) {
          metadata.errorStack = stats.error.stack || '';
        }
        
        metadataJson = JSON.stringify(metadata);
        
        // Limit metadata size
        if (metadataJson.length > LOGGING_CONFIG.MAX_METADATA_SIZE_BYTES) {
          metadataJson = metadataJson.substring(0, LOGGING_CONFIG.MAX_METADATA_SIZE_BYTES - 3) + '...';
        }
      } catch (e) {
        console.warn('Failed to serialize metadata:', e.message);
      }
    }
    
    // Build log row with all columns
    const logRow = [
      sessionId,
      timestamp,
      level || LOG_LEVEL.INFO,
      ruleId,
      status,
      message,
      stats.executionTimeMs || '',
      stats.rowsProcessed || 0,
      stats.columnsProcessed || '',
      stats.fileSizeBytes || '',
      stats.sourceType || '',
      stats.sourceIdentifier || '',
      errorCode,
      errorType,
      stats.retryAttempt !== undefined ? stats.retryAttempt : '',
      stats.destinationId || '',
      stats.destinationTab || '',
      stats.processingMode || '',
      metadataJson
    ];
    
    logsSheet.appendRow(logRow);
    
    // Set vertical alignment for the newly added row
    const lastRow = logsSheet.getLastRow();
    const columnCount = 19;
    const newRowRange = logsSheet.getRange(lastRow, 1, 1, columnCount);
    newRowRange.setVerticalAlignment('middle');
    
    // Auto-scroll to the latest entry for real-time monitoring
    try {
      const activeSheet = SpreadsheetApp.getActiveSheet();
      if (activeSheet && activeSheet.getName() === logsSheet.getName()) {
        logsSheet.setActiveRange(logsSheet.getRange(lastRow, 1, 1, columnCount));
        SpreadsheetApp.flush();
      }
    } catch (scrollError) {
      console.log(`Auto-scroll failed (non-critical): ${scrollError.message}`);
    }
    
    // Log to console with enhanced format
    const consolePrefix = `[${sessionId}] [${level}] ${ruleId}: ${status}`;
    const consoleSuffix = stats.executionTimeMs ? ` (${stats.executionTimeMs}ms)` : '';
    console.log(`${consolePrefix} - ${message}${consoleSuffix}`);
    
    // Log slow operations
    if (LOGGING_CONFIG.ENABLE_PERFORMANCE_TRACKING && 
        stats.executionTimeMs && 
        stats.executionTimeMs > LOGGING_CONFIG.LOG_SLOW_OPERATIONS_MS) {
      console.warn(`⚠️ Slow operation detected: ${ruleId} took ${stats.executionTimeMs}ms`);
    }
    
  } catch (error) {
    // Fallback to console if sheet logging fails
    console.error(`Logging failed: ${error.message}`);
    console.log(`[${sessionId}] ${ruleId}: ${status} - ${message}`);
  }
}

/**
 * Create logs sheet with headers
 * Creates logs sheet with proper headers, formatting, and frozen rows
 * @returns {Sheet} Created logs sheet
 */
function createLogsSheet() {
  const sheet = createSheet('logs');

  // Set up headers if sheet is empty
  if (sheet.getLastRow() === 0) {
    const headers = [
      'Session ID',
      'Timestamp',
      'Log Level',
      'Rule ID',
      'Status',
      'Message',
      'Execution Time (ms)',
      'Rows Processed',
      'Columns Processed',
      'File Size (bytes)',
      'Source Type',
      'Source Identifier',
      'Error Code',
      'Error Type',
      'Retry Attempt',
      'Destination ID',
      'Destination Tab',
      'Processing Mode',
      'Metadata (JSON)'
    ];

    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#E8F0FE');
    headerRange.setVerticalAlignment('middle');
    
    // Freeze the top row (header row)
    sheet.setFrozenRows(1);
    
    // Set vertical alignment for all cells in the sheet
    const allDataRange = sheet.getRange(1, 1, 1, headers.length);
    allDataRange.setVerticalAlignment('middle');
    
    // Set column widths for better readability
    sheet.setColumnWidth(1, 150);  // Session ID
    sheet.setColumnWidth(2, 150);  // Timestamp
    sheet.setColumnWidth(3, 100);  // Log Level
    sheet.setColumnWidth(4, 150);  // Rule ID
    sheet.setColumnWidth(5, 100);  // Status
    sheet.setColumnWidth(6, 300);  // Message
    sheet.setColumnWidth(7, 120);  // Execution Time
    sheet.setColumnWidth(8, 100);  // Rows Processed
    sheet.setColumnWidth(9, 100);  // Columns Processed
    sheet.setColumnWidth(10, 100); // File Size
    sheet.setColumnWidth(11, 100); // Source Type
    sheet.setColumnWidth(12, 200); // Source Identifier
    sheet.setColumnWidth(13, 120); // Error Code
    sheet.setColumnWidth(14, 100); // Error Type
    sheet.setColumnWidth(15, 100); // Retry Attempt
    sheet.setColumnWidth(16, 200); // Destination ID
    sheet.setColumnWidth(17, 150); // Destination Tab
    sheet.setColumnWidth(18, 120); // Processing Mode
    sheet.setColumnWidth(19, 400); // Metadata
    
    // Clean up: Remove extra rows and columns
    cleanupSheetRowsAndColumns(sheet, headers.length, 10);
  }

  return sheet;
}

/**
 * Log entry with automatic performance tracking
 * Wraps operation execution and logs performance metrics
 * @param {string} sessionId - Session identifier
 * @param {string} ruleId - Rule identifier
 * @param {string} level - Log level
 * @param {string} status - Status
 * @param {string} message - Log message
 * @param {Function} operation - Operation to execute and measure
 * @param {Object} [stats={}] - Additional statistics
 * @returns {*} Result of operation
 */
function logWithPerformance(sessionId, ruleId, level, status, message, operation, stats = {}) {
  const startTime = Date.now();
  let result;
  let error;
  
  try {
    result = operation();
    const executionTimeMs = Date.now() - startTime;
    
    logEntryInternal(sessionId, ruleId, level, status, message, {
      ...stats,
      executionTimeMs: executionTimeMs
    });
    
    return result;
  } catch (err) {
    error = err;
    const executionTimeMs = Date.now() - startTime;
    
    logEntryInternal(sessionId, ruleId, LOG_LEVEL.ERROR, LOG_STATUS.ERROR, err.message, {
      ...stats,
      executionTimeMs: executionTimeMs,
      error: err
    });
    
    throw err;
  }
}

/**
 * Get logs for specific session
 * Retrieves all log entries for a given session with all available fields
 * @param {string} sessionId - Session identifier
 * @returns {Array<Object>} Array of log entries with all fields
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
        logLevel: row[LOG_COLUMNS.LOG_LEVEL],
        ruleId: row[LOG_COLUMNS.RULE_ID],
        status: row[LOG_COLUMNS.STATUS],
        message: row[LOG_COLUMNS.MESSAGE],
        executionTimeMs: row[LOG_COLUMNS.EXECUTION_TIME_MS],
        rowsProcessed: row[LOG_COLUMNS.ROWS_PROCESSED],
        columnsProcessed: row[LOG_COLUMNS.COLUMNS_PROCESSED],
        fileSizeBytes: row[LOG_COLUMNS.FILE_SIZE_BYTES],
        sourceType: row[LOG_COLUMNS.SOURCE_TYPE],
        sourceIdentifier: row[LOG_COLUMNS.SOURCE_IDENTIFIER],
        errorCode: row[LOG_COLUMNS.ERROR_CODE],
        errorType: row[LOG_COLUMNS.ERROR_TYPE],
        retryAttempt: row[LOG_COLUMNS.RETRY_ATTEMPT],
        destinationId: row[LOG_COLUMNS.DESTINATION_ID],
        destinationTab: row[LOG_COLUMNS.DESTINATION_TAB],
        processingMode: row[LOG_COLUMNS.PROCESSING_MODE],
        metadata: row[LOG_COLUMNS.METADATA] ? JSON.parse(row[LOG_COLUMNS.METADATA]) : null
      });
    }
  }

  return logs;
}

/**
 * Clean up old log entries
 * Removes log entries older than retention period
 * @returns {number} Number of entries cleaned up
 * @throws {Error} If cleanup operation fails
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
 * Analyzes logs to build session summaries with statistics
 * @param {number} [limit=10] - Maximum number of sessions to return
 * @returns {Array<Object>} Array of session summaries
 * @returns {string} returns[].sessionId - Session identifier
 * @returns {Date} returns[].startTime - Session start time
 * @returns {Date} [returns[].endTime] - Session end time
 * @returns {string} returns[].status - Session status
 * @returns {number} returns[].ruleCount - Number of rules processed
 * @returns {number} returns[].successCount - Number of successful rules
 * @returns {number} returns[].errorCount - Number of failed rules
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
 * Removes all log data while preserving header row
 * @function clearLogs
 */
function clearLogs() {
  try {
    const logsSheet = getSheet('logs');
    const lastRow = logsSheet.getLastRow();
    const columnCount = 19; // Fixed column count
    
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
    const dataRange = logsSheet.getRange(2, 1, lastRow - 1, columnCount);
    dataRange.clearContent();

    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Cleared ${lastRow - 1} log entries`,
      'Clear Logs',
      TOAST_DURATION_MS
    );

    // Position at the header row
    logsSheet.setActiveRange(logsSheet.getRange(1, 1, 1, columnCount));
    SpreadsheetApp.flush();

  } catch (error) {
    SpreadsheetApp.getActiveSpreadsheet().toast(
      `Failed to clear logs: ${error.message}`,
      'Clear Logs Error',
      TOAST_LONG_DURATION_MS
    );
  }
}