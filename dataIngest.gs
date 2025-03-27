/**
 * Data Ingestion System for Google Sheets
 * This script allows for automated data transfer between various sources (emails, Google Sheets)
 * and destinations with configurable behaviors and logging.
 * 
 * VERSION: 2.3.0
 * UPDATED: March 27, 2025
 */

// Global configuration
const CONFIG = {
  // Email notification can be a single address or an array of addresses
  EMAIL_NOTIFICATIONS: ["mark.thomsen@gsa.gov", "is-training@gsa.gov"], // Change these to your email(s)
  
  // Sheet names and settings
  LOG_SHEET_NAME: "ingest-logs",
  CONFIG_SHEET_NAME: "cfg-ingest",
  VERIFICATION_SHEET_NAME: "ingest-verification", // Verification log sheet
  DIAGNOSTIC_SHEET_NAME: "verification-diagnostics", // Added diagnostic sheet
  MAX_LOG_ENTRIES: 100,
  NEW_SHEET_ROWS: 100, // Number of rows for new sheets
  
  // Email configuration
  EMAIL_CONFIG: {
    SEND_ON_START: true,              // Send email when batch job starts
    SEND_ON_COMPLETE: true,           // Send email when batch job completes
    SEND_ON_ERROR: true,              // Send email on errors
    INCLUDE_LOG_ATTACHMENT: true,     // Attach log sheet as CSV
    INCLUDE_VERIFICATION_ATTACHMENT: true, // Attach verification sheet as CSV
    HTML_FORMATTING: true,            // Use HTML formatting for emails
    MAX_ROWS_IN_EMAIL: 100,           // Maximum log rows to include in email body
    EMAIL_SUBJECT_PREFIX: "[Data Ingest]" // Prefix for email subjects
  },
    // Data verification
    VERIFICATION_CONFIG: {
      ENABLED: true,                // Enable verification
      VERIFY_ROW_COUNTS: true,      // Verify row counts match
      VERIFY_COLUMN_COUNTS: true,   // Verify column counts match
      VERIFY_SAMPLE_DATA: true,     // Verify sample data integrity
      SAMPLE_SIZE: 5                // Number of random rows to sample
    },
    
    // Formatting preferences for each sheet
    SHEET_FORMATS: {
      CONFIG_SHEET: {
        headerColor: "#D3D3D3",
        headerFontWeight: "bold",
        timestampFormat: "MM/dd/yyyy HH:mm:ss",
        alternatingRowColors: ["#FFFFFF", "#E6F2FF"],
        columnWidths: {
          "ruleActive": 90,
          "ingestMethod": 120,
          "sheetHandlingMode": 150,
          "lastRunTime": 150,
          "lastRunStatus": 100,
          "lastRunMessage": 200
        },
        statusColors: {
          "SUCCESS": "#B7E1CD", // Light green
          "ERROR": "#F4C7C3",   // Light red
          "SKIPPED": "#FFFFCC"  // Light yellow
        }
      },
      LOG_SHEET: {
        headerColor: "#D3D3D3",
        headerFontWeight: "bold",
        timestampFormat: "MM/dd/yyyy HH:mm:ss",
        alternatingRowColors: ["#FFFFFF", "#E6F2FF"],
        columnWidths: {
          "Timestamp": 180,
          "SessionID": 180,
          "EventType": 120,
          "Message": 500
        },
        eventTypeColors: {
          "ERROR": "#F4C7C3",   // Light red
          "SUCCESS": "#B7E1CD", // Light green
          "WARNING": "#FFFFCC"  // Light yellow
        }
      },
      VERIFICATION_SHEET: {
        headerColor: "#D3D3D3",
        headerFontWeight: "bold",
        timestampFormat: "MM/dd/yyyy HH:mm:ss",
        alternatingRowColors: ["#FFFFFF", "#E6F2FF"],
        columnWidths: {
          "Timestamp": 180,
          "SessionID": 180,
          "RuleID": 100,
          "SourceType": 100,
          "SourceFile": 200,
          "DestinationSheet": 200,
          "SourceRows": 100,
          "DestRows": 100,
          "SourceColumns": 100,
          "DestColumns": 100,
          "RowsMatch": 100,
          "ColumnsMatch": 100,
          "SamplesMatch": 100,
          "DataHash": 200,
          "Status": 100,
          "Details": 300
        },
        statusColors: {
          "COMPLETE": "#B7E1CD", // Light green
          "ERROR": "#F4C7C3"     // Light red
        }
      },
      DIAGNOSTIC_SHEET: {
        headerColor: "#D3D3D3",
        headerFontWeight: "bold",
        timestampFormat: "MM/dd/yyyy HH:mm:ss",
        alternatingRowColors: ["#FFFFFF", "#E6F2FF"],
        columnWidths: {
          "Timestamp": 180,
          "SessionID": 180,
          "Position": 150,
          "Column": 100,
          "SourceValue": 200,
          "SourceType": 100,
          "DestValue": 200,
          "DestType": 100,
          "NormalizedSource": 200,
          "NormalizedDest": 200,
          "Details": 300
        }
      }
    },
    
    // Column descriptions - these appear as notes on column headers
    COLUMN_DESCRIPTIONS: {
      ruleActive: "Check this box to enable this ingest rule",
      ingestMethod: "Select the method: email (import from attachment), gSheet (import from Google Sheet), or push (push data from current sheet)",
      sheetHandlingMode: "How to handle existing sheets: clearAndReuse (keep and clear), recreate (delete and recreate), copyFormat (preserve formatting), append (add to end)",
      
      // Email ingest fields
      in_email_searchString: "Gmail search query to find emails (e.g., 'subject:(Monthly Report) from:example.com')",
      in_email_attachmentPattern: "Regular expression pattern to match attachment filename (e.g., 'Monthly_Report_.*\\.csv')",
      
      // Google Sheet ingest fields
      in_gsheet_sheetId: "ID of the source Google Sheet (from URL or direct entry)",
      in_gsheet_sheetURL: "Full URL of the source Google Sheet",
      in_gsheet_tabName: "Name of the tab in the source Google Sheet to import data from",
      
      // Common destination fields
      dest_sheetId: "ID of the destination Google Sheet (from URL or direct entry)",
      dest_sheetUrl: "Full URL of the destination Google Sheet",
      dest_sheet_tabName: "Name of the tab in the destination Google Sheet to write data to",
      
      // Push fields
      pushSourceTabName: "Name of the tab in the current spreadsheet to push data from",
      pushDestinationSheetId: "ID of the destination Google Sheet for push operation",
      pushDestinationSheetUrl: "Full URL of the destination Google Sheet for push operation",
      pushDestinationTabName: "Name of the tab in the destination Google Sheet to push data to",
      
      // Status fields
      lastRunTime: "Timestamp of the most recent execution of this rule",
      lastRunStatus: "Status of the most recent execution (SUCCESS, ERROR, SKIPPED)",
      lastRunMessage: "Details about the most recent execution result"
    },
    
    // Column mappings - change the right side values to customize column headers
    COLUMN_MAPPINGS: {
      // Essential fields
      ruleActive: "ruleActive",
      ingestMethod: "ingestMethod",
      sheetHandlingMode: "sheetHandlingMode",
      
      // Email ingest fields
      in_email_searchString: "in_email_searchString",
      in_email_attachmentPattern: "in_email_attachmentPattern",
      
      // Google Sheet ingest fields
      in_gsheet_sheetId: "in_gsheet_sheetId",
      in_gsheet_sheetURL: "in_gsheet_sheetURL",
      in_gsheet_tabName: "in_gsheet_tabName",
      
      // Common destination fields
      dest_sheetId: "dest_sheetId",
      dest_sheetUrl: "dest_sheetUrl",
      dest_sheet_tabName: "dest_sheet_tabName",
      
      // Push fields
      pushSourceTabName: "pushSourceTabName",
      pushDestinationSheetId: "pushDestinationSheetId",
      pushDestinationSheetUrl: "pushDestinationSheetUrl",
      pushDestinationTabName: "pushDestinationTabName",
      
      // Status fields
      lastRunTime: "lastRunTime",
      lastRunStatus: "lastRunStatus",
      lastRunMessage: "lastRunMessage"
    }
}



/**
 * Creates or updates the log sheet with enhanced formatting
 * @returns {Sheet} The created or updated sheet
 */
function createLogSheet() {
  const startTime = new Date();
  const sheetName = CONFIG.LOG_SHEET_NAME;
  Logger.log(`Creating/updating log sheet: ${sheetName}`);
  
  const headers = ['Timestamp', 'SessionID', 'EventType', 'Message'];
  
  try {
    // Use the safer sheet creation method
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      // Get formatting preferences from config
      const formatConfig = CONFIG.SHEET_FORMATS.LOG_SHEET;
      
      // Set headers with formatting
      Logger.log(`Setting headers and formatting`);
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);
      headerRange.setBackground(formatConfig.headerColor);
      headerRange.setFontWeight(formatConfig.headerFontWeight);
      
      // Format timestamp column
      Logger.log(`Formatting timestamp column`);
      sheet.getRange(2, 1, CONFIG.NEW_SHEET_ROWS, 1).setNumberFormat(formatConfig.timestampFormat);
      
      // Set column widths based on config
      Logger.log(`Setting column widths`);
      for (let i = 0; i < headers.length; i++) {
        const columnName = headers[i];
        const width = formatConfig.columnWidths[columnName] || 150; // Default width if not specified
        sheet.setColumnWidth(i + 1, width);
      }
      
      // Freeze the header row
      Logger.log(`Freezing header row`);
      sheet.setFrozenRows(1);
      
      // Add alternating row colors
      Logger.log(`Adding alternating row colors`);
      const dataRange = sheet.getDataRange();
      const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied("=ISEVEN(ROW())")
        .setBackground(formatConfig.alternatingRowColors[1])
        .setRanges([dataRange])
        .build();
      
      // Add conditional formatting for event types
      const rules = [alternatingRule];
      
      // Add rules for event types if configured
      if (formatConfig.eventTypeColors) {
        const eventTypeRange = sheet.getRange("C2:C1000"); // EventType column
        
        // Add a rule for each event type color
        for (const [eventType, color] of Object.entries(formatConfig.eventTypeColors)) {
          const eventTypeRule = SpreadsheetApp.newConditionalFormatRule()
            .whenTextEqualTo(eventType)
            .setBackground(color)
            .setRanges([eventTypeRange])
            .build();
          
          rules.push(eventTypeRule);
        }
      }
      
      sheet.setConditionalFormatRules(rules);
      
      // Auto-resize all columns for better display
      Logger.log(`Auto-resizing columns`);
      for (let i = 1; i <= headers.length; i++) {
        sheet.autoResizeColumn(i);
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Created log sheet successfully in ${duration} seconds`);
      
      // Add an initial log entry to document the sheet creation
      const initialSessionId = generateUniqueID();
      sheet.getRange(2, 1, 1, 4).setValues([
        [new Date(), initialSessionId, "SHEET_CREATED", `Log sheet created or recreated successfully in ${duration} seconds`]
      ]);
    });
  } catch (error) {
    const errorMsg = `Error creating log sheet: ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}

/**
 * Creates or updates the verification sheet for data integrity tracking
 * @returns {Sheet} The created or updated sheet
 */
function createVerificationSheet() {
  const startTime = new Date();
  const sheetName = CONFIG.VERIFICATION_SHEET_NAME;
  Logger.log(`Creating/updating verification sheet: ${sheetName}`);
  
  // Define headers for verification log
  const headers = [
    'Timestamp', 
    'SessionID', 
    'RuleID', 
    'SourceType',
    'SourceFile', 
    'DestinationSheet',
    'SourceRows', 
    'DestRows', 
    'SourceColumns', 
    'DestColumns',
    'RowsMatch', 
    'ColumnsMatch',
    'SamplesMatch',
    'DataHash',
    'Status',
    'Details'
  ];
  
  try {
    // Use the safer sheet creation method
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      // Get formatting preferences from config
      const formatConfig = CONFIG.SHEET_FORMATS.VERIFICATION_SHEET;
      
      // Set headers with formatting
      Logger.log(`Setting headers and formatting`);
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);
      headerRange.setBackground(formatConfig.headerColor);
      headerRange.setFontWeight(formatConfig.headerFontWeight);
      
      // Format timestamp column
      Logger.log(`Formatting timestamp column`);
      sheet.getRange(2, 1, CONFIG.NEW_SHEET_ROWS, 1).setNumberFormat(formatConfig.timestampFormat);
      
      // Set column widths based on config
      Logger.log(`Setting column widths`);
      for (let i = 0; i < headers.length; i++) {
        const columnName = headers[i];
        const width = formatConfig.columnWidths[columnName] || 150; // Default width if not specified
        sheet.setColumnWidth(i + 1, width);
      }
      
      // Freeze the header row
      Logger.log(`Freezing header row`);
      sheet.setFrozenRows(1);
      
      // Set up conditional formatting
      Logger.log(`Setting up conditional formatting`);
      const rules = [];
      
      // Status column formatting
      if (formatConfig.statusColors) {
        const statusRange = sheet.getRange("O2:O1000");  // Status column
        
        // Add a rule for each status color
        for (const [status, color] of Object.entries(formatConfig.statusColors)) {
          const statusRule = SpreadsheetApp.newConditionalFormatRule()
            .whenTextEqualTo(status)
            .setBackground(color)
            .setRanges([statusRange])
            .build();
          
          rules.push(statusRule);
        }
      }
      
      // Add alternating row colors
      const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied("=ISEVEN(ROW())")
        .setBackground(formatConfig.alternatingRowColors[1])
        .setRanges([sheet.getDataRange()])
        .build();
      
      rules.push(alternatingRule);
      sheet.setConditionalFormatRules(rules);
      
      // Auto-resize all columns for better display
      Logger.log(`Auto-resizing columns`);
      for (let i = 1; i <= headers.length; i++) {
        sheet.autoResizeColumn(i);
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Created verification sheet successfully in ${duration} seconds`);
      
      // Add an initial log entry to document the sheet creation
      const initialSessionId = generateUniqueID();
      sheet.getRange(2, 1, 1, 16).setValues([
        [new Date(), initialSessionId, "SHEET_CREATED", "Verification", "", "", 
         0, 0, 0, 0, "N/A", "N/A", "N/A", "", "COMPLETE", 
         `Verification sheet created or recreated successfully in ${duration} seconds`]
      ]);
    });
  } catch (error) {
    const errorMsg = `Error creating verification sheet: ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}

/**
 * Creates or updates the diagnostic sheet for detailed data comparison
 * @returns {Sheet} The created or updated sheet
 */
function createDiagnosticSheet() {
  const startTime = new Date();
  const sheetName = CONFIG.DIAGNOSTIC_SHEET_NAME;
  Logger.log(`Creating/updating diagnostic sheet: ${sheetName}`);
  
  // Define headers for diagnostic log
  const headers = [
    'Timestamp', 
    'SessionID', 
    'Position',
    'Column',
    'SourceValue', 
    'SourceType',
    'DestValue', 
    'DestType', 
    'NormalizedSource',
    'NormalizedDest',
    'Details'
  ];
  
  try {
    // Use the safer sheet creation method
    return ensureSafeSheetCreation(sheetName, (sheet) => {
      // Get formatting preferences from config
      const formatConfig = CONFIG.SHEET_FORMATS.DIAGNOSTIC_SHEET;
      
      // Set headers with formatting
      Logger.log(`Setting headers and formatting`);
      const headerRange = sheet.getRange(1, 1, 1, headers.length);
      headerRange.setValues([headers]);
      headerRange.setBackground(formatConfig.headerColor);
      headerRange.setFontWeight(formatConfig.headerFontWeight);
      
      // Format timestamp column
      Logger.log(`Formatting timestamp column`);
      sheet.getRange(2, 1, CONFIG.NEW_SHEET_ROWS, 1).setNumberFormat(formatConfig.timestampFormat);
      
      // Set column widths based on config
      Logger.log(`Setting column widths`);
      for (let i = 0; i < headers.length; i++) {
        const columnName = headers[i];
        const width = formatConfig.columnWidths[columnName] || 150; // Default width if not specified
        sheet.setColumnWidth(i + 1, width);
      }
      
      // Freeze the header row
      Logger.log(`Freezing header row`);
      sheet.setFrozenRows(1);
      
      // Add alternating row colors
      const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
        .whenFormulaSatisfied("=ISEVEN(ROW())")
        .setBackground(formatConfig.alternatingRowColors[1])
        .setRanges([sheet.getDataRange()])
        .build();
      
      sheet.setConditionalFormatRules([alternatingRule]);
      
      // Auto-resize all columns for better display
      Logger.log(`Auto-resizing columns`);
      for (let i = 1; i <= headers.length; i++) {
        sheet.autoResizeColumn(i);
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Created diagnostic sheet successfully in ${duration} seconds`);
      
      // Add an initial log entry to document the sheet creation
      const initialSessionId = generateUniqueID();
      sheet.getRange(2, 1, 1, 11).setValues([
        [new Date(), initialSessionId, "SheetCreation", "N/A", 
         "N/A", "N/A", "N/A", "N/A", "N/A", "N/A",
         `Diagnostic sheet created or recreated successfully in ${duration} seconds`]
      ]);
    });
  } catch (error) {
    const errorMsg = `Error creating diagnostic sheet: ${error.message}`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
}

/**
 * Logs an operation to the log sheet with improved error handling
 * @param {string} sessionId - Session identifier
 * @param {string} eventType - Type of event
 * @param {string} message - Log message
 * @returns {boolean} Success or failure of logging operation
 */
function logOperation(sessionId, eventType, message) {
  Logger.log(`Logging operation: [${sessionId}] ${eventType}: ${message}`);
  
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
    
    // Create log sheet if it doesn't exist
    if (!logSheet) {
      Logger.log(`Log sheet not found, creating new one`);
      try {
        createLogSheet();
        logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
        
        if (!logSheet) {
          throw new Error("Failed to create log sheet");
        }
      } catch (createError) {
        Logger.log(`ERROR CREATING LOG SHEET: ${createError.message}`);
        // Try a simplified approach if the full creation failed
        try {
          logSheet = ss.insertSheet(CONFIG.LOG_SHEET_NAME);
          const headers = ['Timestamp', 'SessionID', 'EventType', 'Message'];
          logSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
          Logger.log("Created basic log sheet as fallback");
        } catch (fallbackError) {
          Logger.log(`FALLBACK CREATION FAILED: ${fallbackError.message}`);
          return false;
        }
      }
    }
    
    // Get the timestamp
    const timestamp = new Date();
    
    // Add log entry to the top (row 2, just below headers)
    Logger.log(`Adding log entry to sheet`);
    try {
      logSheet.insertRowAfter(1);
      const newRow = logSheet.getRange(2, 1, 1, 4);
      newRow.setValues([[timestamp, sessionId, eventType, message]]);
      
      // Format timestamp using sheet-specific format from config
      const timestampFormat = CONFIG.SHEET_FORMATS.LOG_SHEET.timestampFormat || "MM/dd/yyyy HH:mm:ss";
      logSheet.getRange(2, 1).setNumberFormat(timestampFormat);
      
      // Apply conditional formatting based on event type if specified in config
      try {
        const eventTypeColors = CONFIG.SHEET_FORMATS.LOG_SHEET.eventTypeColors;
        if (eventTypeColors && eventTypeColors[eventType]) {
          logSheet.getRange(2, 3, 1, 2).setBackground(eventTypeColors[eventType]);
        }
      } catch (formatError) {
        // Ignore formatting errors - they shouldn't stop the logging
        Logger.log(`WARNING: Couldn't apply event type formatting: ${formatError.message}`);
      }
      
      // Trim log to maximum entries
      const totalRows = logSheet.getLastRow();
      if (totalRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalRows - CONFIG.MAX_LOG_ENTRIES - 1;
        Logger.log(`Trimming log, deleting ${deleteCount} oldest entries`);
        logSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
      }
      
      // Auto-resize if needed
      try {
        logSheet.autoResizeColumn(4); // Message column
      } catch (resizeError) {
        // Ignore resize errors
        Logger.log(`WARNING: Couldn't auto-resize column: ${resizeError.message}`);
      }
      
      return true;
    } catch (writeError) {
      Logger.log(`ERROR WRITING TO LOG: ${writeError.message}`);
      
      // Try one more time with a simpler approach
      try {
        const lastRow = logSheet.getLastRow();
        logSheet.getRange(lastRow + 1, 1, 1, 4).setValues([[timestamp, sessionId, eventType, message]]);
        Logger.log("Successfully wrote log entry using fallback method");
        return true;
      } catch (fallbackWriteError) {
        Logger.log(`FALLBACK WRITE FAILED: ${fallbackWriteError.message}`);
        return false;
      }
    }
  } catch (error) {
    // Fall back to Logger if we can't write to the log sheet
    Logger.log(`ERROR LOGGING: ${error.message}`);
    Logger.log(`Original log: [${sessionId}] ${eventType}: ${message}`);
    return false;
  }
}

/**
 * Logs a verification entry to the verification sheet with improved reliability
 * @param {Object} verification - Verification data object
 * @returns {boolean} Success or failure of logging operation
 */
function logVerification(verification) {
  Logger.log(`Logging verification: [${verification.sessionId}] ${verification.ruleId}`);
  
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
    
    // Create verification sheet if it doesn't exist
    if (!verificationSheet) {
      Logger.log(`Verification sheet not found, creating new one`);
      try {
        createVerificationSheet();
        verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
        
        if (!verificationSheet) {
          throw new Error("Failed to create verification sheet");
        }
      } catch (createError) {
        Logger.log(`ERROR CREATING VERIFICATION SHEET: ${createError.message}`);
        // Try a simplified approach if the full creation failed
        try {
          verificationSheet = ss.insertSheet(CONFIG.VERIFICATION_SHEET_NAME);
          const headers = [
            'Timestamp', 'SessionID', 'RuleID', 'SourceType', 'SourceFile', 'DestinationSheet',
            'SourceRows', 'DestRows', 'SourceColumns', 'DestColumns', 'RowsMatch', 'ColumnsMatch',
            'SamplesMatch', 'DataHash', 'Status', 'Details'
          ];
          verificationSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
          Logger.log("Created basic verification sheet as fallback");
        } catch (fallbackError) {
          Logger.log(`FALLBACK CREATION FAILED: ${fallbackError.message}`);
          return false;
        }
      }
    }
    
    // Default values for missing fields
    const rowsMatch = verification.sourceRowCount === verification.destinationRowCount ? 'YES' : 'NO';
    const columnsMatch = verification.sourceColumnCount === verification.destinationColumnCount ? 'YES' : 'NO';
    const samplesMatch = verification.samplesMatch ? 'YES' : 'NO';
    const status = verification.isComplete ? 'COMPLETE' : 'ERROR';
    
    // Get formatting preferences from config
    const formatConfig = CONFIG.SHEET_FORMATS.VERIFICATION_SHEET;
    
    // Add verification entry to the top (row 2, just below headers)
    Logger.log(`Adding verification entry to sheet`);
    try {
      verificationSheet.insertRowAfter(1);
      const newRow = verificationSheet.getRange(2, 1, 1, 16);
      newRow.setValues([[
        verification.timestamp,
        verification.sessionId,
        verification.ruleId,
        verification.sourceType || '',
        verification.sourceFile || '',
        verification.destinationSheet || '',
        verification.sourceRowCount || 0,
        verification.destinationRowCount || 0,
        verification.sourceColumnCount || 0,
        verification.destinationColumnCount || 0,
        rowsMatch,
        columnsMatch,
        samplesMatch,
        verification.dataHash || '',
        status,
        verification.errorDetails || ''
      ]]);
      
      // Format timestamp using sheet-specific format
      const timestampFormat = formatConfig.timestampFormat || "MM/dd/yyyy HH:mm:ss";
      verificationSheet.getRange(2, 1).setNumberFormat(timestampFormat);
      
      // Apply conditional formatting based on status if specified in config
      try {
        const statusColors = formatConfig.statusColors;
        if (statusColors && statusColors[status]) {
          verificationSheet.getRange(2, 15, 1, 1).setBackground(statusColors[status]);
        }
      } catch (formatError) {
        // Ignore formatting errors - they shouldn't stop the logging
        Logger.log(`WARNING: Couldn't apply status formatting: ${formatError.message}`);
      }
      
      // Trim log to maximum entries
      const totalRows = verificationSheet.getLastRow();
      if (totalRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalRows - CONFIG.MAX_LOG_ENTRIES - 1;
        Logger.log(`Trimming verification log, deleting ${deleteCount} oldest entries`);
        verificationSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
      }
      
      return true;
    } catch (writeError) {
      Logger.log(`ERROR WRITING TO VERIFICATION SHEET: ${writeError.message}`);
      
      // Try one more time with a simpler approach
      try {
        const lastRow = verificationSheet.getLastRow();
        verificationSheet.getRange(lastRow + 1, 1, 1, 16).setValues([[
          verification.timestamp,
          verification.sessionId,
          verification.ruleId,
          verification.sourceType || '',
          verification.sourceFile || '',
          verification.destinationSheet || '',
          verification.sourceRowCount || 0,
          verification.destinationRowCount || 0,
          verification.sourceColumnCount || 0,
          verification.destinationColumnCount || 0,
          rowsMatch,
          columnsMatch,
          samplesMatch,
          verification.dataHash || '',
          status,
          verification.errorDetails || ''
        ]]);
        Logger.log("Successfully wrote verification entry using fallback method");
        return true;
      } catch (fallbackWriteError) {
        Logger.log(`FALLBACK WRITE FAILED: ${fallbackWriteError.message}`);
        return false;
      }
    }
  } catch (error) {
    // Fall back to Logger if we can't write to the verification sheet
    Logger.log(`ERROR LOGGING VERIFICATION: ${error.message}`);
    Logger.log(`Original verification entry: ${JSON.stringify(verification)}`);
    return false;
  }
}

/**
 * Logs a diagnostic entry for detailed data comparison
 * @param {Object} diagnostic - Diagnostic data object
 * @returns {boolean} Success or failure of logging operation
 */
function logDiagnostic(diagnostic) {
  Logger.log(`Logging diagnostic data: [${diagnostic.sessionId}] ${diagnostic.position}`);
  
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);
    
    // Create diagnostic sheet if it doesn't exist
    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet not found, creating new one`);
      try {
        createDiagnosticSheet();
        diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);
        
        if (!diagnosticSheet) {
          throw new Error("Failed to create diagnostic sheet");
        }
      } catch (createError) {
        Logger.log(`ERROR CREATING DIAGNOSTIC SHEET: ${createError.message}`);
        return false;
      }
    }
    
    // Add diagnostic entry
    Logger.log(`Adding diagnostic entry to sheet`);
    try {
      diagnosticSheet.insertRowAfter(1);
      const newRow = diagnosticSheet.getRange(2, 1, 1, 11);
      newRow.setValues([[
        diagnostic.timestamp || new Date(),
        diagnostic.sessionId,
        diagnostic.position,
        diagnostic.column || '',
        diagnostic.sourceValue,
        typeof diagnostic.sourceValue,
        diagnostic.destValue,
        typeof diagnostic.destValue,
        String(diagnostic.normalizedSource),
        String(diagnostic.normalizedDest),
        diagnostic.details || ''
      ]]);
      
      // Trim log to maximum entries
      const totalRows = diagnosticSheet.getLastRow();
      if (totalRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalRows - CONFIG.MAX_LOG_ENTRIES - 1;
        Logger.log(`Trimming diagnostic log, deleting ${deleteCount} oldest entries`);
        diagnosticSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
      }
      
      return true;
    } catch (writeError) {
      Logger.log(`ERROR WRITING TO DIAGNOSTIC SHEET: ${writeError.message}`);
      return false;
    }
  } catch (error) {
    Logger.log(`ERROR LOGGING DIAGNOSTIC: ${error.message}`);
    return false;
  }
}

/**
 * IMPROVED: Verify that sample data matches between source and destination
 * Now with better type handling and diagnostic logging for mismatches
 * @param {Array} sourceData - Source 2D array of data
 * @param {Sheet} destSheet - Destination sheet object
 * @param {string} sessionId - Session identifier for logging
 * @param {string} sheetHandlingMode - How sheets are being handled
 * @param {number} beforeRowCount - Row count before operation (for append mode)
 * @returns {boolean} True if samples match, false otherwise
 */
function verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount) {
  if (!sourceData || sourceData.length === 0) {
    return false;
  }
  
  try {
    // Determine sample points to check
    const SAMPLE_SIZE = CONFIG.VERIFICATION_CONFIG.SAMPLE_SIZE || 5;
    
    // Samples to check - always check first and last row, plus some random rows
    const samplesToCheck = [
      {position: 'First Row', sourceIdx: 0}
    ];
    
    // Only add last row if there's more than one row
    if (sourceData.length > 1) {
      samplesToCheck.push({
        position: 'Last Row', 
        sourceIdx: sourceData.length - 1
      });
    }
    
    // Add some random rows from the middle if there are enough rows
    if (sourceData.length > 3) {
      // Pick a few random rows (avoid duplicates)
      const selectedIndices = new Set();
      selectedIndices.add(0); // First row already selected
      selectedIndices.add(sourceData.length - 1); // Last row already selected
      
      // Try to add random rows up to SAMPLE_SIZE
      for (let attempt = 0; attempt < SAMPLE_SIZE * 2 && selectedIndices.size < Math.min(SAMPLE_SIZE, sourceData.length); attempt++) {
        const randomIndex = Math.floor(Math.random() * (sourceData.length - 2)) + 1; // Avoid first and last rows
        if (!selectedIndices.has(randomIndex)) {
          selectedIndices.add(randomIndex);
          samplesToCheck.push({
            position: `Row ${randomIndex + 1}`,
            sourceIdx: randomIndex
          });
        }
      }
    }
    
    // Check each sample
    for (const sample of samplesToCheck) {
      const sourceRow = sourceData[sample.sourceIdx];
      
      // For append mode, adjust destination row index
      const destRowIdx = (sheetHandlingMode === 'append' && beforeRowCount > 0 && sample.sourceIdx > 0) ? 
                        beforeRowCount + sample.sourceIdx : sample.sourceIdx + 1;
      
      const destRange = destSheet.getRange(destRowIdx, 1, 1, sourceRow.length);
      const destRow = destRange.getValues()[0];
      
      // Compare values with advanced type normalization
      for (let i = 0; i < sourceRow.length; i++) {
        const sourceVal = sourceRow[i];
        const destVal = destRow[i];
        
        // Normalize values for comparison to handle type differences
        const sourceNormalized = normalizeValue(sourceVal);
        const destNormalized = normalizeValue(destVal);
        
        if (sourceNormalized !== destNormalized) {
          const errorMsg = `${sample.position} mismatch at column ${i+1}: Source="${sourceVal}", Dest="${destVal}"`;
          Logger.log(errorMsg);
          
          // Log detailed diagnostic information for mismatches
          logDiagnostic({
            timestamp: new Date(),
            sessionId: sessionId,
            position: sample.position,
            column: i + 1,
            sourceValue: sourceVal,
            destValue: destVal,
            normalizedSource: sourceNormalized,
            normalizedDest: destNormalized,
            details: errorMsg
          });
          
          if (sessionId) {
            logOperation(sessionId, "SAMPLE_MISMATCH", errorMsg);
          }
          
          return false;
        }
      }
    }
    
    return true;
  } catch (error) {
    const errorMsg = `Error in sample verification: ${error.message}`;
    Logger.log(errorMsg);
    if (sessionId) {
      logOperation(sessionId, "SAMPLE_CHECK_ERROR", errorMsg);
    }
    return false;
  }
}

/**
 * Normalize values for comparison to handle type differences
 * @param {*} value - The value to normalize
 * @returns {string} Normalized string representation
 */
function normalizeValue(value) {
  // Handle null and undefined
  if (value === null || value === undefined) {
    return '';
  }
  
  // Handle dates
  if (value instanceof Date) {
    return value.getTime().toString();
  }
  
  // Handle numbers (including zero)
  if (typeof value === 'number') {
    // Convert to string with fixed precision to handle floating point issues
    return value.toFixed(10).replace(/\.?0+$/, '');
  }
  
  // Handle booleans
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  
  // Handle strings and other types
  return String(value).trim();
}

/**
 * Sends an error notification email with improved formatting and attachments
 * @param {string} subject - Email subject
 * @param {string} message - Email message
 * @param {string} sessionId - Session identifier for log filtering
 */
function sendErrorNotification(subject, message, sessionId) {
  Logger.log(`Preparing to send error notification: ${subject}`);
  
  try {
    if (CONFIG.EMAIL_NOTIFICATIONS && CONFIG.EMAIL_NOTIFICATIONS.length > 0) {
      // Add prefix to subject if configured
      if (CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX) {
        subject = `${CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX} ERROR: ${subject}`;
      } else {
        subject = `ERROR: ${subject}`;
      }
      
      // Create the email content
      let emailBody = "";
      let htmlBody = "";
      
      // Plain text version
      emailBody = `Error in Data Ingest System:
      
Time: ${new Date().toLocaleString()}
Session ID: ${sessionId}
Error: ${message}

Please check the log sheet for details.`;
      
      // HTML version if enabled
      if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
        const spreadsheetUrl = SpreadsheetApp.getActiveSpreadsheet().getUrl();
        const spreadsheetName = SpreadsheetApp.getActiveSpreadsheet().getName();
        
        htmlBody = `
<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }
    h2 { color: #CC0000; border-bottom: 1px solid #CC0000; padding-bottom: 10px; }
    table { border-collapse: collapse; width: 100%; margin: 20px 0; }
    th { background-color: #f2f2f2; text-align: left; padding: 8px; }
    td { padding: 8px; border: 1px solid #ddd; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    .error { color: #CC0000; font-weight: bold; }
    .info { color: #0066CC; }
    .success { color: #007700; }
    .button { display: inline-block; padding: 10px 20px; background-color: #0066CC; color: white; 
              text-decoration: none; border-radius: 5px; margin-top: 20px; }
  </style>
</head>
<body>
  <h2>Data Ingest System Error</h2>
  <p><strong>Time:</strong> ${new Date().toLocaleString()}</p>
  <p><strong>Spreadsheet:</strong> ${spreadsheetName}</p>
  <p><strong>Session ID:</strong> ${sessionId}</p>
  <p class="error"><strong>Error:</strong> ${message}</p>
  <p><strong>Please check the log sheet for details.</strong></p>
`;
        
        // Add log entries if available
        if (sessionId) {
          const logEntries = getLogEntriesForSession(sessionId);
          if (logEntries && logEntries.length > 0) {
            htmlBody += `
  <h3>Recent Log Entries</h3>
  <table>
    <tr>
      <th>Timestamp</th>
      <th>Event Type</th>
      <th>Message</th>
    </tr>
    ${logEntries.map(entry => `
    <tr>
      <td>${formatDate(entry.timestamp)}</td>
      <td>${entry.eventType}</td>
      <td>${entry.message}</td>
    </tr>
    `).join('')}
  </table>
`;
          }
        }
        
        // Add verification entries if available and configured
        if (sessionId && CONFIG.VERIFICATION_CONFIG.ENABLED) {
          try {
            const ss = SpreadsheetApp.getActiveSpreadsheet();
            const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
            
            if (verificationSheet) {
              const allData = verificationSheet.getDataRange().getValues();
              if (allData.length > 1) {
                const headers = allData[0];
                const sessionIdIdx = headers.indexOf("SessionID");
                
                if (sessionIdIdx !== -1) {
                  const sessionEntries = allData.filter((row, index) => 
                    index > 0 && row[sessionIdIdx] === sessionId);
                  
                  if (sessionEntries.length > 0) {
                    htmlBody += `
  <h3>Verification Results</h3>
  <table>
    <tr>
      <th>Rule ID</th>
      <th>Source</th>
      <th>Destination</th>
      <th>Rows Expected</th>
      <th>Rows Processed</th>
      <th>Status</th>
    </tr>
    ${sessionEntries.map(entry => {
      const ruleIdx = headers.indexOf("RuleID");
      const sourceIdx = headers.indexOf("SourceFile");
      const destIdx = headers.indexOf("DestinationSheet");
      const rowsSourceIdx = headers.indexOf("SourceRows");
      const rowsDestIdx = headers.indexOf("DestRows");
      const statusIdx = headers.indexOf("Status");
      
      const status = entry[statusIdx] || "UNKNOWN";
      const statusClass = status === "COMPLETE" ? "success" : 
                        status === "ERROR" ? "error" : "info";
      
      return `
    <tr>
      <td>${entry[ruleIdx] || ""}</td>
      <td>${entry[sourceIdx] || ""}</td>
      <td>${entry[destIdx] || ""}</td>
      <td align="right">${entry[rowsSourceIdx] || 0}</td>
      <td align="right">${entry[rowsDestIdx] || 0}</td>
      <td class="${statusClass}">${status}</td>
    </tr>
      `;
    }).join('')}
  </table>
                    `;
                  }
                }
              }
            }
          } catch (verificationError) {
            Logger.log(`Error displaying verification data in email: ${verificationError.message}`);
            // Don't let this error stop the email - just continue without verification data
          }
        }
        
        // Add link to spreadsheet
        htmlBody += `
  <p><a href="${spreadsheetUrl}" class="button">Open Spreadsheet</a></p>
  <p style="color: #999; font-size: 12px; margin-top: 30px;">
    This is an automated message from the Data Ingest System.<br>
    Please do not reply to this email.
  </p>
</body>
</html>
`;
      }
      
      // Handle both single email and array of emails
      const emails = Array.isArray(CONFIG.EMAIL_NOTIFICATIONS) 
        ? CONFIG.EMAIL_NOTIFICATIONS 
        : [CONFIG.EMAIL_NOTIFICATIONS];
      
      Logger.log(`Sending error notification to ${emails.length} recipient(s): ${emails.join(', ')}`);
      
      // Prepare attachments if configured
      let attachments = [];
      
      if (CONFIG.EMAIL_CONFIG.INCLUDE_LOG_ATTACHMENT) {
        const logAttachment = createLogAttachment(sessionId);
        if (logAttachment) {
          attachments.push(logAttachment);
        }
      }
      
      if (CONFIG.EMAIL_CONFIG.INCLUDE_VERIFICATION_ATTACHMENT) {
        const verificationAttachment = createVerificationAttachment(sessionId);
        if (verificationAttachment) {
          attachments.push(verificationAttachment);
        }
      }
      
      // Send to each email
      emails.forEach(email => {
        if (email && email.trim() !== '') {
          GmailApp.sendEmail(email, subject, emailBody, {
            htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : null,
            attachments: attachments
          });
          Logger.log(`Notification sent to: ${email}`);
        }
      });
      
      // Log that we sent notifications
      logOperation(sessionId, "EMAIL_SENT", 
        `Error notification email sent to ${emails.length} recipient(s) with ${attachments.length} attachments`);
      
    } else {
      Logger.log(`No notification recipients configured, skipping email notification`);
    }
  } catch (error) {
    Logger.log(`ERROR SENDING NOTIFICATION: ${error.message}`);
  }
}

/**
 * Sends a notification when a job starts
 * @param {string} sessionId - Session identifier
 * @param {number} rulesToProcess - Number of rules to process (optional)
 */
function sendJobStartNotification(sessionId, rulesToProcess) {
  Logger.log(`Preparing to send job start notification for session: ${sessionId}`);
  
  try {
    if (CONFIG.EMAIL_NOTIFICATIONS && CONFIG.EMAIL_NOTIFICATIONS.length > 0) {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const spreadsheetUrl = ss.getUrl();
      const spreadsheetName = ss.getName();
      
      // Create email content
      let subject = `Data Ingest Job Started - ${spreadsheetName}`;
      if (CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX) {
        subject = `${CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX} ${subject}`;
      }
      
      let emailBody = `Data Ingest job has started:
      
Time: ${new Date().toLocaleString()}
Session ID: ${sessionId}
Spreadsheet: ${spreadsheetName}
Rules to process: ${rulesToProcess ? rulesToProcess : 'All active rules'}

You will receive a completion notification when the job finishes.`;
      
      // HTML version if enabled
      let htmlBody = "";
      if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
        htmlBody = `
<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }
    h2 { color: #0066CC; border-bottom: 1px solid #0066CC; padding-bottom: 10px; }
    .button { display: inline-block; padding: 10px 20px; background-color: #0066CC; color: white; 
              text-decoration: none; border-radius: 5px; margin-top: 20px; }
  </style>
</head>
<body>
  <h2>Data Ingest Job Started</h2>
  <p><strong>Time:</strong> ${new Date().toLocaleString()}</p>
  <p><strong>Session ID:</strong> ${sessionId}</p>
  <p><strong>Spreadsheet:</strong> ${spreadsheetName}</p>
  <p><strong>Rules to process:</strong> ${rulesToProcess ? rulesToProcess : 'All active rules'}</p>
  <p>You will receive a completion notification when the job finishes.</p>
  <p><a href="${spreadsheetUrl}" class="button">Open Spreadsheet</a></p>
  <p style="color: #999; font-size: 12px; margin-top: 30px;">
    This is an automated message from the Data Ingest System.<br>
    Please do not reply to this email.
  </p>
</body>
</html>
`;
      }
      
      // Handle both single email and array of emails
      const emails = Array.isArray(CONFIG.EMAIL_NOTIFICATIONS) 
        ? CONFIG.EMAIL_NOTIFICATIONS 
        : [CONFIG.EMAIL_NOTIFICATIONS];
      
      Logger.log(`Sending start notification to ${emails.length} recipient(s): ${emails.join(', ')}`);
      
      // Send to each email
      emails.forEach(email => {
        if (email && email.trim() !== '') {
          GmailApp.sendEmail(email, subject, emailBody, {
            htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : null
          });
          Logger.log(`Start notification sent to: ${email}`);
        }
      });
    } else {
      Logger.log(`No notification recipients configured, skipping start notification`);
    }
  } catch (error) {
    Logger.log(`ERROR SENDING START NOTIFICATION: ${error.message}`);
  }
}

/**
 * Sends a detailed run summary email after completion
 * @param {string} sessionId - Session identifier
 * @param {string} status - Overall status of the run (COMPLETE, ERROR, TEST)
 * @param {Array} results - Array of result objects for each rule
 */
function sendRunSummaryEmail(sessionId, status, results) {
  Logger.log(`Preparing to send run summary email for session: ${sessionId}`);
  
  try {
    if (CONFIG.EMAIL_NOTIFICATIONS && CONFIG.EMAIL_NOTIFICATIONS.length > 0) {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const spreadsheetUrl = ss.getUrl();
      const spreadsheetName = ss.getName();
      
      // Count successes, errors, skipped
      const successCount = results.filter(r => r.status === "SUCCESS").length;
      const errorCount = results.filter(r => r.status === "ERROR").length;
      const skippedCount = results.filter(r => r.status === "SKIPPED").length;
      
      // Calculate total rows processed
      const totalRowsProcessed = results.reduce((sum, r) => sum + (r.rowsProcessed || 0), 0);
      const totalRowsExpected = results.reduce((sum, r) => sum + (r.rowsExpected || 0), 0);
      const runDuration = results.reduce((sum, r) => sum + (r.duration || 0), 0).toFixed(1);
      
      // Create email subject
      let subject = "";
      if (CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX) {
        subject += CONFIG.EMAIL_CONFIG.EMAIL_SUBJECT_PREFIX + " ";
      }
      
      if (status === "TEST") {
        subject += "Test Report";
      } else {
        subject += errorCount > 0 ? "Run Summary - ERRORS" : "Run Summary - Success";
      }
      
      subject += ` - ${spreadsheetName}`;
      
      // Create plain text version
      let emailBody = `Data Ingest Run Summary:
      
Time: ${new Date().toLocaleString()}
Session ID: ${sessionId}
Spreadsheet: ${spreadsheetName}
Duration: ${runDuration}s

Results:
- Total rules processed: ${results.length}
- Successful: ${successCount}
- Errors: ${errorCount}
- Skipped: ${skippedCount}
- Total rows processed: ${totalRowsProcessed.toLocaleString()} of ${totalRowsExpected.toLocaleString()}

${errorCount > 0 ? "ERRORS OCCURRED - Please check the details below or in the log sheet." : "All operations completed successfully."}
`;
      
      // Add rule details to plain text
      if (results.length > 0) {
        emailBody += "\n\nRule Details:\n";
        results.forEach(r => {
          emailBody += `\n${r.ruleName} - ${r.status}`;
          if (r.status === "SUCCESS") {
            emailBody += ` - ${r.rowsProcessed.toLocaleString()} rows in ${r.duration ? r.duration.toFixed(1) : 0}s`;
          } else if (r.status === "ERROR") {
            emailBody += ` - Error: ${r.message}`;
          }
        });
      }
      
      emailBody += `\n\nPlease check the log sheet for more details.`;
      
      // HTML version if enabled
      let htmlBody = "";
      if (CONFIG.EMAIL_CONFIG.HTML_FORMATTING) {
        // Start of HTML content with improved styling
        htmlBody = `
<html>
<head>
  <style>
    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; color: #333; }
    h2 { color: #0066CC; border-bottom: 1px solid #0066CC; padding-bottom: 10px; }
    h3 { color: #333; border-bottom: 1px solid #ddd; padding-bottom: 5px; margin-top: 20px; }
    table { border-collapse: collapse; width: 100%; margin: 20px 0; }
    th { background-color: #f2f2f2; text-align: left; padding: 8px; }
    td { padding: 8px; border: 1px solid #ddd; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    .error { color: #CC0000; font-weight: bold; }
    .info { color: #0066CC; }
    .success { color: #007700; font-weight: bold; }
    .warning { color: #FF9900; }
    .button { display: inline-block; padding: 10px 20px; background-color: #0066CC; color: white; 
              text-decoration: none; border-radius: 5px; margin-top: 20px; }
    .summary-card { border: 1px solid #ddd; border-radius: 5px; padding: 15px; margin: 20px 0; 
                   background-color: #f9f9f9; }
    .progress-bar-container { width: 100%; background-color: #f1f1f1; border-radius: 5px; margin: 10px 0; }
    .progress-bar { background-color: #4CAF50; height: 24px; border-radius: 5px; text-align: center; 
                   line-height: 24px; color: white; }
  </style>
</head>
<body>
  <h2>Data Ingest ${status === "TEST" ? "Test Report" : "Run Summary"}</h2>
  <p><strong>Time:</strong> ${new Date().toLocaleString()}</p>
  <p><strong>Session ID:</strong> ${sessionId}</p>
  <p><strong>Spreadsheet:</strong> ${spreadsheetName}</p>
  <p><strong>Total Duration:</strong> ${runDuration}s</p>

  <div class="summary-card">
    <h3>Results Summary</h3>
    <table border="0" cellpadding="5">
      <tr>
        <td><strong>Total rules processed:</strong></td>
        <td>${results.length}</td>
      </tr>
      <tr>
        <td><strong>Successful:</strong></td>
        <td class="success">${successCount}</td>
      </tr>
      <tr>
        <td><strong>Errors:</strong></td>
        <td ${errorCount > 0 ? 'class="error"' : ''}>${errorCount}</td>
      </tr>
      <tr>
        <td><strong>Skipped:</strong></td>
        <td>${skippedCount}</td>
      </tr>
      <tr>
        <td><strong>Total rows processed:</strong></td>
        <td>${totalRowsProcessed.toLocaleString()} of ${totalRowsExpected.toLocaleString()}</td>
      </tr>
    </table>
    
    <!-- Add progress bar for completion percentage -->
    <div class="progress-bar-container">
      <div class="progress-bar" style="width: ${totalRowsExpected > 0 ? Math.round((totalRowsProcessed / totalRowsExpected) * 100) : 100}%">
        ${totalRowsExpected > 0 ? Math.round((totalRowsProcessed / totalRowsExpected) * 100) : 100}% Complete
      </div>
    </div>
  </div>

  <p class="${errorCount > 0 ? 'error' : 'success'}">
    ${errorCount > 0 ? 'ERRORS OCCURRED - Please check the details below or in the log sheet.' : 
                      'All operations completed successfully.'}
  </p>

  <h3>Rule Details</h3>
  <table border="1" cellpadding="5" style="border-collapse: collapse;">
    <tr style="background-color: #f2f2f2;">
      <th>Rule</th>
      <th>Status</th>
      <th>Rows Processed</th>
      <th>Rows Expected</th>
      <th>Completion</th>
      <th>Duration</th>
      <th>Message</th>
    </tr>
    ${results.map(r => `
    <tr ${r.status === 'ERROR' ? 'style="background-color: #ffcccc;"' : 
         (r.status === 'SKIPPED' ? 'style="background-color: #ffffcc;"' : '')}>
      <td>${r.ruleName}</td>
      <td class="${r.status === 'SUCCESS' ? 'success' : 
                  r.status === 'ERROR' ? 'error' : 
                  r.status === 'SKIPPED' ? 'warning' : 'info'}">${r.status}</td>
      <td align="right">${(r.rowsProcessed || 0).toLocaleString()}</td>
      <td align="right">${(r.rowsExpected || 0).toLocaleString()}</td>
      <td align="right">${r.rowsExpected > 0 ? Math.round((r.rowsProcessed / r.rowsExpected) * 100) : 0}%</td>
      <td align="right">${r.duration ? r.duration.toFixed(1) : 0}s</td>
      <td>${r.message || ''}</td>
    </tr>
    `).join('')}
  </table>
`;

        // Add verification data if available and enabled
        if (sessionId && CONFIG.VERIFICATION_CONFIG.ENABLED) {
          try {
            const verificationData = getVerificationDataForSession(sessionId);
            if (verificationData && verificationData.length > 0) {
              htmlBody += `
  <h3>Data Verification Results</h3>
  <table border="1" cellpadding="5" style="border-collapse: collapse;">
    <tr style="background-color: #f2f2f2;">
      <th>Rule ID</th>
      <th>Source</th>
      <th>Destination</th>
      <th>Source Rows</th>
      <th>Dest Rows</th>
      <th>Status</th>
      <th>Verification</th>
    </tr>
    ${verificationData.map(v => `
    <tr ${v.status === 'ERROR' ? 'style="background-color: #ffcccc;"' : ''}>
      <td>${v.ruleId || ''}</td>
      <td>${v.sourceFile || ''}</td>
      <td>${v.destinationSheet || ''}</td>
      <td align="right">${v.sourceRowCount || 0}</td>
      <td align="right">${v.destinationRowCount || 0}</td>
      <td class="${v.status === 'COMPLETE' ? 'success' : 'error'}">${v.status || ''}</td>
      <td>
        Rows: ${v.rowsMatch === 'YES' ? '✅' : '❌'}, 
        Columns: ${v.columnsMatch === 'YES' ? '✅' : '❌'}, 
        Samples: ${v.samplesMatch === 'YES' ? '✅' : '❌'}
      </td>
    </tr>
    `).join('')}
  </table>
`;
            }
          } catch (verificationError) {
            // Don't let verification errors stop email generation
            Logger.log(`Error getting verification data for email: ${verificationError.message}`);
          }
        }

        // Add log entries if available
        if (sessionId) {
          const logEntries = getLogEntriesForSession(sessionId);
          if (logEntries && logEntries.length > 0) {
            htmlBody += `
  <h3>Recent Log Entries</h3>
  <p><em>Showing the ${logEntries.length} most recent log entries for this session</em></p>
  <table border="1" cellpadding="5" style="border-collapse: collapse;">
    <tr style="background-color: #f2f2f2;">
      <th>Timestamp</th>
      <th>Event Type</th>
      <th>Message</th>
    </tr>
    ${logEntries.map(entry => `
    <tr ${entry.eventType === 'ERROR' ? 'style="background-color: #ffeeee;"' : ''}>
      <td>${formatDate(entry.timestamp)}</td>
      <td ${entry.eventType === 'ERROR' ? 'class="error"' : 
           entry.eventType === 'SUCCESS' ? 'class="success"' : 
           entry.eventType === 'WARNING' ? 'class="warning"' : 'class="info"'}>
        ${entry.eventType}
      </td>
      <td>${entry.message}</td>
    </tr>
    `).join('')}
  </table>
`;
          }
        }
        
        // Add link to spreadsheet and footer
        htmlBody += `
  <p><a href="${spreadsheetUrl}" class="button">Open Spreadsheet</a></p>
  
  <p style="color: #999; font-size: 12px; margin-top: 30px; border-top: 1px solid #ddd; padding-top: 10px;">
    This is an automated message from the Data Ingest System.<br>
    Please do not reply to this email.<br>
    Generated on ${new Date().toLocaleString()}
  </p>
</body>
</html>
`;
      }
      
      // Handle both single email and array of emails
      const emails = Array.isArray(CONFIG.EMAIL_NOTIFICATIONS) 
        ? CONFIG.EMAIL_NOTIFICATIONS 
        : [CONFIG.EMAIL_NOTIFICATIONS];
      
      Logger.log(`Sending run summary to ${emails.length} recipient(s): ${emails.join(', ')}`);
      
      // Prepare attachments if configured
      let attachments = [];
      
      if (CONFIG.EMAIL_CONFIG.INCLUDE_LOG_ATTACHMENT) {
        const logAttachment = createLogAttachment(sessionId);
        if (logAttachment) {
          attachments.push(logAttachment);
          Logger.log(`Added log attachment to email: ${logAttachment.getName()}`);
        }
      }
      
      if (CONFIG.EMAIL_CONFIG.INCLUDE_VERIFICATION_ATTACHMENT) {
        const verificationAttachment = createVerificationAttachment(sessionId);
        if (verificationAttachment) {
          attachments.push(verificationAttachment);
          Logger.log(`Added verification attachment to email: ${verificationAttachment.getName()}`);
        }
      }
      
      // Send to each email
      emails.forEach(email => {
        if (email && email.trim() !== '') {
          GmailApp.sendEmail(email, subject, emailBody, {
            htmlBody: CONFIG.EMAIL_CONFIG.HTML_FORMATTING ? htmlBody : null,
            attachments: attachments
          });
          Logger.log(`Run summary sent to: ${email}`);
        }
      });
      
      // Log that we sent the summary
      logOperation(sessionId, "EMAIL_SENT", 
        `Run summary email sent to ${emails.length} recipient(s) with ${attachments.length} attachments`);
      
    } else {
      Logger.log(`No notification recipients configured, skipping summary email`);
    }
  } catch (error) {
    Logger.log(`ERROR SENDING SUMMARY EMAIL: ${error.message}`);
    // Try to log the error to the log sheet
    try {
      logOperation(sessionId, "EMAIL_ERROR", `Failed to send summary email: ${error.message}`);
    } catch (logError) {
      // If we can't log, just continue
      Logger.log(`Also failed to log the email error: ${logError.message}`);
    }
  }
}

/**
 * Gets verification data for a session
 * @param {string} sessionId - Session identifier
 * @returns {Array} Array of verification data objects
 */
function getVerificationDataForSession(sessionId) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
    
    if (!verificationSheet) {
      Logger.log(`Verification sheet not found`);
      return [];
    }
    
    // Get all data from the verification sheet
    const allData = verificationSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No verification entries found (empty sheet)`);
      return [];
    }
    
    // Get headers
    const headers = allData[0];
    const sessionIdIdx = headers.indexOf("SessionID");
    
    if (sessionIdIdx === -1) {
      Logger.log(`Verification sheet is missing SessionID column`);
      return [];
    }
    
    // Extract indices for all needed fields
    const fieldIndices = {
      timestamp: headers.indexOf("Timestamp"),
      ruleId: headers.indexOf("RuleID"),
      sourceType: headers.indexOf("SourceType"),
      sourceFile: headers.indexOf("SourceFile"),
      destinationSheet: headers.indexOf("DestinationSheet"),
      sourceRowCount: headers.indexOf("SourceRows"),
      destinationRowCount: headers.indexOf("DestRows"),
      sourceColumnCount: headers.indexOf("SourceColumns"),
      destinationColumnCount: headers.indexOf("DestColumns"),
      rowsMatch: headers.indexOf("RowsMatch"),
      columnsMatch: headers.indexOf("ColumnsMatch"),
      samplesMatch: headers.indexOf("SamplesMatch"),
      dataHash: headers.indexOf("DataHash"),
      status: headers.indexOf("Status"),
      details: headers.indexOf("Details")
    };
    
    // Filter for the requested session
    const sessionEntries = allData.filter((row, index) => index > 0 && row[sessionIdIdx] === sessionId);
    
    // Convert to objects
    return sessionEntries.map(row => {
      const entry = {};
      
      // Add each field if the column exists
      Object.entries(fieldIndices).forEach(([field, index]) => {
        if (index !== -1) {
          entry[field] = row[index];
        }
      });
      
      return entry;
    });
  } catch (error) {
    Logger.log(`Error getting verification data: ${error.message}`);
    return [];
  }
}

/**
 * Gets diagnostic data for a session
 * @param {string} sessionId - Session identifier
 * @returns {Array} Array of diagnostic data objects
 */
function getDiagnosticDataForSession(sessionId) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);
    
    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet not found`);
      return [];
    }
    
    // Get all data from the diagnostic sheet
    const allData = diagnosticSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No diagnostic entries found (empty sheet)`);
      return [];
    }
    
    // Get headers
    const headers = allData[0];
    const sessionIdIdx = headers.indexOf("SessionID");
    
    if (sessionIdIdx === -1) {
      Logger.log(`Diagnostic sheet is missing SessionID column`);
      return [];
    }
    
    // Filter for the requested session
    const sessionEntries = allData.filter((row, index) => index > 0 && row[sessionIdIdx] === sessionId);
    
    // Convert to objects
    return sessionEntries.map(row => {
      return {
        timestamp: row[headers.indexOf("Timestamp")],
        sessionId: row[sessionIdIdx],
        position: row[headers.indexOf("Position")],
        column: row[headers.indexOf("Column")],
        sourceValue: row[headers.indexOf("SourceValue")],
        sourceType: row[headers.indexOf("SourceType")],
        destValue: row[headers.indexOf("DestValue")],
        destType: row[headers.indexOf("DestType")],
        normalizedSource: row[headers.indexOf("NormalizedSource")],
        normalizedDest: row[headers.indexOf("NormalizedDest")],
        details: row[headers.indexOf("Details")]
      };
    });
  } catch (error) {
    Logger.log(`Error getting diagnostic data: ${error.message}`);
    return [];
  }
}

/**
 * Create a CSV attachment of log entries for the session
 * @param {string} sessionId - Session identifier
 * @returns {Blob} Blob containing the log data as CSV
 */
function createLogAttachment(sessionId) {
  try {
    Logger.log(`Creating log attachment for session: ${sessionId}`);
    
    const logEntries = getLogEntriesForSession(sessionId);
    if (!logEntries || logEntries.length === 0) {
      Logger.log(`No log entries found for session: ${sessionId}`);
      
      // Return a minimal log file even if no entries were found
      const minimalContent = "Timestamp,SessionID,EventType,Message\n" +
                           `"${formatDate(new Date())}","${sessionId}","NO_LOGS_FOUND","No log entries were found for this session"`;
      
      return Utilities.newBlob(minimalContent, "text/csv", `ingest_log_${sessionId}.csv`);
    }
    
    // Create CSV content
    let csvContent = "Timestamp,SessionID,EventType,Message\n";
    
    logEntries.forEach(entry => {
      const timestamp = formatDate(entry.timestamp);
      // Escape quotes and commas in the message field
      const message = entry.message.replace(/"/g, '""').replace(/\n/g, ' ');
      csvContent += `"${timestamp}","${sessionId}","${entry.eventType}","${message}"\n`;
    });
    
    // Create the attachment
    const attachment = Utilities.newBlob(csvContent, "text/csv", `ingest_log_${sessionId}.csv`);
    Logger.log(`Created log attachment with ${logEntries.length} entries`);
    
    return attachment;
  } catch (error) {
    Logger.log(`ERROR CREATING LOG ATTACHMENT: ${error.message}`);
    // Create an error report CSV
    const errorContent = "Timestamp,SessionID,EventType,Message\n" +
                      `"${formatDate(new Date())}","${sessionId}","ATTACHMENT_ERROR","Failed to create log attachment: ${error.message}"`;
    
    return Utilities.newBlob(errorContent, "text/csv", `ingest_log_${sessionId}_error.csv`);
  }
}

/**
 * Create a CSV attachment of verification entries for the session
 * @param {string} sessionId - Session identifier
 * @returns {Blob} Blob containing the verification data as CSV
 */
function createVerificationAttachment(sessionId) {
  try {
    Logger.log(`Creating verification attachment for session: ${sessionId}`);
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
    
    if (!verificationSheet) {
      Logger.log(`Verification sheet not found`);
      return null;
    }
    
    // Get all data from the verification sheet
    const allData = verificationSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No verification entries found (empty sheet)`);
      return null;
    }
    
    // Get headers and indices
    const headers = allData[0];
    const sessionIdIdx = headers.indexOf("SessionID");
    
    if (sessionIdIdx === -1) {
      Logger.log(`Verification sheet is missing SessionID column`);
      return null;
    }
    
    // Filter for the requested session
    const sessionEntries = allData.filter((row, index) => index > 0 && row[sessionIdIdx] === sessionId);
    
    if (sessionEntries.length === 0) {
      Logger.log(`No verification entries found for session: ${sessionId}`);
      
      // Return a minimal file even if no entries were found
      const minimalHeaders = headers.join('","');
      const minimalContent = `"${minimalHeaders}"\n` +
                           `"${formatDate(new Date())}","${sessionId}","NO_DATA","","","","","","","","","","","","NO_VERIFICATIONS_FOUND","No verification data found for this session"`;
      
      return Utilities.newBlob(minimalContent, "text/csv", `ingest_verification_${sessionId}.csv`);
    }
    
    // Create CSV content with headers first
    let csvContent = `"${headers.join('","')}"\n`;
    
    // Add each entry
    sessionEntries.forEach(row => {
      // Format each value and handle special characters
      const formattedRow = row.map(value => {
        if (value instanceof Date) {
          return formatDate(value);
        } else if (typeof value === 'string') {
          return value.replace(/"/g, '""').replace(/\n/g, ' ');
        } else {
          return value;
        }
      });
      
      csvContent += `"${formattedRow.join('","')}"\n`;
    });
    
    // Create the attachment
    const attachment = Utilities.newBlob(csvContent, "text/csv", `ingest_verification_${sessionId}.csv`);
    Logger.log(`Created verification attachment with ${sessionEntries.length} entries`);
    
    return attachment;
  } catch (error) {
    Logger.log(`ERROR CREATING VERIFICATION ATTACHMENT: ${error.message}`);
    
    // Create an error report CSV
    const errorContent = "Timestamp,SessionID,Error\n" +
                      `"${formatDate(new Date())}","${sessionId}","Failed to create verification attachment: ${error.message}"`;
    
    return Utilities.newBlob(errorContent, "text/csv", `ingest_verification_${sessionId}_error.csv`);
  }
}

/**
 * Create a CSV attachment of diagnostic entries for the session
 * @param {string} sessionId - Session identifier
 * @returns {Blob} Blob containing the diagnostic data as CSV
 */
function createDiagnosticAttachment(sessionId) {
  try {
    Logger.log(`Creating diagnostic attachment for session: ${sessionId}`);
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);
    
    if (!diagnosticSheet) {
      Logger.log(`Diagnostic sheet not found`);
      return null;
    }
    
    // Get all data from the verification sheet
    const allData = diagnosticSheet.getDataRange().getValues();
    if (allData.length <= 1) {
      Logger.log(`No diagnostic entries found (empty sheet)`);
      return null;
    }
    
    // Get headers and indices
    const headers = allData[0];
    const sessionIdIdx = headers.indexOf("SessionID");
    
    if (sessionIdIdx === -1) {
      Logger.log(`Diagnostic sheet is missing SessionID column`);
      return null;
    }
    
    // Filter for the requested session
    const sessionEntries = allData.filter((row, index) => index > 0 && row[sessionIdIdx] === sessionId);
    
    if (sessionEntries.length === 0) {
      Logger.log(`No diagnostic entries found for session: ${sessionId}`);
      return null;
    }
    
    // Create CSV content with headers first
    let csvContent = `"${headers.join('","')}"\n`;
    
    // Add each entry
    sessionEntries.forEach(row => {
      // Format each value and handle special characters
      const formattedRow = row.map(value => {
        if (value instanceof Date) {
          return formatDate(value);
        } else if (typeof value === 'string') {
          return value.replace(/"/g, '""').replace(/\n/g, ' ');
        } else {
          return value;
        }
      });
      
      csvContent += `"${formattedRow.join('","')}"\n`;
    });
    
    // Create the attachment
    const attachment = Utilities.newBlob(csvContent, "text/csv", `ingest_diagnostic_${sessionId}.csv`);
    Logger.log(`Created diagnostic attachment with ${sessionEntries.length} entries`);
    
    return attachment;
  } catch (error) {
    Logger.log(`ERROR CREATING DIAGNOSTIC ATTACHMENT: ${error.message}`);
    return null;
  }
}

/**
 * Retrieves log entries for a specific session
 * @param {string} sessionId - Session identifier
 * @param {number} maxEntries - Maximum number of entries to return (optional)
 * @returns {Array} Array of log entry objects
 */
function getLogEntriesForSession(sessionId, maxEntries) {
  try {
    Logger.log(`Retrieving log entries for session: ${sessionId}`);
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
    
    if (!logSheet) {
      Logger.log(`Log sheet not found`);
      return [];
    }
    
    // Get log data
    const logData = logSheet.getDataRange().getValues();
    if (logData.length <= 1) {
      Logger.log(`No log entries found (empty log)`);
      return [];
    }
    
    // Get headers
    const headers = logData[0];
    const timestampIdx = headers.indexOf("Timestamp");
    const sessionIdIdx = headers.indexOf("SessionID");
    const eventTypeIdx = headers.indexOf("EventType");
    const messageIdx = headers.indexOf("Message");
    
    if (timestampIdx === -1 || sessionIdIdx === -1 || eventTypeIdx === -1 || messageIdx === -1) {
      Logger.log(`Log sheet is missing required columns`);
      return [];
    }
    
    // Filter entries by session ID and convert to objects
    const sessionEntries = [];
    for (let i = 1; i < logData.length; i++) {
      const row = logData[i];
      
      if (row[sessionIdIdx] === sessionId) {
        sessionEntries.push({
          timestamp: row[timestampIdx],
          eventType: row[eventTypeIdx],
          message: row[messageIdx]
        });
      }
    }
    
    Logger.log(`Found ${sessionEntries.length} log entries for session: ${sessionId}`);
    
    // Sort by timestamp (latest first) and limit to max rows
    sessionEntries.sort((a, b) => b.timestamp - a.timestamp);
    
    const limitEntries = maxEntries || CONFIG.EMAIL_CONFIG.MAX_ROWS_IN_EMAIL;
    return sessionEntries.slice(0, limitEntries);
  } catch (error) {
    Logger.log(`ERROR RETRIEVING LOG ENTRIES: ${error.message}`);
    return [];
  }
}

/**
 * Format a date object for display
 * @param {Date} date - The date to format
 * @param {string} format - Optional format string
 * @returns {string} Formatted date string
 */
function formatDate(date, format) {
  if (!date) return "";
  
  // If it's already a string, return it
  if (typeof date === 'string') {
    return date;
  }
  
  try {
    return Utilities.formatDate(date, Session.getScriptTimeZone(), 
                              format || CONFIG.SHEET_FORMATS.LOG_SHEET.timestampFormat || "MM/dd/yyyy HH:mm:ss");
  } catch (error) {
    return date.toString();
  }
}

/**
 * Calculate a hash value from data for verification
 * @param {Array} data - 2D array of data
 * @returns {string} Hash value of the data
 */
function calculateDataHash(data) {
  if (!data || data.length === 0) {
    return "empty";
  }
  
  try {
    // Create a string representation of the data
    // For large datasets, we'll just sample some rows for efficiency
    const MAX_SAMPLE_SIZE = 100; // Maximum number of rows to include in hash
    const rowsToSample = data.length > MAX_SAMPLE_SIZE ? 
                        [
                          ...data.slice(0, 10), // First 10 rows
                          ...data.slice(Math.floor(data.length / 2) - 5, Math.floor(data.length / 2) + 5), // 10 rows from middle
                          ...data.slice(data.length - 10) // Last 10 rows
                        ] : 
                        data;
    
    // Join rows into a string representation
    const dataString = rowsToSample.map(row => row.join('|')).join('\n');
    
    // Use simple hash for efficiency (this can be replaced with a more robust algorithm if needed)
    let hash = 0;
    for (let i = 0; i < dataString.length; i++) {
      const char = dataString.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    
    // Convert to hex string and add data length as additional validation
    return `${Math.abs(hash).toString(16)}-r${data.length}-c${data[0].length}`;
  } catch (error) {
    Logger.log(`ERROR CALCULATING DATA HASH: ${error.message}`);
    return "error";
  }
}

/**
 * Executes a function with retries if it fails
 * @param {Function} fn - The function to execute
 * @param {number} maxRetries - Maximum number of retry attempts
 * @param {number} initialDelay - Initial delay in milliseconds before retrying
 * @returns {*} The result of the function
 */
function executeWithRetry(fn, maxRetries = 3, initialDelay = 1000) {
  let retries = 0;
  let delay = initialDelay;
  
  while (true) {
    try {
      return fn(); // Try to execute the function
    } catch (error) {
      retries++;
      
      // If we've reached max retries or it's not a temporary error, rethrow
      if (retries >= maxRetries || !isTemporaryError(error)) {
        throw error;
      }
      
      // Log the retry attempt
      Logger.log(`Temporary error encountered: ${error.message}. Retrying in ${delay}ms (attempt ${retries}/${maxRetries})`);
      
      // Wait before retrying
      Utilities.sleep(delay);
      
      // Exponential backoff - double the delay for next retry
      delay *= 2;
    }
  }
}

/**
 * Checks if an error is likely to be temporary
 * @param {Error} error - The error to check
 * @returns {boolean} True if the error is likely temporary
 */
function isTemporaryError(error) {
  const message = error.message.toLowerCase();
  
  // Common temporary error patterns
  const temporaryPatterns = [
    'timeout', 
    'rate limit', 
    'quota',
    'service unavailable',
    'internal error',
    'server error',
    'try again',
    'too many requests',
    'backend error',
    'temporarily unavailable'
  ];
  
  return temporaryPatterns.some(pattern => message.includes(pattern));
}

/**
 * Creates or updates a progress indicator in the spreadsheet
 * @param {string} operation - The operation being performed
 * @param {number} progress - Progress percentage (0-100)
 * @param {string} status - Status message
 */
function updateProgressIndicator(operation, progress, status) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let progressSheet = ss.getSheetByName('_progress');
    
    if (!progressSheet) {
      // Create progress sheet if it doesn't exist
      progressSheet = ss.insertSheet('_progress');
      
      // Set up the progress sheet
      progressSheet.deleteColumns(2, progressSheet.getMaxColumns() - 1);
      progressSheet.deleteRows(2, progressSheet.getMaxRows() - 1);
      progressSheet.setColumnWidth(1, 400);
      
      // Hide the sheet (it will still be visible when active)
      progressSheet.hideSheet();
    }
    
    // Update the progress information
    progressSheet.getRange(1, 1).setValue(`Operation: ${operation} - Progress: ${progress}% - Status: ${status}`);
    
    // Create a temporary trigger to remove the progress sheet when done
    if (progress >= 100) {
      // Schedule deletion of progress sheet after 30 seconds
      ScriptApp.newTrigger('removeProgressSheet')
        .timeBased()
        .after(30000) // 30 seconds
        .create();
    }
  } catch (error) {
    // Just log errors - don't let progress indicator issues affect the main process
    Logger.log(`Error updating progress indicator: ${error.message}`);
  }
}

/**
 * Removes the progress indicator sheet
 */
function removeProgressSheet() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const progressSheet = ss.getSheetByName('_progress');
    
    if (progressSheet) {
      ss.deleteSheet(progressSheet);
    }
    
    // Delete all triggers created by this script
    const triggers = ScriptApp.getProjectTriggers();
    for (const trigger of triggers) {
      if (trigger.getHandlerFunction() === 'removeProgressSheet') {
        ScriptApp.deleteTrigger(trigger);
      }
    }
  } catch (error) {
    Logger.log(`Error removing progress sheet: ${error.message}`);
  }
}

/**
 * Generates a unique ID for tracking operations
 * @returns {string} A unique identifier
 */
function generateUniqueID() {
  const timestamp = new Date().getTime();
  const randomNum = Math.floor((Math.random() * 1000000) + 1);
  const id = `${timestamp}-${randomNum}`;
  Logger.log(`Generated unique ID: ${id}`);
  return id;
}

/**
 * Gets a required value from a row, throwing an error if not found
 * @param {Array} row - Row of data
 * @param {Object} headerMap - Map of header names to column indices
 * @param {string} internalColumnName - Internal name of the column
 * @returns {*} The value from the row
 */
function getRequiredValue(row, headerMap, internalColumnName) {
  // Map the internal column name to the user-defined header
  const columnName = CONFIG.COLUMN_MAPPINGS[internalColumnName] || internalColumnName;
  
  Logger.log(`Getting required value for column: ${columnName} (internal name: ${internalColumnName})`);
  
  if (!(columnName in headerMap)) {
    const errorMsg = `Required column "${columnName}" not found in configuration`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
  
  const value = row[headerMap[columnName]];
  if (value === undefined || value === null || value === '') {
    const errorMsg = `Required value for "${columnName}" is missing`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
  
  Logger.log(`Retrieved value: ${value}`);
  return value;
}

/**
 * Extracts a resource ID either directly or from a URL
 * @param {Array} row - Row of data
 * @param {Object} headerMap - Map of header names to column indices
 * @param {string} idInternalColumnName - Internal name of the ID column
 * @param {string} urlInternalColumnName - Internal name of the URL column
 * @returns {string} The extracted resource ID
 */
function getResourceId(row, headerMap, idInternalColumnName, urlInternalColumnName) {
  // Map the internal column names to the user-defined headers
  const idColumnName = CONFIG.COLUMN_MAPPINGS[idInternalColumnName] || idInternalColumnName;
  const urlColumnName = CONFIG.COLUMN_MAPPINGS[urlInternalColumnName] || urlInternalColumnName;
  
  Logger.log(`Getting resource ID from columns: ${idColumnName} or ${urlColumnName}`);
  
  // First try to get the ID directly
  if (idColumnName in headerMap) {
    const id = row[headerMap[idColumnName]];
    if (id && id.trim() !== '') {
      Logger.log(`Found direct ID: ${id}`);
      return id;
    }
  }
  
  // If no ID, try to extract from URL
  if (urlColumnName in headerMap) {
    const url = row[headerMap[urlColumnName]];
    if (url && url.trim() !== '') {
      Logger.log(`Attempting to extract ID from URL: ${url}`);
      
      // Try different URL formats:
      
      // Format 1: Standard Google Sheets URL
      // https://docs.google.com/spreadsheets/d/SHEET_ID/edit
      let match = url.match(/\/d\/([^\/]+)/);
      if (match && match[1]) {
        Logger.log(`Extracted ID from standard Google Sheets URL: ${match[1]}`);
        return match[1];
      }
      
      // Format 2: Direct ID that looks like a URL but isn't
      // https://SHEET_ID
      match = url.match(/^https?:\/\/([a-zA-Z0-9_-]+)$/);
      if (match && match[1]) {
        Logger.log(`Extracted ID from simple URL: ${match[1]}`);
        return match[1];
      }
      
      // Format 3: Just the ID
      if (/^[a-zA-Z0-9_-]+$/.test(url)) {
        Logger.log(`URL appears to be just the ID: ${url}`);
        return url;
      }
      
      Logger.log(`Could not extract ID from URL using any known pattern`);
    }
  }
  
  const errorMsg = `Could not find resource ID from either ${idColumnName} or ${urlColumnName}`;
  Logger.log(errorMsg);
  throw new Error(errorMsg);
}

/**
 * Creates a map of header names to column indices
 * @param {Array} headers - Array of header names
 * @returns {Object} Map of header names to column indices
 */
function createHeaderMap(headers) {
  const headerMap = {};
  headers.forEach((header, index) => {
    headerMap[header] = index;
  });
  Logger.log(`Created header map with ${Object.keys(headerMap).length} entries`);
  return headerMap;
}

/**
 * Validates the configuration sheet
 */
function validateConfiguration() {
  Logger.log("Starting configuration validation");
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
  
  if (!configSheet) {
    Logger.log("Configuration sheet not found");
    ui.alert('Error', 'Configuration sheet not found. Please create it first.', ui.ButtonSet.OK);
    return;
  }
  
  const configData = configSheet.getDataRange().getValues();
  Logger.log(`Loaded configuration with ${configData.length} rows (including header)`);
  
  const headers = configData[0];
  const headerMap = createHeaderMap(headers);
  
  // Verify required headers
  const requiredHeaders = [CONFIG.COLUMN_MAPPINGS.ruleActive, CONFIG.COLUMN_MAPPINGS.ingestMethod];
  const missingHeaders = requiredHeaders.filter(header => !(header in headerMap));
  
  if (missingHeaders.length > 0) {
    Logger.log(`Missing required headers: ${missingHeaders.join(', ')}`);
    ui.alert('Error', `The configuration sheet is missing required columns: ${missingHeaders.join(', ')}`, ui.ButtonSet.OK);
    return;
  }
  
  // Validate each active row
  let errors = [];
  let warnings = [];
  let validRules = 0;
  
  for (let i = 1; i < configData.length; i++) {
    const row = configData[i];
    const rowNum = i + 1;
    
    // Skip empty rows
    if (!isRowPopulated(row)) {
      Logger.log(`Row ${rowNum} is empty, skipping validation`);
      continue;
    }
    
    Logger.log(`Validating row ${rowNum}`);
    
    const ruleActive = row[headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive]];
    
    // Skip inactive rules
    if (ruleActive !== true) {
      Logger.log(`Row ${rowNum} is not active, skipping detailed validation`);
      continue;
    }
    
    validRules++;
    const ingestMethod = row[headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod]];
    Logger.log(`Row ${rowNum} is active with method: ${ingestMethod}`);
    
    // Validate based on ingest method
    if (ingestMethod === 'email') {
      validateEmailRule(row, headerMap, rowNum, errors, warnings);
    } else if (ingestMethod === 'gSheet') {
      validateGSheetRule(row, headerMap, rowNum, errors, warnings);
    } else if (ingestMethod === 'push') {
      validatePushRule(row, headerMap, rowNum, errors, warnings);
    } else if (!ingestMethod) {
      const errorMsg = `Row ${rowNum}: Missing required 'ingestMethod' value`;
      Logger.log(errorMsg);
      errors.push(errorMsg);
    } else {
      const errorMsg = `Row ${rowNum}: Unknown ingest method: ${ingestMethod}`;
      Logger.log(errorMsg);
      errors.push(errorMsg);
    }
    
    // Check sheet handling mode for all methods
    validateSheetHandlingMode(row, headerMap, rowNum, warnings);
  }
  
  // Display validation results
  if (errors.length === 0 && warnings.length === 0) {
    const message = validRules > 0 
      ? `The configuration is valid with ${validRules} active rule(s).` 
      : 'The configuration has no active rules.';
      
    Logger.log(`Validation successful: ${message}`);
    ui.alert('Validation Successful', message, ui.ButtonSet.OK);
  } else {
    let message = '';
    
    if (errors.length > 0) {
      message += 'ERRORS:\n' + errors.join('\n') + '\n\n';
    }
    
    if (warnings.length > 0) {
      message += 'WARNINGS:\n' + warnings.join('\n');
    }
    
    Logger.log(`Validation found ${errors.length} errors and ${warnings.length} warnings`);
    ui.alert('Validation Results', message, ui.ButtonSet.OK);
  }
}

/**
 * Validates an email ingest rule
 * @param {Array} row - Row data 
 * @param {Object} headerMap - Header to column index map
 * @param {number} rowNum - Row number for error messages
 * @param {Array} errors - Array to collect errors
 * @param {Array} warnings - Array to collect warnings
 */
function validateEmailRule(row, headerMap, rowNum, errors, warnings) {
  Logger.log(`Validating email rule in row ${rowNum}`);
  
  const emailSearchHeader = CONFIG.COLUMN_MAPPINGS.in_email_searchString;
  const attachmentPatternHeader = CONFIG.COLUMN_MAPPINGS.in_email_attachmentPattern;
  
  // Check email search string
  if (!(emailSearchHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${emailSearchHeader}' column required for email ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[emailSearchHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${emailSearchHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Check attachment pattern
  if (!(attachmentPatternHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${attachmentPatternHeader}' column required for email ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[attachmentPatternHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${attachmentPatternHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Validate destination fields
  validateDestinationFields(row, headerMap, rowNum, errors);
}

/**
 * Validates a Google Sheet ingest rule
 * @param {Array} row - Row data 
 * @param {Object} headerMap - Header to column index map
 * @param {number} rowNum - Row number for error messages
 * @param {Array} errors - Array to collect errors
 * @param {Array} warnings - Array to collect warnings
 */
function validateGSheetRule(row, headerMap, rowNum, errors, warnings) {
  Logger.log(`Validating gSheet rule in row ${rowNum}`);
  
  const sheetIdHeader = CONFIG.COLUMN_MAPPINGS.in_gsheet_sheetId;
  const sheetUrlHeader = CONFIG.COLUMN_MAPPINGS.in_gsheet_sheetURL;
  const tabNameHeader = CONFIG.COLUMN_MAPPINGS.in_gsheet_tabName;
  
  // Check source sheet ID or URL
  let hasSourceId = false;
  if (sheetIdHeader in headerMap && row[headerMap[sheetIdHeader]]) {
    hasSourceId = true;
  } else if (sheetUrlHeader in headerMap && row[headerMap[sheetUrlHeader]]) {
    hasSourceId = true;
  }
  
  if (!hasSourceId) {
    const errorMsg = `Row ${rowNum}: Missing required source sheet ID or URL for gSheet ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Check source tab name
  if (!(sourceTabHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${sourceTabHeader}' column required for push ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[sourceTabHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${sourceTabHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Check destination sheet ID or URL
  let hasDestId = false;
  if (destSheetIdHeader in headerMap && row[headerMap[destSheetIdHeader]]) {
    hasDestId = true;
  } else if (destSheetUrlHeader in headerMap && row[headerMap[destSheetUrlHeader]]) {
    hasDestId = true;
  }
  
  if (!hasDestId) {
    const errorMsg = `Row ${rowNum}: Missing required destination sheet ID or URL for push ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Check destination tab name
  if (!(destTabHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${destTabHeader}' column required for push ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[destTabHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${destTabHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
}

/**
 * Validates the destination fields for a rule
 * @param {Array} row - Row data 
 * @param {Object} headerMap - Header to column index map
 * @param {number} rowNum - Row number for error messages
 * @param {Array} errors - Array to collect errors
 */
function validateDestinationFields(row, headerMap, rowNum, errors) {
  Logger.log(`Validating destination fields for row ${rowNum}`);
  
  const destSheetIdHeader = CONFIG.COLUMN_MAPPINGS.dest_sheetId;
  const destSheetUrlHeader = CONFIG.COLUMN_MAPPINGS.dest_sheetUrl;
  const destTabHeader = CONFIG.COLUMN_MAPPINGS.dest_sheet_tabName;
  
  // Check destination sheet ID or URL
  let hasDestId = false;
  if (destSheetIdHeader in headerMap && row[headerMap[destSheetIdHeader]]) {
    hasDestId = true;
  } else if (destSheetUrlHeader in headerMap && row[headerMap[destSheetUrlHeader]]) {
    hasDestId = true;
  }
  
  if (!hasDestId) {
    const errorMsg = `Row ${rowNum}: Missing required destination sheet ID or URL`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Check destination tab name
  if (!(destTabHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${destTabHeader}' column`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[destTabHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${destTabHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
}

/**
 * Validates the sheet handling mode
 * @param {Array} row - Row data 
 * @param {Object} headerMap - Header to column index map
 * @param {number} rowNum - Row number for error messages
 * @param {Array} warnings - Array to collect warnings
 */
function validateSheetHandlingMode(row, headerMap, rowNum, warnings) {
  const sheetHandlingModeHeader = CONFIG.COLUMN_MAPPINGS.sheetHandlingMode;
  
  if (sheetHandlingModeHeader in headerMap) {
    const mode = row[headerMap[sheetHandlingModeHeader]];
    if (mode && !['clearAndReuse', 'recreate', 'copyFormat', 'append'].includes(mode)) {
      const warningMsg = `Row ${rowNum}: Invalid '${sheetHandlingModeHeader}' value: ${mode}. Will default to 'clearAndReuse'.`;
      Logger.log(warningMsg);
      warnings.push(warningMsg);
    }
  } else {
    const warningMsg = `Configuration is missing '${sheetHandlingModeHeader}' column. All operations will default to 'clearAndReuse'.`;
    Logger.log(warningMsg);
    if (!warnings.includes(warningMsg)) {
      warnings.push(warningMsg);
    }
  }
}

/**
 * Set up time-based triggers for scheduled jobs
 */
function setupTriggers() {
  // First check current triggers
  const ui = SpreadsheetApp.getUi();
  const currentTriggers = ScriptApp.getProjectTriggers();
  
  // Confirm with user before deleting existing triggers
  if (currentTriggers.length > 0) {
    const response = ui.alert(
      'Existing Triggers',
      `Found ${currentTriggers.length} existing trigger(s). Do you want to replace them?`,
      ui.ButtonSet.YES_NO);
    
    if (response !== ui.Button.YES) {
      ui.alert('Setup Cancelled', 'Trigger setup was cancelled. Existing triggers were not modified.', ui.ButtonSet.OK);
      return;
    }
    
    // Delete all existing triggers
    for (const trigger of currentTriggers) {
      ScriptApp.deleteTrigger(trigger);
    }
    Logger.log(`Deleted ${currentTriggers.length} existing triggers`);
  }
  
  try {
    // Create trigger for daily execution
    ScriptApp.newTrigger('runAll')
      .timeBased()
      .everyDays(1)
      .atHour(1) // 1 AM
      .create();
    Logger.log("Created daily trigger for runAll at 1 AM");
    
    // Create trigger for log cleanup
    ScriptApp.newTrigger('cleanupLogs')
      .timeBased()
      .everyDays(1)
      .atHour(0) // Midnight
      .create();
    Logger.log("Created daily trigger for cleanupLogs at midnight");
    
    // Create the onEdit trigger for maintaining formatting
    ScriptApp.newTrigger('onEdit')
      .forSpreadsheet(SpreadsheetApp.getActive())
      .onEdit()
      .create();
    Logger.log("Created onEdit trigger");
    
    // Create the onOpen trigger
    ScriptApp.newTrigger('dataIngestOnOpen')
      .forSpreadsheet(SpreadsheetApp.getActive())
      .onOpen()
      .create();
    Logger.log("Created onOpen trigger");
    
    ui.alert('Success', 'Triggers have been set up successfully:\n\n' + 
            '- Daily execution at 1 AM\n' +
            '- Daily log cleanup at midnight\n' +
            '- Automatic formatting on edit\n' +
            '- Menu creation on open', ui.ButtonSet.OK);
  } catch (error) {
    Logger.log(`Error setting up triggers: ${error.message}`);
    ui.alert('Error', `Failed to set up triggers: ${error.message}`, ui.ButtonSet.OK);
  }
}

/**
 * Periodically clean up log entries to prevent the log sheet from getting too large
 */
function cleanupLogs() {
  const sessionId = generateUniqueID();
  logOperation(sessionId, "CLEANUP_START", "Starting scheduled log cleanup");
  
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let cleanedCount = 0;
    
    // Clean up log sheet
    const logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
    if (logSheet) {
      const totalLogRows = logSheet.getLastRow();
      if (totalLogRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalLogRows - CONFIG.MAX_LOG_ENTRIES - 1;
        logSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
        cleanedCount += deleteCount;
        Logger.log(`Cleaned up ${deleteCount} old log entries`);
      }
    }
    
    // Clean up verification sheet
    const verificationSheet = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME);
    if (verificationSheet) {
      const totalVerificationRows = verificationSheet.getLastRow();
      if (totalVerificationRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalVerificationRows - CONFIG.MAX_LOG_ENTRIES - 1;
        verificationSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
        cleanedCount += deleteCount;
        Logger.log(`Cleaned up ${deleteCount} old verification entries`);
      }
    }
    
    // Clean up diagnostic sheet
    const diagnosticSheet = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME);
    if (diagnosticSheet) {
      const totalDiagnosticRows = diagnosticSheet.getLastRow();
      if (totalDiagnosticRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalDiagnosticRows - CONFIG.MAX_LOG_ENTRIES - 1;
        diagnosticSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
        cleanedCount += deleteCount;
        Logger.log(`Cleaned up ${deleteCount} old diagnostic entries`);
      }
    }
    
    logOperation(sessionId, "CLEANUP_COMPLETE", `Cleaned up a total of ${cleanedCount} log entries`);
  } catch (error) {
    Logger.log(`Error during log cleanup: ${error.message}`);
    try {
      logOperation(sessionId, "CLEANUP_ERROR", `Error during log cleanup: ${error.message}`);
    } catch (logError) {
      Logger.log(`Failed to log cleanup error: ${logError.message}`);
    }
  }
}

/**
 * Handle edits to automatically add checkboxes and maintain formatting 
 * Ensures all rules have proper checkboxes and validation even when adding new rows
 * This function runs automatically when a user edits the spreadsheet
 * @param {Event} e - The edit event
 */
function onEdit(e) {
  const sheet = e.source.getActiveSheet();
  // Only process edits on the config sheet
  if (sheet.getName() !== CONFIG.CONFIG_SHEET_NAME) {
    return;
  }
  
  // Get headers
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  
  // Find the ruleActive column
  const ruleActiveHeader = CONFIG.COLUMN_MAPPINGS.ruleActive;
  const ruleActiveCol = headers.indexOf(ruleActiveHeader) + 1;
  
  if (ruleActiveCol <= 0) {
    return; // Column not found
  }
  
  // If we've edited a cell in a row that doesn't have a checkbox in the ruleActive column,
  // add a checkbox
  if (e.range.getRow() > 1) {
    const ruleActiveRange = sheet.getRange(e.range.getRow(), ruleActiveCol);
    if (!ruleActiveRange.hasCheckboxes()) {
      try {
        ruleActiveRange.insertCheckboxes();
        Logger.log(`Added checkbox at row ${e.range.getRow()}, column ${ruleActiveCol}`);
      } catch (error) {
        Logger.log(`Error adding checkbox: ${error.message}`);
      }
    }
  }
  
  // Also ensure ingestMethod has data validation
  const ingestMethodHeader = CONFIG.COLUMN_MAPPINGS.ingestMethod;
  const ingestMethodCol = headers.indexOf(ingestMethodHeader) + 1;
  
  if (ingestMethodCol > 0 && e.range.getRow() > 1) {
    const methodCell = sheet.getRange(e.range.getRow(), ingestMethodCol);
    const validation = methodCell.getDataValidation();
    
    if (!validation) {
      // Add validation if missing
      try {
        const methodRule = SpreadsheetApp.newDataValidation()
          .requireValueInList(['email', 'gSheet', 'push'], true)
          .build();
        methodCell.setDataValidation(methodRule);
        Logger.log(`Added method validation at row ${e.range.getRow()}, column ${ingestMethodCol}`);
      } catch (error) {
        Logger.log(`Error adding method validation: ${error.message}`);
      }
    }
  }
  
  // Do the same for sheetHandlingMode
  const sheetHandlingModeHeader = CONFIG.COLUMN_MAPPINGS.sheetHandlingMode;
  const sheetHandlingModeCol = headers.indexOf(sheetHandlingModeHeader) + 1;
  
  if (sheetHandlingModeCol > 0 && e.range.getRow() > 1) {
    const handlingCell = sheet.getRange(e.range.getRow(), sheetHandlingModeCol);
    const validation = handlingCell.getDataValidation();
    
    if (!validation) {
      // Add validation if missing
      try {
        const handlingRule = SpreadsheetApp.newDataValidation()
          .requireValueInList(['clearAndReuse', 'recreate', 'copyFormat', 'append'], true)
          .build();
        handlingCell.setDataValidation(handlingRule);
        Logger.log(`Added handling mode validation at row ${e.range.getRow()}, column ${sheetHandlingModeCol}`);
      } catch (error) {
        Logger.log(`Error adding handling mode validation: ${error.message}`);
      }
    }
  }
  
  // Reapply conditional formatting if needed
  try {
    // Check if we've added a new row that exceeds the current conditional formatting range
    const formatRules = sheet.getConditionalFormatRules();
    if (formatRules.length > 0 && e.range.getRow() > sheet.getLastRow() - 5) {
      // If we're getting close to the end of the formatted area, refresh the rules
      setupConditionalFormatting(sheet);
      Logger.log("Refreshed conditional formatting rules for new rows");
    }
  } catch (formatError) {
    Logger.log(`Error updating conditional formatting: ${formatError.message}`);
  }

  if (!(tabNameHeader in headerMap)) {
    const errorMsg = `Row ${rowNum}: Missing '${tabNameHeader}' column required for gSheet ingest`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  } else if (!row[headerMap[tabNameHeader]]) {
    const errorMsg = `Row ${rowNum}: Missing required '${tabNameHeader}' value`;
    Logger.log(errorMsg);
    errors.push(errorMsg);
  }
  
  // Validate destination fields
  validateDestinationFields(row, headerMap, rowNum, errors);
}

/**
 * Validates a push rule
 * @param {Array} row - Row data 
 * @param {Object} headerMap - Header to column index map
 * @param {number} rowNum - Row number for error messages
 * @param {Array} errors - Array to collect errors
 * @param {Array} warnings - Array to collect warnings
 */
function validatePushRule(row, headerMap, rowNum, errors, warnings) {
  Logger.log(`Validating push rule in row ${rowNum}`);
  
  const sourceTabHeader = CONFIG.COLUMN_MAPPINGS.pushSourceTabName;
  const destSheetIdHeader = CONFIG.COLUMN_MAPPINGS.pushDestinationSheetId;
  const destSheetUrlHeader = CONFIG.COLUMN_MAPPINGS.pushDestinationSheetUrl;
  const destTabHeader = CONFIG.COLUMN_MAPPINGS.pushDestinationTabName;
  
  // Check,
  

};

/**
 * Creates the custom menu when the spreadsheet is opened and initializes required sheets
 * Note: If you have multiple onOpen functions, use dataIngestOnOpen() instead
 * and run it manually from the script editor
 */
function onOpen() {
  // Use the renamed version to avoid conflicts with other scripts
  dataIngestOnOpen();
}

/**
 * Alternative function for creating the menu to avoid conflicts
 * This can be run manually or set up as an installable trigger
 */
function dataIngestOnOpen() {
  // First verify that necessary sheets exist and create them if needed
  ensureRequiredSheetsExist();
  
  // Then create the menu
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Data Ingest')
    .addItem('Set Up Sheets', 'setupSheets')
    .addSeparator()
    .addItem('Run All Active Rules', 'runAll')
    .addItem('Run Selected Rules', 'runSelectedRules')
    .addSeparator()
    .addItem('Validate Configuration', 'validateConfiguration')
    .addItem('Set Up Scheduled Triggers', 'setupTriggers')
    .addSeparator()
    .addItem('Send Test Email Report', 'sendTestEmailReport')
    .addSeparator()
    .addItem('Check System Status', 'checkSystemStatus') // New menu item for diagnostics
    .addToUi();
}

/**
 * IMPROVED: Ensures all required sheets exist and creates them if missing
 * Now uses safer sheet creation with backups and improved error handling
 */
function ensureRequiredSheetsExist() {
  const requiredSheets = [
    { name: CONFIG.CONFIG_SHEET_NAME, createFn: createCfgIngestSheet },
    { name: CONFIG.LOG_SHEET_NAME, createFn: createLogSheet },
    { name: CONFIG.VERIFICATION_SHEET_NAME, createFn: createVerificationSheet },
    { name: CONFIG.DIAGNOSTIC_SHEET_NAME, createFn: createDiagnosticSheet }
  ];
  
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheetsMissing = false;
    
    // Check if required sheets exist and create them if needed
    for (const sheet of requiredSheets) {
      let existingSheet = ss.getSheetByName(sheet.name);
      if (!existingSheet) {
        Logger.log(`Required sheet "${sheet.name}" is missing. Creating it now.`);
        try {
          sheet.createFn();
          sheetsMissing = true;
        } catch (createError) {
          Logger.log(`ERROR creating ${sheet.name}: ${createError.message}`);
          // Try a simplified approach if the function failed
          try {
            existingSheet = ss.insertSheet(sheet.name);
            if (sheet.name === CONFIG.LOG_SHEET_NAME) {
              existingSheet.appendRow(['Timestamp', 'SessionID', 'EventType', 'Message']);
            } else if (sheet.name === CONFIG.VERIFICATION_SHEET_NAME) {
              existingSheet.appendRow([
                'Timestamp', 'SessionID', 'RuleID', 'SourceType', 'SourceFile',
                'DestinationSheet', 'SourceRows', 'DestRows', 'SourceColumns',
                'DestColumns', 'RowsMatch', 'ColumnsMatch', 'SamplesMatch',
                'DataHash', 'Status', 'Details'
              ]);
            } else if (sheet.name === CONFIG.DIAGNOSTIC_SHEET_NAME) {
              existingSheet.appendRow([
                'Timestamp', 'SessionID', 'Position', 'Column', 
                'SourceValue', 'SourceType', 'DestValue', 'DestType',
                'NormalizedSource', 'NormalizedDest', 'Details'
              ]);
            }
            
            // Add minimal formatting
            existingSheet.getRange(1, 1, 1, existingSheet.getLastColumn()).setFontWeight('bold');
            
            Logger.log(`Created basic ${sheet.name} sheet as fallback`);
          } catch (fallbackError) {
            Logger.log(`CRITICAL ERROR: Both creation methods failed for ${sheet.name}: ${fallbackError.message}`);
            // Continue to other sheets instead of aborting entirely
          }
        }
      }
    }
    
    // Log initialization status
    if (sheetsMissing) {
      const initSessionId = generateUniqueID();
      // Try to log but don't fail if logging fails
      try {
        logOperation(initSessionId, "INITIALIZATION", "Created missing required sheets during startup");
      } catch (logError) {
        Logger.log(`Could not log initialization: ${logError.message}`);
      }
      Logger.log("Successfully created all required sheets during initialization");
    } else {
      Logger.log("All required sheets already exist. No initialization needed.");
    }
    
    return true;
  } catch (error) {
    Logger.log(`ERROR DURING INITIALIZATION: ${error.message}`);
    return false;
  }
}

/**
 * IMPROVED: Safely create a sheet with backup of existing data
 * @param {string} sheetName - Name of the sheet to create
 * @param {Function} setupFunction - Function to set up the new sheet
 * @returns {Sheet} The created/updated sheet
 */
function ensureSafeSheetCreation(sheetName, setupFunction) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let existingSheet = ss.getSheetByName(sheetName);
  let backupSheet = null;
  
  // Create backup if sheet already exists
  if (existingSheet) {
    try {
      // Create backup with timestamp
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const backupName = `${sheetName}-backup-${timestamp}`;
      backupSheet = existingSheet.copyTo(ss).setName(backupName);
      Logger.log(`Created backup of ${sheetName} as ${backupName}`);
    } catch (backupError) {
      Logger.log(`Warning: Could not create backup of ${sheetName}: ${backupError.message}`);
      // Continue with existing sheet rather than failing
    }
  }
  
  try {
    // Create new sheet with a temporary name to avoid conflicts
    const tempName = `${sheetName}-new`;
    let newSheet = ss.getSheetByName(tempName);
    if (newSheet) {
      // If a temp sheet already exists (from a previous failed attempt), delete it
      ss.deleteSheet(newSheet);
    }
    
    newSheet = ss.insertSheet(tempName);
    
    // Apply setup function to configure the new sheet
    setupFunction(newSheet);
    
    // Only after successful creation, rename the sheet
    if (existingSheet) {
      try {
        ss.deleteSheet(existingSheet);
      } catch (deleteError) {
        Logger.log(`Warning: Could not delete existing ${sheetName}: ${deleteError.message}`);
        // Try to rename the existing sheet instead
        try {
          existingSheet.setName(`${sheetName}-old`);
        } catch (renameError) {
          Logger.log(`Warning: Could not rename existing ${sheetName}: ${renameError.message}`);
        }
      }
    }
    
    // Rename the new sheet to the target name
    newSheet.setName(sheetName);
    
    return newSheet;
  } catch (error) {
    // If creation fails but we have a backup, try to restore it
    Logger.log(`Error creating new ${sheetName}: ${error.message}`);
    
    if (backupSheet) {
      try {
        // Rename backup to original if we deleted the original
        if (!ss.getSheetByName(sheetName)) {
          backupSheet.setName(sheetName);
          Logger.log(`Restored backup for ${sheetName}`);
          return backupSheet;
        }
      } catch (restoreError) {
        Logger.log(`Error restoring backup of ${sheetName}: ${restoreError.message}`);
      }
    }
    
    // Return the existing sheet if available, otherwise throw the error
    if (existingSheet && ss.getSheetByName(existingSheet.getName())) {
      Logger.log(`Keeping existing ${sheetName} due to error`);
      return existingSheet;
    }
    
    throw new Error(`Failed to create or restore ${sheetName}: ${error.message}`);
  }
}

/**
 * Checks the system status and displays a report
 */
function checkSystemStatus() {
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const statusSessionId = generateUniqueID();
  
  try {
    // Check required sheets
    const requiredSheets = [
      CONFIG.CONFIG_SHEET_NAME,
      CONFIG.LOG_SHEET_NAME,
      CONFIG.VERIFICATION_SHEET_NAME,
      CONFIG.DIAGNOSTIC_SHEET_NAME
    ];
    
    const sheetStatus = {};
    for (const sheetName of requiredSheets) {
      const sheet = ss.getSheetByName(sheetName);
      sheetStatus[sheetName] = {
        exists: sheet !== null,
        rowCount: sheet ? sheet.getLastRow() : 0
      };
    }
    
    // Check if config sheet has any rules
    let configHasRules = false;
    let activeRuleCount = 0;
    
    if (sheetStatus[CONFIG.CONFIG_SHEET_NAME].exists) {
      const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
      const configData = configSheet.getDataRange().getValues();
      
      if (configData.length > 1) {
        configHasRules = true;
        
        // Count active rules
        if (configData.length > 1) {
          const headers = configData[0];
          const ruleActiveIndex = headers.indexOf(CONFIG.COLUMN_MAPPINGS.ruleActive);
          
          if (ruleActiveIndex >= 0) {
            for (let i = 1; i < configData.length; i++) {
              if (configData[i][ruleActiveIndex] === true) {
                activeRuleCount++;
              }
            }
          }
        }
      }
    }
    
    // Check trigger status
    const triggers = ScriptApp.getProjectTriggers();
    const triggerStatus = {
      total: triggers.length,
      onOpen: 0,
      timeBased: 0,
      onEdit: 0
    };
    
    for (const trigger of triggers) {
      const eventType = trigger.getEventType();
      if (eventType === ScriptApp.EventType.ON_OPEN) {
        triggerStatus.onOpen++;
      } else if (eventType === ScriptApp.EventType.CLOCK) {
        triggerStatus.timeBased++;
      } else if (eventType === ScriptApp.EventType.ON_EDIT) {
        triggerStatus.onEdit++;
      }
    }
    
    // Log the system check
    logOperation(statusSessionId, "SYSTEM_CHECK", "System status check performed");
    
    // Display status report
    let statusMessage = "Data Ingest System Status:\n\n";
    
    // Sheet status
    statusMessage += "Required Sheets:\n";
    for (const [name, status] of Object.entries(sheetStatus)) {
      statusMessage += `- ${name}: ${status.exists ? '✅ Exists' : '❌ Missing'}`;
      if (status.exists) {
        statusMessage += ` (${status.rowCount} rows)`;
      }
      statusMessage += "\n";
    }
    
    // Config status
    statusMessage += "\nConfiguration:\n";
    statusMessage += `- Rules defined: ${configHasRules ? '✅ Yes' : '❌ No'}\n`;
    statusMessage += `- Active rules: ${activeRuleCount}\n`;
    
    // Trigger status
    statusMessage += "\nTriggers:\n";
    statusMessage += `- Total triggers: ${triggerStatus.total}\n`;
    statusMessage += `- On open triggers: ${triggerStatus.onOpen}\n`;
    statusMessage += `- Time-based triggers: ${triggerStatus.timeBased}\n`;
    statusMessage += `- On edit triggers: ${triggerStatus.onEdit}\n`;
    
    // Email configuration
    statusMessage += "\nEmail Configuration:\n";
    statusMessage += `- Recipients: ${CONFIG.EMAIL_NOTIFICATIONS.join(", ")}\n`;
    statusMessage += `- Send on start: ${CONFIG.EMAIL_CONFIG.SEND_ON_START ? '✅ Yes' : '❌ No'}\n`;
    statusMessage += `- Send on complete: ${CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE ? '✅ Yes' : '❌ No'}\n`;
    statusMessage += `- Send on error: ${CONFIG.EMAIL_CONFIG.SEND_ON_ERROR ? '✅ Yes' : '❌ No'}\n`;
    
    ui.alert('System Status Report', statusMessage, ui.ButtonSet.OK);
    
  } catch (error) {
    logOperation(statusSessionId, "STATUS_ERROR", `Error checking system status: ${error.message}`);
    ui.alert('Error', `Failed to check system status: ${error.message}`, ui.ButtonSet.OK);
  }
}

/**
 * Sends a test email report
 */
function sendTestEmailReport() {
  const ui = SpreadsheetApp.getUi();
  
  try {
    // Ensure all required sheets exist
    const sheetsExist = ensureRequiredSheetsExist();
    if (!sheetsExist) {
      ui.alert('Error', 'Required sheets are missing and could not be created. Cannot send test email.', ui.ButtonSet.OK);
      return;
    }
    
    // Confirm with the user
    const response = ui.alert(
      'Send Test Email',
      'This will send a test email report to all configured recipients. Continue?',
      ui.ButtonSet.YES_NO
    );
    
    if (response !== ui.Button.YES) {
      return;
    }
    
    const sessionId = generateUniqueID();
    logOperation(sessionId, "TEST_EMAIL", "User requested test email report");
    
    // Create sample results for test email
    const testResults = [
      { ruleName: "Test Rule #1", status: "SUCCESS", rowsProcessed: 1250, rowsExpected: 1250, duration: 3.5 },
      { ruleName: "Test Rule #2", status: "ERROR", rowsProcessed: 980, rowsExpected: 1000, duration: 2.1, message: "Sample error message for testing" },
      { ruleName: "Test Rule #3", status: "SKIPPED", rowsProcessed: 0, rowsExpected: 0, duration: 0, message: "Rule not active" }
    ];
    
    // Create a sample verification entry
    logVerification({
      timestamp: new Date(),
      sessionId: sessionId,
      ruleId: "Test Rule #1",
      sourceType: "Test",
      sourceFile: "test_data.csv",
      destinationSheet: "test_destination",
      sourceRowCount: 1250,
      destinationRowCount: 1250,
      sourceColumnCount: 10,
      destinationColumnCount: 10,
      samplesMatch: true,
      dataHash: "test-hash-12345",
      isComplete: true
    });
    
    // Create some sample log entries
    logOperation(sessionId, "TEST_START", "Starting test email process");
    logOperation(sessionId, "TEST_INFO", "This is a sample information log entry");
    logOperation(sessionId, "TEST_WARNING", "This is a sample warning log entry");
    logOperation(sessionId, "TEST_ERROR", "This is a sample error log entry for testing purposes");
    logOperation(sessionId, "TEST_SUCCESS", "This is a sample success log entry");
    
    try {
      sendRunSummaryEmail(sessionId, "TEST", testResults);
      ui.alert('Success', 'Test email sent successfully. Please check your inbox.', ui.ButtonSet.OK);
      logOperation(sessionId, "EMAIL_SENT", "Test email sent successfully");
    } catch (error) {
      logOperation(sessionId, "EMAIL_ERROR", `Failed to send test email: ${error.message}`);
      ui.alert('Error', `Failed to send test email: ${error.message}`, ui.ButtonSet.OK);
    }
  } catch (error) {
    ui.alert('Error', `An error occurred: ${error.message}`, ui.ButtonSet.OK);
  }
}

/**
 * IMPROVED: Creates or updates configuration and logging sheets with safer methods
 */
function setupSheets() {
  Logger.log("Starting setupSheets function");
  const ui = SpreadsheetApp.getUi();
  const sessionId = generateUniqueID();
  
  try {
    // Check if sheets already exist
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const configExists = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME) !== null;
    const logExists = ss.getSheetByName(CONFIG.LOG_SHEET_NAME) !== null;
    const verificationExists = ss.getSheetByName(CONFIG.VERIFICATION_SHEET_NAME) !== null;
    const diagnosticExists = ss.getSheetByName(CONFIG.DIAGNOSTIC_SHEET_NAME) !== null;
    
    Logger.log(`Existing sheets check: Config exists: ${configExists}, Log exists: ${logExists}, Verification exists: ${verificationExists}, Diagnostic exists: ${diagnosticExists}`);
    
    const existingSheets = [];
    if (configExists) existingSheets.push(CONFIG.CONFIG_SHEET_NAME);
    if (logExists) existingSheets.push(CONFIG.LOG_SHEET_NAME);
    if (verificationExists) existingSheets.push(CONFIG.VERIFICATION_SHEET_NAME);
    if (diagnosticExists) existingSheets.push(CONFIG.DIAGNOSTIC_SHEET_NAME);
    
    let proceedWithSetup = true;
    
    if (existingSheets.length > 0) {
      const message = `The following sheets already exist: ${existingSheets.join(', ')}\n\nWhat would you like to do?\n\n` +
                    `• Click "Yes" to BACKUP these sheets (create copies without replacing)\n` +
                    `• Click "No" to BACKUP AND REPLACE these sheets\n` +
                    `• Click "Cancel" to cancel the operation\n`;
      
      const response = ui.alert('Sheets Already Exist', message, ui.ButtonSet.YES_NO_CANCEL);
      
      Logger.log(`User selected option: ${response}`);
      
      if (response === ui.Button.CANCEL) {
        Logger.log("User canceled the operation");
        return;
      } else if (response === ui.Button.YES) {
        // Just backup without replacing
        Logger.log("Creating backups without replacing existing sheets");
        backupExistingSheets(ss, existingSheets);
        logOperation(sessionId, "BACKUP_CREATED", `Created backups of ${existingSheets.length} existing sheets without replacing them`);
        ui.alert('Backup Complete', 'Backup copies of the existing sheets have been created. Original sheets were not modified.', ui.ButtonSet.OK);
        proceedWithSetup = false;
      } else if (response === ui.Button.NO) {
        // Backup and replace
        Logger.log("Creating backups and replacing existing sheets");
        backupExistingSheets(ss, existingSheets);
        logOperation(sessionId, "BACKUP_CREATED", `Created backups of ${existingSheets.length} existing sheets for replacement`);
        proceedWithSetup = true;
      }
    }
    
    // Proceed with sheet setup if needed
    if (proceedWithSetup) {
      let createdSheets = [];
      
      if (!configExists || (configExists && existingSheets.includes(CONFIG.CONFIG_SHEET_NAME))) {
        try {
          Logger.log("Creating/updating configuration sheet");
          createCfgIngestSheet();
          createdSheets.push(CONFIG.CONFIG_SHEET_NAME);
        } catch (configError) {
          const errorMsg = `Error creating configuration sheet: ${configError.message}`;
          Logger.log(errorMsg);
          logOperation(sessionId, "SHEET_ERROR", errorMsg);
          ui.alert('Error', `Failed to create configuration sheet: ${configError.message}`, ui.ButtonSet.OK);
        }
      }
      
      if (!logExists || (logExists && existingSheets.includes(CONFIG.LOG_SHEET_NAME))) {
        try {
          Logger.log("Creating/updating log sheet");
          createLogSheet();
          createdSheets.push(CONFIG.LOG_SHEET_NAME);
        } catch (logError) {
          const errorMsg = `Error creating log sheet: ${logError.message}`;
          Logger.log(errorMsg);
          logOperation(sessionId, "SHEET_ERROR", errorMsg);
          ui.alert('Error', `Failed to create log sheet: ${logError.message}`, ui.ButtonSet.OK);
        }
      }
      
      if (!verificationExists || (verificationExists && existingSheets.includes(CONFIG.VERIFICATION_SHEET_NAME))) {
        try {
          Logger.log("Creating/updating verification sheet");
          createVerificationSheet();
          createdSheets.push(CONFIG.VERIFICATION_SHEET_NAME);
        } catch (verificationError) {
          const errorMsg = `Error creating verification sheet: ${verificationError.message}`;
          Logger.log(errorMsg);
          logOperation(sessionId, "SHEET_ERROR", errorMsg);
          ui.alert('Error', `Failed to create verification sheet: ${verificationError.message}`, ui.ButtonSet.OK);
        }
      }
      
      if (!diagnosticExists || (diagnosticExists && existingSheets.includes(CONFIG.DIAGNOSTIC_SHEET_NAME))) {
        try {
          Logger.log("Creating/updating diagnostic sheet");
          createDiagnosticSheet();
          createdSheets.push(CONFIG.DIAGNOSTIC_SHEET_NAME);
        } catch (diagnosticError) {
          const errorMsg = `Error creating diagnostic sheet: ${diagnosticError.message}`;
          Logger.log(errorMsg);
          logOperation(sessionId, "SHEET_ERROR", errorMsg);
          ui.alert('Error', `Failed to create diagnostic sheet: ${diagnosticError.message}`, ui.ButtonSet.OK);
        }
      }
      
      if (createdSheets.length === 4) {
        logOperation(sessionId, "SETUP_COMPLETE", "All required sheets were successfully created or updated");
        ui.alert('Setup Complete', 'All required sheets have been created/updated successfully.', ui.ButtonSet.OK);
      } else if (createdSheets.length > 0) {
        logOperation(sessionId, "SETUP_PARTIAL", `Created/updated ${createdSheets.length} of 4 required sheets: ${createdSheets.join(', ')}`);
        ui.alert('Partial Setup', `Created/updated ${createdSheets.length} of 4 required sheets: ${createdSheets.join(', ')}\n\nSome sheets failed to create - check the logs for details.`, ui.ButtonSet.OK);
      } else {
        logOperation(sessionId, "SETUP_FAILED", "Failed to create any required sheets");
        ui.alert('Setup Failed', 'Failed to create any required sheets. Check the logs for details.', ui.ButtonSet.OK);
      }
    }
    
    Logger.log("setupSheets function completed");
  } catch (error) {
    const errorMsg = `Setup error: ${error.message}`;
    Logger.log(errorMsg);
    try {
      logOperation(sessionId, "SETUP_ERROR", errorMsg);
    } catch (logError) {
      Logger.log(`Failed to log setup error: ${logError.message}`);
    }
    ui.alert('Error', `An error occurred during setup: ${error.message}`, ui.ButtonSet.OK);
  }
}

/**
 * IMPROVED: Backup existing sheets with error handling for each sheet
 * @param {Spreadsheet} ss - The spreadsheet
 * @param {Array} sheetNames - Array of sheet names to back up
 * @returns {Array} Array of backup sheet names
 */
function backupExistingSheets(ss, sheetNames) {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const backupResults = [];
  
  for (const sheetName of sheetNames) {
    try {
      Logger.log(`Creating backup of sheet: ${sheetName}`);
      const sheet = ss.getSheetByName(sheetName);
      
      if (!sheet) {
        Logger.log(`Sheet ${sheetName} not found, skipping backup`);
        continue;
      }
      
      const backupName = `${sheetName}-backup-${timestamp}`;
      const backupSheet = sheet.copyTo(ss).setName(backupName);
      
      Logger.log(`Created backup sheet: ${backupName}`);
      backupResults.push(backupName);
    } catch (error) {
      Logger.log(`ERROR creating backup of ${sheetName}: ${error.message}`);
      // Continue with other sheets
    }
  }
  
  return backupResults;
}

/**
 * Main function to run all active ingest rules
 */
function runAll() {
  Logger.log("Starting runAll function");
  const sessionId = generateUniqueID();
  logOperation(sessionId, "START", "Starting full ingest process");
  
  // First ensure all required sheets exist
  try {
    const sheetsExist = ensureRequiredSheetsExist();
    if (!sheetsExist) {
      const errorMsg = "Required sheets are missing and could not be created. Cannot proceed with ingest process.";
      Logger.log(errorMsg);
      logOperation(sessionId, "ABORT", errorMsg);
      SpreadsheetApp.getUi().alert('Error', errorMsg, SpreadsheetApp.getUi().ButtonSet.OK);
      return;
    }
  } catch (initError) {
    const errorMsg = `Error during initialization: ${initError.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "INIT_ERROR", errorMsg);
    SpreadsheetApp.getUi().alert('Error', errorMsg, SpreadsheetApp.getUi().ButtonSet.OK);
    return;
  }
  
  try {
    // Send start notification if enabled
    if (CONFIG.EMAIL_CONFIG.SEND_ON_START) {
      try {
        sendJobStartNotification(sessionId);
        logOperation(sessionId, "EMAIL_SENT", "Start notification email sent successfully");
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send start notification: ${emailError.message}`);
        // Continue despite email error
      }
    }
    
    // Run the ingest process and store results
    const results = ingestData(sessionId);
    
    logOperation(sessionId, "COMPLETE", "Ingest process completed successfully");
    Logger.log("runAll function completed successfully");
    
    // Send completion notification if enabled
    if (CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE) {
      try {
        sendRunSummaryEmail(sessionId, "COMPLETE", results);
        logOperation(sessionId, "EMAIL_SENT", "Completion notification email sent successfully");
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send completion notification: ${emailError.message}`);
      }
    }
    
    // Show success message
    SpreadsheetApp.getUi().alert('Success', 
      `Ingest process completed successfully:\n` +
      `- Rules processed: ${results.length}\n` +
      `- Successful: ${results.filter(r => r.status === "SUCCESS").length}\n` +
      `- Errors: ${results.filter(r => r.status === "ERROR").length}\n` +
      `- Skipped: ${results.filter(r => r.status === "SKIPPED").length}\n\n` +
      `Check the log sheet for details.`,
      SpreadsheetApp.getUi().ButtonSet.OK);
      
  } catch (error) {
    const errorMsg = `Error in main process: ${error.message}`;
    Logger.log(`Error in runAll: ${errorMsg}`);
    logOperation(sessionId, "ERROR", errorMsg);
    
    // Send error notification
    if (CONFIG.EMAIL_CONFIG.SEND_ON_ERROR) {
      try {
        sendErrorNotification("Ingest Process Error", errorMsg, sessionId);
        logOperation(sessionId, "EMAIL_SENT", "Error notification email sent successfully");
      } catch (emailError) {
        logOperation(sessionId, "EMAIL_ERROR", `Failed to send error notification: ${emailError.message}`);
      }
    }
    
    // Show error message
    SpreadsheetApp.getUi().alert('Error', 
      `An error occurred during the ingest process:\n${error.message}\n\nCheck the log sheet for details.`,
      SpreadsheetApp.getUi().ButtonSet.OK);
  }
}

/**
 * Runs only the selected rules in the configuration sheet
 */
function runSelectedRules() {
  Logger.log("Starting runSelectedRules function");
  const ui = SpreadsheetApp.getUi();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // First ensure all required sheets exist
  const sessionId = generateUniqueID();
  
  try {
    const sheetsExist = ensureRequiredSheetsExist();
    if (!sheetsExist) {
      const errorMsg = "Required sheets are missing and could not be created. Cannot proceed with ingest process.";
      Logger.log(errorMsg);
      logOperation(sessionId, "ABORT", errorMsg);
      ui.alert('Error', errorMsg, ui.ButtonSet.OK);
      return;
    }
  } catch (initError) {
    const errorMsg = `Error during initialization: ${initError.message}`;
    Logger.log(errorMsg);
    logOperation(sessionId, "INIT_ERROR", errorMsg);
    ui.alert('Error', errorMsg, ui.ButtonSet.OK);
    return;
  }
  
  // Get configuration sheet
  const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
  
  if (!configSheet) {
    const errorMsg = "Configuration sheet not found. Please create it first.";
    Logger.log(errorMsg);
    logOperation(sessionId, "ERROR", errorMsg);
    ui.alert('Error', errorMsg, ui.ButtonSet.OK);
    return;
  }
  
  // Get the selected ranges
  const selectedRanges = ss.getActiveRangeList();
  if (!selectedRanges) {
    const errorMsg = "Please select one or more rows in the configuration sheet.";
    Logger.log(errorMsg);
    logOperation(sessionId, "ERROR", errorMsg);
    ui.alert('Error', errorMsg, ui.ButtonSet.OK);
    return;
  }
  
  // Extract unique row numbers from all selected ranges
  const ranges = selectedRanges.getRanges();
  const selectedRows = [];
  let onConfigSheet = true;
  
  Logger.log(`Processing ${ranges.length} selected ranges`);
  
  for (const range of ranges) {
    if (range.getSheet().getName() !== CONFIG.CONFIG_SHEET_NAME) {
      onConfigSheet = false;
      Logger.log(`Selection includes sheet other than config: ${range.getSheet().getName()}`);
      break;
    }
    
    const startRow = range.getRow();
    const numRows = range.getNumRows();
    
    Logger.log(`Range starts at row ${startRow} with ${numRows} rows`);
    
    for (let i = 0; i < numRows; i++) {
      const row = startRow + i;
      if (row > 1 && !selectedRows.includes(row)) { // Skip header row
        selectedRows.push(row);
      }
    }
  }
  
  if (!onConfigSheet) {
    const errorMsg = "Please select rows in the configuration sheet.";
    Logger.log(errorMsg);
    logOperation(sessionId, "ERROR", errorMsg);
    ui.alert('Error', errorMsg, ui.ButtonSet.OK);
    return;
  }
  
  if (selectedRows.length === 0) {
    const errorMsg = "Please select one or more data rows (not the header row).";
    Logger.log(errorMsg);
    logOperation(sessionId, "ERROR", errorMsg);
    ui.alert('Error', errorMsg, ui.ButtonSet.OK);
    return;
  }
  
  Logger.log(`Selected rows: ${selectedRows.join(', ')}`);
  
  // Confirm with the user
  const response = ui.alert(
    'Confirm Run',
    `Are you sure you want to run ${selectedRows.length} selected rule(s)?`,
    ui.ButtonSet.YES_NO
  );
  
  if (response !== ui.Button.YES) {
    Logger.log("User canceled the operation");
    logOperation(sessionId, "CANCELLED", "User cancelled the operation");
    return;
  }
  
  // Run the selected rules
  Logger.log(`Starting to run ${selectedRows.length} selected rules with session ID: ${sessionId}`);
  logOperation(sessionId, "START", `Running ${selectedRows.length} selected rule(s)`);
  
  // Send start notification if enabled
  if (CONFIG.EMAIL_CONFIG.SEND_ON_START) {
    try {
      sendJobStartNotification(sessionId, selectedRows.length);
      logOperation(sessionId, "EMAIL_SENT", "Start notification email sent successfully");
    } catch (emailError) {
      logOperation(sessionId, "EMAIL_ERROR", `Failed to send start notification: ${emailError.message}`);
      // Continue despite email error
    }
  }
  
  // Get all data from the config sheet for efficiency
  const configData = configSheet.getDataRange().getValues();
  const headers = configData[0];
  const headerMap = createHeaderMap(headers);
  
  Logger.log(`Loaded configuration sheet with ${configData.length} rows`);
  Logger.log(`Headers: ${headers.join(', ')}`);
  
  const ingestMethodKey = CONFIG.COLUMN_MAPPINGS.ingestMethod;
  const ruleActiveKey = CONFIG.COLUMN_MAPPINGS.ruleActive;
  
  let successCount = 0;
  let errorCount = 0;
  let skippedCount = 0;
  let results = []; // Store results for email report
  
  for (const rowNum of selectedRows) {
    const rowData = configData[rowNum - 1]; // -1 because configData is 0-indexed
    const ruleId = `Rule #${rowNum}`;
    
    Logger.log(`Processing ${ruleId}`);
    
    try {
      // Using consistent rule activation check
      const ruleActive = rowData[headerMap[ruleActiveKey]];
      
      Logger.log(`${ruleId} active status: ${ruleActive}`);
                         
      if (ruleActive !== true) {  // Use consistent check (ruleActive !== true)
        Logger.log(`${ruleId} is not active, skipping`);
        logOperation(sessionId, "SKIPPED", `${ruleId} is not active. Enable it to run.`);
        skippedCount++;
        
        // Add to results for reporting
        results.push({
          ruleName: ruleId,
          status: "SKIPPED",
          rowsProcessed: 0,
          rowsExpected: 0,
          duration: 0,
          message: "Rule not active"
        });
        
        continue;
      }
      
      // Get the ingest method
      const ingestMethod = headerMap[ingestMethodKey] !== undefined ? 
                          rowData[headerMap[ingestMethodKey]] : null;
      
      Logger.log(`${ruleId} ingest method: ${ingestMethod}`);
                          
      if (!ingestMethod) {
        throw new Error(`Ingest method not specified`);
      }
      
      // Get sheet handling mode
      const sheetHandlingModeKey = CONFIG.COLUMN_MAPPINGS.sheetHandlingMode;
      const sheetHandlingMode = headerMap[sheetHandlingModeKey] !== undefined ? 
                               rowData[headerMap[sheetHandlingModeKey]] || 'clearAndReuse' : 
                               'clearAndReuse';
      
      Logger.log(`${ruleId} sheet handling mode: ${sheetHandlingMode}`);
      
      logOperation(sessionId, "PROCESSING", `Processing ${ruleId} (${ingestMethod})`);
      
      const startTime = new Date();
      let result;
      
      // Process based on ingest method - using optimized versions with retry logic and verification
      if (ingestMethod === 'email') {
        result = executeWithRetry(() => processEmailIngestWithVerification(sessionId, rowData, headerMap, sheetHandlingMode, ruleId));
      } else if (ingestMethod === 'gSheet') {
        result = executeWithRetry(() => processGSheetIngestWithVerification(sessionId, rowData, headerMap, sheetHandlingMode, ruleId));
      } else if (ingestMethod === 'push') {
        result = executeWithRetry(() => processSheetPushWithVerification(sessionId, rowData, headerMap, sheetHandlingMode, ruleId));
      } else {
        throw new Error(`Unknown ingest method: ${ingestMethod}`);
      }
      
      const duration = (new Date() - startTime) / 1000;
      
      // Update status in the sheet
      updateRuleStatus(configSheet, rowNum, "SUCCESS", "Processed successfully");
      successCount++;
      Logger.log(`${ruleId} processed successfully`);
      logOperation(sessionId, "SUCCESS", `${ruleId} processed successfully`);
      
      // Add to results for reporting
      results.push({
        ruleName: ruleId,
        status: "SUCCESS",
        rowsProcessed: result.rowsProcessed || 0,
        rowsExpected: result.rowsExpected || 0,
        duration: duration,
        message: "Processed successfully"
      });
      
    } catch (error) {
      // Update status in the sheet
      Logger.log(`Error processing ${ruleId}: ${error.message}`);
      updateRuleStatus(configSheet, rowNum, "ERROR", error.message);
      errorCount++;
      logOperation(sessionId, "ERROR", `Error processing ${ruleId}: ${error.message}`);
      
      // Add to results for reporting
      results.push({
        ruleName: ruleId,
        status: "ERROR",
        rowsProcessed: 0,
        rowsExpected: 0,
        duration: 0,
        message: error.message
      });
    }
  }
  
  // Show summary
  const summary = `Run completed: ${successCount} successful, ${errorCount} failed, ${skippedCount} skipped`;
  Logger.log(summary);
  logOperation(sessionId, "COMPLETE", summary);
  
  // Send completion email if enabled
  if (CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE) {
    try {
      sendRunSummaryEmail(sessionId, "COMPLETE", results);
      logOperation(sessionId, "EMAIL_SENT", "Completion notification email sent successfully");
    } catch (emailError) {
      logOperation(sessionId, "EMAIL_ERROR", `Failed to send completion notification: ${emailError.message}`);
    }
  }
  
  ui.alert('Run Complete', 
           `Processed ${selectedRows.length} rules:\n- ${successCount} successful\n- ${errorCount} failed\n- ${skippedCount} skipped\n\nCheck the log sheet for details.`, 
           ui.ButtonSet.OK);
}

/**
 * Updates the status columns for a rule
 * @param {Sheet} sheet - The configuration sheet
 * @param {number} row - Row number
 * @param {string} status - Status text
 * @param {string} message - Status message
 */
function updateRuleStatus(sheet, row, status, message) {
  Logger.log(`Updating status for row ${row}: ${status} - ${message}`);
  try {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const headerMap = createHeaderMap(headers);
    
    const lastRunTimeKey = CONFIG.COLUMN_MAPPINGS.lastRunTime;
    const lastRunStatusKey = CONFIG.COLUMN_MAPPINGS.lastRunStatus;
    const lastRunMessageKey = CONFIG.COLUMN_MAPPINGS.lastRunMessage;
    
    Logger.log(`Status column indices - Time: ${headerMap[lastRunTimeKey]}, Status: ${headerMap[lastRunStatusKey]}, Message: ${headerMap[lastRunMessageKey]}`);
    
    // Only update if the header columns exist
    if (headerMap[lastRunTimeKey] !== undefined) {
      sheet.getRange(row, headerMap[lastRunTimeKey] + 1).setValue(new Date());
    }
    
    if (headerMap[lastRunStatusKey] !== undefined) {
      sheet.getRange(row, headerMap[lastRunStatusKey] + 1).setValue(status);
    }
    
    if (headerMap[lastRunMessageKey] !== undefined) {
      sheet.getRange(row, headerMap[lastRunMessageKey] + 1).setValue(message);
    }
    
    Logger.log(`Status updated successfully for row ${row}`);
  } catch (error) {
    // Don't let status update errors interrupt the main flow
    Logger.log(`Error updating status for row ${row}: ${error.message}`);
  }
}

/**
 * Main ingestion function that processes all active rules
 * @param {string} sessionId - Unique identifier for this execution session
 * @returns {Array} Array of result objects for reporting
 */
function ingestData(sessionId) {
  Logger.log(`Starting ingestData with session ID: ${sessionId}`);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
  
  if (!configSheet) {
    const errorMsg = `Configuration sheet "${CONFIG.CONFIG_SHEET_NAME}" not found`;
    Logger.log(errorMsg);
    throw new Error(errorMsg);
  }
  
  const configData = configSheet.getDataRange().getValues();
  Logger.log(`Loaded configuration with ${configData.length} rows (including header)`);
  
  const headers = configData[0];
  const headerMap = createHeaderMap(headers);
  
  // Verify required headers exist
  const requiredHeaders = [CONFIG.COLUMN_MAPPINGS.ruleActive, CONFIG.COLUMN_MAPPINGS.ingestMethod];
  for (const header of requiredHeaders) {
    if (!(header in headerMap)) {
      const errorMsg = `Required header "${header}" not found in configuration sheet`;
      Logger.log(errorMsg);
      throw new Error(errorMsg);
    }
  }
  
  let successCount = 0;
  let errorCount = 0;
  let skippedCount = 0;
  let results = []; // Store results for email report
  
  // Added progress tracking
  const totalRules = configData.length - 1;
  let processedRules = 0;
  
  // Process each row in the config
  for (let i = 1; i < configData.length; i++) {
    const row = configData[i];
    const rowNum = i + 1;
    Logger.log(`Checking row ${rowNum}`);
    
    // Update progress indicator
    processedRules++;
    const progress = Math.round((processedRules / totalRules) * 100);
    updateProgressIndicator("Data Ingest", progress, `Processing row ${rowNum}/${configData.length - 1}`);
    
    const ruleActive = row[headerMap[CONFIG.COLUMN_MAPPINGS.ruleActive]];
    
    // Skip empty rows or inactive rules
    if (!isRowPopulated(row)) {
      Logger.log(`Row ${rowNum} is empty, skipping`);
      skippedCount++;
      continue;
    } else if (ruleActive !== true) {
      Logger.log(`Row ${rowNum} is inactive, skipping`);
      updateRuleStatus(configSheet, rowNum, "SKIPPED", "Rule not active");
      skippedCount++;
      
      // Add to results for reporting
      results.push({
        ruleName: `Rule #${rowNum}`,
        status: "SKIPPED",
        rowsProcessed: 0,
        rowsExpected: 0,
        duration: 0,
        message: "Rule not active"
      });
      
      continue;
    }
    
    const ruleId = `Rule #${rowNum}`;
    Logger.log(`Processing ${ruleId}`);
    logOperation(sessionId, "PROCESSING", `Processing ${ruleId}`);
    
    try {
      const ingestMethod = row[headerMap[CONFIG.COLUMN_MAPPINGS.ingestMethod]];
      Logger.log(`${ruleId} ingest method: ${ingestMethod}`);
      
      const sheetHandlingMode = row[headerMap[CONFIG.COLUMN_MAPPINGS.sheetHandlingMode]] || 'clearAndReuse';
      Logger.log(`${ruleId} sheet handling mode: ${sheetHandlingMode}`);
      
      const startTime = new Date();
      let result;
      
      // IMPROVED: Using optimized versions with retry logic and verification
      if (ingestMethod === 'email') {
        result = executeWithRetry(() => processEmailIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
      } else if (ingestMethod === 'gSheet') {
        result = executeWithRetry(() => processGSheetIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
      } else if (ingestMethod === 'push') {
        result = executeWithRetry(() => processSheetPushWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId));
      } else {
        throw new Error(`Unknown ingest method: ${ingestMethod}`);
      }
      
      const duration = (new Date() - startTime) / 1000;
      
      updateRuleStatus(configSheet, rowNum, "SUCCESS", `Processed ${ingestMethod} rule successfully`);
      Logger.log(`${ruleId} processed successfully`);
      successCount++;
      
      // Add to results for reporting
      results.push({
        ruleName: ruleId,
        status: "SUCCESS",
        rowsProcessed: result.rowsProcessed || 0,
        rowsExpected: result.rowsExpected || 0,
        duration: duration,
        message: `Processed ${ingestMethod} rule successfully`
      });
      
    } catch (error) {
      Logger.log(`Error processing ${ruleId}: ${error.message}`);
      updateRuleStatus(configSheet, rowNum, "ERROR", error.message);
      logOperation(sessionId, "ERROR", `Error processing ${ruleId}: ${error.message}`);
      
      // Only send individual error emails if batch email is not configured
      if (CONFIG.EMAIL_CONFIG.SEND_ON_ERROR && !CONFIG.EMAIL_CONFIG.SEND_ON_COMPLETE) {
        sendErrorNotification(`Error in ${ruleId}`, error.message, sessionId);
      }
      
      errorCount++;
      
      // Add to results for reporting
      results.push({
        ruleName: ruleId,
        status: "ERROR",
        rowsProcessed: 0,
        rowsExpected: 0,
        duration: 0,
        message: error.message
      });
    }
  }
  
  // Clean up progress indicator
  updateProgressIndicator("Data Ingest", 100, "Complete");
  
  const summary = `Run completed: ${successCount} successful, ${errorCount} failed, ${skippedCount} skipped`;
  Logger.log(summary);
  logOperation(sessionId, "SUMMARY", summary);
  
  // Return all results
  return results;
}

/**
 * Checks if a row has any non-empty values (excluding the first column which is the checkbox)
 * @param {Array} row - Row data array
 * @returns {boolean} True if the row contains data, false if it's empty
 */
function isRowPopulated(row) {
  // Skip the first column (checkbox) and check if any other column has data
  for (let i = 1; i < row.length; i++) {
    if (row[i] !== null && row[i] !== undefined && row[i] !== '') {
      return true;
    }
  }
  return false;
}

/**
 * Enhanced process email ingestion rule with verification
 * @param {string} sessionId - Session identifier
 * @param {Array} row - Configuration row
 * @param {Object} headerMap - Map of header names to column indices
 * @param {string} sheetHandlingMode - How to handle existing sheets
 * @param {string} ruleId - Identifier for the rule
 * @returns {Object} Result of processing including row counts
 */
function processEmailIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`Starting email ingest process with verification`);
  
  const searchString = getRequiredValue(row, headerMap, 'in_email_searchString');
  const attachmentPattern = getRequiredValue(row, headerMap, 'in_email_attachmentPattern');
  const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
  const destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName');
  
  // Log once at the beginning with all parameters
  Logger.log(`Email search: "${searchString}", Pattern: "${attachmentPattern}", Destination: ${destSheetId}:${destSheetTabName}`);
  logOperation(sessionId, "EMAIL_SEARCH", `Searching emails with: ${searchString}`);
  
  try {
    // Use a more selective search limit
    const MAX_THREADS = 50; // Limit the number of threads to search
    Logger.log(`Limited to ${MAX_THREADS} threads for search`);
    
    // Execute search with a limit
    const threads = GmailApp.search(searchString, 0, MAX_THREADS);
    Logger.log(`Found ${threads.length} matching threads`);
    
    if (threads.length === 0) {
      throw new Error("No emails found matching the search criteria");
    }
    
    // Instead of getting all messages at once, process threads one by one
    const attachmentRegex = new RegExp(attachmentPattern);
    let processedAttachment = false;
    let sourceMetadata = null;
    let data = null;
    let sourceMessage = null;
    
    for (let i = 0; i < threads.length && !processedAttachment; i++) {
      const thread = threads[i];
      const messages = thread.getMessages();
      
      Logger.log(`Checking thread ${i+1}/${threads.length} with ${messages.length} messages`);
      
      for (const message of messages) {
        const attachments = message.getAttachments();
        
        if (attachments.length > 0) {
          const matchedAttachments = attachments.filter(att => {
            return attachmentRegex.test(att.getName());
          });
          
          if (matchedAttachments.length === 1) {
            const attachment = matchedAttachments[0];
            Logger.log(`Found matching attachment: ${attachment.getName()} (${attachment.getSize()} bytes)`);
            logOperation(sessionId, "ATTACHMENT_FOUND", `Found matching attachment: ${attachment.getName()}`);
            
            // Create metadata about the source
            sourceMetadata = {
              filename: attachment.getName(),
              size: attachment.getSize(),
              type: attachment.getContentType(),
              emailSubject: message.getSubject(),
              emailDate: message.getDate(),
              emailFrom: message.getFrom()
            };
            
            sourceMessage = message;
            
            // Parse CSV - catch errors for malformed CSV
            try {
              data = Utilities.parseCsv(attachment.getDataAsString());
              
              if (data.length === 0 || data[0].length === 0) {
                throw new Error("Attachment contains no data");
              }
              
              // Log metadata about the parsed data
              logOperation(sessionId, "DATA_PARSED", 
                `Parsed data from ${attachment.getName()}: ${data.length} rows x ${data[0].length} columns`);
              
              processedAttachment = true;
              break; // Found matching attachment, stop processing messages
            } catch (csvError) {
              throw new Error(`Error parsing CSV data: ${csvError.message}`);
            }
          } else if (matchedAttachments.length > 1) {
            Logger.log(`Multiple matching attachments found (${matchedAttachments.length})`);
            throw new Error(`Multiple attachments matching the pattern ${attachmentPattern} found in email`);
          }
        }
      }
      
      if (processedAttachment) {
        break; // Stop processing threads once we've found a match
      }
    }
    
    if (!processedAttachment || !data) {
      Logger.log(`No matching attachments found in any email`);
      throw new Error(`No attachment matching the pattern ${attachmentPattern} found in any email`);
    }
    
    // Calculate data fingerprint/hash for verification
    const dataHash = calculateDataHash(data);
    logOperation(sessionId, "DATA_HASH", `Source data hash: ${dataHash}`);
    
    // Open destination sheet
    const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);
    
    // Get before count if appending
    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      beforeRowCount = destSheet.getLastRow();
      logOperation(sessionId, "APPEND_MODE", `Destination had ${beforeRowCount} rows before append`);
    }
    
    // Write the data
    destSheet.getRange(sheetHandlingMode === 'append' ? beforeRowCount + 1 : 1, 
                      1, data.length, data[0].length).setValues(data);
    
    // Verify the data was written correctly if verification is enabled
    if (CONFIG.VERIFICATION_CONFIG.ENABLED) {
      const afterRowCount = destSheet.getLastRow();
      const expectedRowCount = sheetHandlingMode === 'append' ? beforeRowCount + data.length : data.length;
      
      // Verify row counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS && afterRowCount !== expectedRowCount) {
        const errorMsg = `Row count mismatch: Expected ${expectedRowCount} rows, got ${afterRowCount}`;
        logOperation(sessionId, "ROW_COUNT_ERROR", errorMsg);
        throw new Error(errorMsg);
      }
      
      // Verify column counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS) {
        const afterColCount = destSheet.getLastColumn();
        if (afterColCount < data[0].length) {
          const errorMsg = `Column count mismatch: Expected ${data[0].length} columns, got ${afterColCount}`;
          logOperation(sessionId, "COLUMN_COUNT_ERROR", errorMsg);
          throw new Error(errorMsg);
        }
      }
      
      // IMPROVED: Verify sample data with better type handling
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA) {
        const success = verifyDataSamples(data, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        
        if (!success) {
          throw new Error("Data verification failed: Sample data mismatch between source and destination");
        }
      }
      
      // Log verification success
      logVerification({
        timestamp: new Date(),
        sessionId: sessionId,
        ruleId: ruleId,
        sourceType: "Email Attachment",
        sourceFile: sourceMetadata.filename,
        sourceMetadata: JSON.stringify(sourceMetadata),
        destinationSheet: destSheetTabName,
        sourceRowCount: data.length,
        destinationRowCount: afterRowCount - (sheetHandlingMode === 'append' ? beforeRowCount : 0),
        sourceColumnCount: data[0].length,
        destinationColumnCount: destSheet.getLastColumn(),
        samplesMatch: true,
        dataHash: dataHash,
        isComplete: true
      });
    }
    
    // Auto-resize columns for better display (only for smaller datasets)
    if (data.length > 0 && data.length < 500 && data[0].length < 20) {
      for (let i = 1; i <= data[0].length; i++) {
        destSheet.autoResizeColumn(i);
      }
    }
    
    const duration = (new Date() - startTime) / 1000;
    Logger.log(`Email ingest completed successfully in ${duration} seconds`);
    
    return {
      status: "SUCCESS",
      rowsProcessed: data.length,
      rowsExpected: data.length,
      duration: duration,
      dataHash: dataHash,
      attachmentSize: sourceMetadata.size
    };
  } catch (error) {
    Logger.log(`Email ingest error: ${error.message}`);
    throw new Error(`Email ingest error: ${error.message}`);
  }
}

/**
 * Enhanced process Google Sheet ingestion rule with verification
 * @param {string} sessionId - Session identifier
 * @param {Array} row - Configuration row
 * @param {Object} headerMap - Map of header names to column indices
 * @param {string} sheetHandlingMode - How to handle existing sheets
 * @param {string} ruleId - Identifier for the rule
 * @returns {Object} Result of processing including row counts
 */
function processGSheetIngestWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`Starting Google Sheet ingest process with verification`);
  
  const sourceSheetId = getResourceId(row, headerMap, 'in_gsheet_sheetId', 'in_gsheet_sheetURL');
  const sourceTabName = getRequiredValue(row, headerMap, 'in_gsheet_tabName');
  const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
  const destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName');
  
  // Log once at the beginning
  Logger.log(`Source: ${sourceSheetId}:${sourceTabName}, Destination: ${destSheetId}:${destSheetTabName}, Mode: ${sheetHandlingMode}`);
  logOperation(sessionId, "SHEET_ACCESS", `Accessing source sheet: ${sourceSheetId}, tab: ${sourceTabName}`);
  
  try {
    // Open source spreadsheet and get data
    const sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
    const sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
    
    if (!sourceSheet) {
      throw new Error(`Source sheet tab "${sourceTabName}" not found`);
    }
    
    const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
    
    // Copy with formatting mode (handled differently)
    if (sheetHandlingMode === 'copyFormat') {
      // Copying sheet with formatting is an efficient operation
      const tempSheet = sourceSheet.copyTo(destSpreadsheet);
      
      const existingSheet = destSpreadsheet.getSheetByName(destSheetTabName);
      if (existingSheet && existingSheet.getSheetId() !== tempSheet.getSheetId()) {
        destSpreadsheet.deleteSheet(existingSheet);
      }
      
      tempSheet.setName(destSheetTabName);
      logOperation(sessionId, "SHEET_COPIED", `Copied sheet with formatting to destination`);
      
      // For copyFormat mode, we can't do detailed verification since we're doing a direct copy
      return {
        status: "SUCCESS",
        rowsProcessed: sourceSheet.getLastRow(),
        rowsExpected: sourceSheet.getLastRow(),
        duration: (new Date() - startTime) / 1000
      };
    }
    
    // For other modes, get source data and process with verification
    const sourceData = sourceSheet.getDataRange().getValues();
    const dataHash = calculateDataHash(sourceData);
    
    logOperation(sessionId, "DATA_READ", `Read ${sourceData.length} rows x ${sourceData.length > 0 ? sourceData[0].length : 0} columns from source`);
    logOperation(sessionId, "DATA_HASH", `Source data hash: ${dataHash}`);
    
    // Open destination sheet
    const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);
    
    // Get before count if appending
    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      beforeRowCount = destSheet.getLastRow();
      logOperation(sessionId, "APPEND_MODE", `Destination had ${beforeRowCount} rows before append`);
    }
    
    // Process in batches for very large datasets
    const BATCH_SIZE = 1000; // Process 1000 rows at a time
    
    if (sheetHandlingMode === 'append') {
      // Append mode
      if (beforeRowCount === 0) {
        // Empty sheet - set all data at once
        destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
      } else {
        // For large datasets, split into batches
        if (sourceData.length > BATCH_SIZE) {
          // Process header row if needed 
          // Then process data in batches
          for (let i = 1; i < sourceData.length; i += BATCH_SIZE) {
            const batchSize = Math.min(BATCH_SIZE, sourceData.length - i);
            const batch = sourceData.slice(i, i + batchSize);
            destSheet.getRange(beforeRowCount + i, 1, batch.length, sourceData[0].length).setValues(batch);
            
            logOperation(sessionId, "BATCH_PROCESSED", `Processed batch of ${batch.length} rows`);
          }
        } else {
          // Small dataset - process in one go
          destSheet.getRange(beforeRowCount + 1, 1, sourceData.length - 1, sourceData[0].length)
                 .setValues(sourceData.slice(1));
        }
      }
    } else {
      // For clearAndReuse or recreate modes
      if (sourceData.length > BATCH_SIZE) {
        for (let i = 0; i < sourceData.length; i += BATCH_SIZE) {
          const batchSize = Math.min(BATCH_SIZE, sourceData.length - i);
          const batch = sourceData.slice(i, i + batchSize);
          destSheet.getRange(i + 1, 1, batch.length, sourceData[0].length).setValues(batch);
          
          logOperation(sessionId, "BATCH_PROCESSED", `Processed batch of ${batch.length} rows`);
        }
      } else {
        // Small dataset - process in one go
        destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
      }
    }
    
    // Verify the data was written correctly if verification is enabled
    if (CONFIG.VERIFICATION_CONFIG.ENABLED) {
      const afterRowCount = destSheet.getLastRow();
      const expectedRowCount = sheetHandlingMode === 'append' ? 
                              beforeRowCount + (sourceData.length - 1) : 
                              sourceData.length;
      
      // Verify row counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS) {
        if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
          // When appending, we expect all rows except header
          if (Math.abs(afterRowCount - expectedRowCount) > 1) { // Allow 1 row difference for potential header issues
            const errorMsg = `Row count mismatch: Expected ~${expectedRowCount} rows, got ${afterRowCount}`;
            logOperation(sessionId, "ROW_COUNT_ERROR", errorMsg);
            throw new Error(errorMsg);
          }
        } else {
          // For non-append operations, we expect exact match
          if (afterRowCount !== sourceData.length) {
            const errorMsg = `Row count mismatch: Expected ${sourceData.length} rows, got ${afterRowCount}`;
            logOperation(sessionId, "ROW_COUNT_ERROR", errorMsg);
            throw new Error(errorMsg);
          }
        }
      }
      
      // Verify column counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS) {
        const afterColCount = destSheet.getLastColumn();
        if (afterColCount < sourceData[0].length) {
          const errorMsg = `Column count mismatch: Expected ${sourceData[0].length} columns, got ${afterColCount}`;
          logOperation(sessionId, "COLUMN_COUNT_ERROR", errorMsg);
          throw new Error(errorMsg);
        }
      }
      
      // IMPROVED: Verify sample data with better type handling
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA) {
        const success = verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        
        if (!success) {
          throw new Error("Data verification failed: Sample data mismatch between source and destination");
        }
      }
      
      // Log verification
      logVerification({
        timestamp: new Date(),
        sessionId: sessionId,
        ruleId: ruleId,
        sourceType: "Google Sheet",
        sourceFile: `${sourceSheetId}:${sourceTabName}`,
        sourceMetadata: JSON.stringify({
          sheetId: sourceSheetId,
          tabName: sourceTabName,
          sheetName: sourceSpreadsheet.getName()
        }),
        destinationSheet: destSheetTabName,
        sourceRowCount: sourceData.length,
        destinationRowCount: afterRowCount,
        sourceColumnCount: sourceData[0].length,
        destinationColumnCount: destSheet.getLastColumn(),
        samplesMatch: true,
        dataHash: dataHash,
        isComplete: true
      });
    }
    
    logOperation(sessionId, "DATA_WRITTEN", `Wrote ${sourceData.length} rows to destination`);
    
    // Optimize column resizing - only do it for reasonable sized datasets
    if (sourceData.length > 0 && sourceData.length < 500 && sourceData[0].length < 20) {
      for (let i = 1; i <= sourceData[0].length; i++) {
        destSheet.autoResizeColumn(i);
      }
    }
    
    const duration = (new Date() - startTime) / 1000;
    Logger.log(`Google Sheet ingest completed successfully in ${duration} seconds`);
    
    return {
      status: "SUCCESS",
      rowsProcessed: sourceData.length,
      rowsExpected: sourceData.length,
      duration: duration,
      dataHash: dataHash
    };
  } catch (error) {
    Logger.log(`Google Sheet ingest error: ${error.message}`);
    throw new Error(`Google Sheet ingest error: ${error.message}`);
  }
}

/**
 * Enhanced process sheet push rule with verification
 * @param {string} sessionId - Session identifier
 * @param {Array} row - Configuration row
 * @param {Object} headerMap - Map of header names to column indices
 * @param {string} sheetHandlingMode - How to handle existing sheets
 * @param {string} ruleId - Identifier for the rule
 * @returns {Object} Result of processing including row counts
 */
function processSheetPushWithVerification(sessionId, row, headerMap, sheetHandlingMode, ruleId) {
  const startTime = new Date();
  Logger.log(`Starting sheet push process with verification`);
  
  const sourceTabName = getRequiredValue(row, headerMap, 'pushSourceTabName');
  const destSheetId = getResourceId(row, headerMap, 'pushDestinationSheetId', 'pushDestinationSheetUrl');
  const destTabName = getRequiredValue(row, headerMap, 'pushDestinationTabName');
  
  logOperation(sessionId, "PUSH_INIT", `Pushing from tab ${sourceTabName} to destination ${destSheetId}:${destTabName}`);
  
  try {
    const sourceSpreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
    
    if (!sourceSheet) {
      throw new Error(`Source sheet "${sourceTabName}" not found in active spreadsheet`);
    }
    
    // Get source data
    const sourceData = sourceSheet.getDataRange().getValues();
    const dataHash = calculateDataHash(sourceData);
    
    logOperation(sessionId, "DATA_READ", `Read ${sourceData.length} rows x ${sourceData.length > 0 ? sourceData[0].length : 0} columns from source`);
    logOperation(sessionId, "DATA_HASH", `Source data hash: ${dataHash}`);
    
    // Open destination sheet
    const destSheet = openDestinationSheet(destSheetId, destTabName, sheetHandlingMode);
    
    // Get before count if appending
    let beforeRowCount = 0;
    if (sheetHandlingMode === 'append') {
      beforeRowCount = destSheet.getLastRow();
      logOperation(sessionId, "APPEND_MODE", `Destination had ${beforeRowCount} rows before append`);
    }
    
    // Write the data
    if (sheetHandlingMode === 'append') {
      if (beforeRowCount === 0) {
        // If sheet is empty, just set all data
        destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
      } else {
        // Skip header row when appending
        destSheet.getRange(beforeRowCount + 1, 1, sourceData.length - 1, sourceData[0].length)
               .setValues(sourceData.slice(1));
      }
      logOperation(sessionId, "DATA_APPENDED", `Appended ${sourceData.length - 1} rows to destination`);
    } else {
      // For clearAndReuse or recreate, set all data
      destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
      logOperation(sessionId, "DATA_WRITTEN", `Wrote ${sourceData.length} rows to destination`);
    }
    
    // Verify the data was written correctly if verification is enabled
    if (CONFIG.VERIFICATION_CONFIG.ENABLED) {
      const afterRowCount = destSheet.getLastRow();
      const expectedRowCount = sheetHandlingMode === 'append' ? 
                              beforeRowCount + (sourceData.length - 1) : 
                              sourceData.length;
      
      // Verify row counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_ROW_COUNTS) {
        if (sheetHandlingMode === 'append' && beforeRowCount > 0) {
          // When appending, we expect all rows except header
          if (Math.abs(afterRowCount - expectedRowCount) > 1) { // Allow 1 row difference for potential header issues
            const errorMsg = `Row count mismatch: Expected ~${expectedRowCount} rows, got ${afterRowCount}`;
            logOperation(sessionId, "ROW_COUNT_ERROR", errorMsg);
            throw new Error(errorMsg);
          }
        } else {
          // For non-append operations, we expect exact match
          if (afterRowCount !== sourceData.length) {
            const errorMsg = `Row count mismatch: Expected ${sourceData.length} rows, got ${afterRowCount}`;
            logOperation(sessionId, "ROW_COUNT_ERROR", errorMsg);
            throw new Error(errorMsg);
          }
        }
      }
      
      // Verify column counts
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_COLUMN_COUNTS) {
        const afterColCount = destSheet.getLastColumn();
        if (afterColCount < sourceData[0].length) {
          const errorMsg = `Column count mismatch: Expected ${sourceData[0].length} columns, got ${afterColCount}`;
          logOperation(sessionId, "COLUMN_COUNT_ERROR", errorMsg);
          throw new Error(errorMsg);
        }
      }
      
      // IMPROVED: Verify sample data with better type handling
      if (CONFIG.VERIFICATION_CONFIG.VERIFY_SAMPLE_DATA) {
        const success = verifyDataSamples(sourceData, destSheet, sessionId, sheetHandlingMode, beforeRowCount);
        
        if (!success) {
          throw new Error("Data verification failed: Sample data mismatch between source and destination");
        }
      }
      
      // Log verification
      logVerification({
        timestamp: new Date(),
        sessionId: sessionId,
        ruleId: ruleId,
        sourceType: "Sheet Push",
        sourceFile: sourceTabName,
        sourceMetadata: JSON.stringify({
          sheetName: sourceSpreadsheet.getName(),
          tabName: sourceTabName
        }),
        destinationSheet: destTabName,
        sourceRowCount: sourceData.length,
        destinationRowCount: afterRowCount,
        sourceColumnCount: sourceData[0].length,
        destinationColumnCount: destSheet.getLastColumn(),
        samplesMatch: true,
        dataHash: dataHash,
        isComplete: true
      });
    }
    
    // Auto-resize columns for better display
    if (sourceData.length > 0 && sourceData.length < 500 && sourceData[0].length < 20) {
      for (let i = 1; i <= sourceData[0].length; i++) {
        destSheet.autoResizeColumn(i);
      }
    }
    
    const duration = (new Date() - startTime) / 1000;
    Logger.log(`Sheet push completed successfully in ${duration} seconds`);
    
    return {
      status: "SUCCESS",
      rowsProcessed: sourceData.length,
      rowsExpected: sourceData.length,
      duration: duration,
      dataHash: dataHash
    };
  } catch (error) {
    Logger.log(`Sheet push error: ${error.message}`);
    throw new Error(`Sheet push error: ${error.message}`);
  }
}

/**
 * Opens or creates a destination sheet based on the specified handling mode
 * @param {string} sheetId - ID of the destination spreadsheet
 * @param {string} tabName - Name of the tab
 * @param {string} handlingMode - How to handle existing sheets (clearAndReuse, recreate, copyFormat, append)
 * @returns {Sheet} The destination sheet object
 */
function openDestinationSheet(sheetId, tabName, handlingMode) {
  Logger.log(`Opening destination sheet: ID=${sheetId}, tab=${tabName}, mode=${handlingMode}`);
  
  const spreadsheet = SpreadsheetApp.openById(sheetId);
  Logger.log(`Opened spreadsheet: ${spreadsheet.getName()}`);
  
  let sheet = spreadsheet.getSheetByName(tabName);
  const sheetExists = sheet !== null;
  Logger.log(`Sheet "${tabName}" exists: ${sheetExists}`);
  
  if (!sheet) {
    // If sheet doesn't exist, create it regardless of mode
    sheet = spreadsheet.insertSheet(tabName);
    Logger.log(`Created new sheet tab: ${tabName}`);
  } else {
    // Handle existing sheet based on mode
    switch (handlingMode) {
      case 'clearAndReuse':
        Logger.log(`Using "clearAndReuse" mode - clearing sheet content`);
        sheet.clear();
        Logger.log(`Cleared existing sheet tab: ${tabName}`);
        break;
      case 'recreate':
        Logger.log(`Using "recreate" mode - deleting and recreating sheet`);
        // Create a new sheet first, then delete the old one to avoid losing the only sheet
        const newSheet = spreadsheet.insertSheet(tabName + "_new");
        spreadsheet.deleteSheet(sheet);
        newSheet.setName(tabName);
        sheet = newSheet;
        Logger.log(`Recreated sheet tab: ${tabName}`);
        break;
      case 'append':
        // Do nothing - data will be appended by the calling function
        Logger.log(`Using "append" mode - keeping existing sheet as is`);
        Logger.log(`Using existing sheet tab for append: ${tabName}`);
        break;
      case 'copyFormat':
        // Do nothing - will be handled by the calling function
        Logger.log(`Using "copyFormat" mode - handled by caller`);
        Logger.log(`Sheet will be replaced with formatting preserved: ${tabName}`);
        break;
      default:
        // Default to clearAndReuse
        Logger.log(`Unknown mode "${handlingMode}", defaulting to clearAndReuse`);
        sheet.clear();
        Logger.log(`Using default mode (clearAndReuse) for sheet tab: ${tabName}`);
    }
  }
  
  return sheet;
}

/**
 * IMPROVED: Creates or updates the ingestion configuration sheet with safer methods
 */
function createCfgIngestSheet() {
  const startTime = new Date();
  const sheetName = CONFIG.CONFIG_SHEET_NAME;
  Logger.log(`Creating/updating configuration sheet: ${sheetName}`);
  
  // Use the column mappings from CONFIG
  const headers = Object.values(CONFIG.COLUMN_MAPPINGS);
  Logger.log(`Headers to use: ${headers.join(', ')}`);
  
  // Sample data
  const sampleData = [
    true, 
    'email', 
    'clearAndReuse',
    'subject:(Monthly Report)', 
    'Monthly_Report_.*\\.csv', 
    '',
    '', 
    '', 
    'your_sheet_id_here',
    'https://docs.google.com/spreadsheets/d/your_sheet_id_here/edit',
    'imported_data',
    '',
    '',
    '',
    '',
    '',
    '',
    ''
  ];

  // Use the safer sheet creation method
  return ensureSafeSheetCreation(sheetName, (sheet) => {
    // Set headers with formatting
    Logger.log(`Setting headers and formatting for ${sheetName}`);
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setValues([headers]);
    
    // Apply formatting to headers
    const formatConfig = CONFIG.SHEET_FORMATS.CONFIG_SHEET;
    headerRange.setBackground(formatConfig.headerColor);
    headerRange.setFontWeight(formatConfig.headerFontWeight);
    
    // Add sample data to first row
    Logger.log(`Adding sample data to first row`);
    sheet.getRange(2, 1, 1, Math.min(sampleData.length, headers.length)).setValues([sampleData]);
    
    // Add more empty rows to accommodate growth
    const emptyRows = CONFIG.NEW_SHEET_ROWS - 1; // -1 because we already added sample row
    if (emptyRows > 0) {
      Logger.log(`Adding ${emptyRows} additional empty rows`);
      // Create empty rows with just the checkbox set to false
      const emptyData = Array(emptyRows).fill([false]);
      sheet.getRange(3, 1, emptyRows, 1).setValues(emptyData);
    }
    
    // Add data validation for ingest method
    Logger.log(`Adding data validation for ingest method column`);
    const methodRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(['email', 'gSheet', 'push'], true)
      .build();
    
    const ingestMethodCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.ingestMethod) + 1;
    if (ingestMethodCol > 0) {
      sheet.getRange(2, ingestMethodCol, CONFIG.NEW_SHEET_ROWS, 1).setDataValidation(methodRule);
    }
    
    // Add data validation for sheet handling mode
    Logger.log(`Adding data validation for sheet handling mode column`);
    const handlingRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(['clearAndReuse', 'recreate', 'copyFormat', 'append'], true)
      .build();
    
    const sheetHandlingModeCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.sheetHandlingMode) + 1;
    if (sheetHandlingModeCol > 0) {
      sheet.getRange(2, sheetHandlingModeCol, CONFIG.NEW_SHEET_ROWS, 1).setDataValidation(handlingRule);
    }
    
    // Get column positions for formatting
    const ruleActiveCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.ruleActive) + 1;
    const lastRunTimeCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.lastRunTime) + 1;
    const lastRunStatusCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.lastRunStatus) + 1;
    const lastRunMessageCol = headers.indexOf(CONFIG.COLUMN_MAPPINGS.lastRunMessage) + 1;
    
    if (ruleActiveCol > 0) {
      Logger.log(`Adding checkboxes to rule active column`);
      sheet.getRange(2, ruleActiveCol, CONFIG.NEW_SHEET_ROWS, 1).insertCheckboxes();
    }
    
    if (lastRunTimeCol > 0) {
      Logger.log(`Formatting last run time column`);
      sheet.getRange(2, lastRunTimeCol, CONFIG.NEW_SHEET_ROWS, 1)
           .setNumberFormat(formatConfig.timestampFormat);
    }
    
    // Set column widths
    Logger.log(`Setting column widths`);
    for (const [colName, width] of Object.entries(formatConfig.columnWidths)) {
      const colIndex = headers.indexOf(CONFIG.COLUMN_MAPPINGS[colName]);
      if (colIndex >= 0) {
        sheet.setColumnWidth(colIndex + 1, width);
      }
    }
    
    // Add help text to headers - add description notes
    Logger.log(`Adding description notes to column headers`);
    for (const [internalName, description] of Object.entries(CONFIG.COLUMN_DESCRIPTIONS)) {
      const columnName = CONFIG.COLUMN_MAPPINGS[internalName];
      const colIndex = headers.indexOf(columnName) + 1;
      if (colIndex > 0) {
        sheet.getRange(1, colIndex).setNote(description);
      }
    }
    
    // Freeze the header row
    Logger.log(`Freezing header row`);
    sheet.setFrozenRows(1);
    
    // Set up conditional formatting
    Logger.log(`Setting up conditional formatting`);
    setupConditionalFormatting(sheet);
    
    // Auto-resize all columns for better display
    Logger.log(`Auto-resizing columns`);
    for (let i = 1; i <= headers.length; i++) {
      sheet.autoResizeColumn(i);
    }
    
    Logger.log(`Finished setting up configuration sheet`);
  });
}

/**
 * Sets up conditional formatting for the configuration sheet
 * @param {Sheet} sheet - The configuration sheet
 */
function setupConditionalFormatting(sheet) {
  Logger.log(`Setting up conditional formatting for sheet: ${sheet.getName()}`);
  
  // Clear existing conditional formatting rules
  sheet.clearConditionalFormatRules();
  
  // Get the index of the status column
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const statusHeaderName = CONFIG.COLUMN_MAPPINGS.lastRunStatus;
  const statusColIndex = headers.indexOf(statusHeaderName);
  
  Logger.log(`Status column index: ${statusColIndex}`);
  
  // Create an array to hold all rules
  const rules = [];
  
  if (statusColIndex !== -1) {
    // Create conditional formatting for success status
    Logger.log(`Creating conditional formatting for SUCCESS status`);
    const successRange = sheet.getRange(2, statusColIndex + 1, Math.max(1, sheet.getLastRow() - 1), 1);
    const successRule = SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo("SUCCESS")
      .setBackground("#B7E1CD")  // Light green
      .setRanges([successRange])
      .build();
    
    // Create conditional formatting for error status
    Logger.log(`Creating conditional formatting for ERROR status`);
    const errorRule = SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo("ERROR")
      .setBackground("#F4C7C3")  // Light red
      .setRanges([successRange])
      .build();
    
    // Add conditional formatting for skipped status
    Logger.log(`Creating conditional formatting for SKIPPED status`);
    const skippedRule = SpreadsheetApp.newConditionalFormatRule()
      .whenTextEqualTo("SKIPPED")
      .setBackground("#FFFFCC")  // Light yellow
      .setRanges([successRange])
      .build();
    
    // Add rules to the array
    rules.push(successRule);
    rules.push(errorRule);
    rules.push(skippedRule);
  }
  
  // Create alternating row colors for readability
  Logger.log(`Creating alternating row colors`);
  const dataRange = sheet.getDataRange();
  const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
    .whenFormulaSatisfied("=ISEVEN(ROW())")
    .setBackground("#F0F8FF")  // Light blue
    .setRanges([dataRange])
    .build();
  
  rules.push(alternatingRule);
  
  // Set all the rules at once
  if (rules.length > 0) {
    Logger.log(`Applying ${rules.length} conditional formatting rules`);
    sheet.setConditionalFormatRules(rules);
  }
}