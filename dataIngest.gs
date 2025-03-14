/**
 * Data Ingestion System for Google Sheets
 * This script allows for automated data transfer between various sources (emails, Google Sheets)
 * and destinations with configurable behaviors and logging.
 */

// Global configuration
const CONFIG = {
    // Email notification can be a single address or an array of addresses
    EMAIL_NOTIFICATIONS: ["mark.thomsen@gsa.gov", "is-training@gsa.gov"], // Change these to your email(s)
    
    // Sheet names and settings
    LOG_SHEET_NAME: "ingest-logs",
    CONFIG_SHEET_NAME: "cfg-ingest",
    MAX_LOG_ENTRIES: 100,
    NEW_SHEET_ROWS: 5, // Default number of rows to add in new sheets
    
    // Formatting
    TIMESTAMP_FORMAT: "MM/dd/yyyy HH:mm:ss",
    ALTERNATE_ROW_COLOR: "#E6F2FF", // Light blue
    HEADER_BG_COLOR: "#D3D3D3",
    
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
  };
  
  /**
   * Creates the menu items when the spreadsheet is opened
   */
  function onOpen() {
    Logger.log("Initializing menu items");
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('Data Ingest')
      .addItem('Run All Rules', 'runAll')
      .addItem('Run Selected Rules', 'runSelectedRules')
      .addSeparator()
      .addItem('Create/Update Sheets', 'setupSheets')
      .addItem('Validate Configuration', 'validateConfiguration')
      .addToUi();
    Logger.log("Menu items created successfully");
  }
  
  /**
   * Creates or updates both configuration and log sheets
   */
  function setupSheets() {
    Logger.log("Starting setupSheets function");
    const ui = SpreadsheetApp.getUi();
    
    // Check if sheets already exist
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const configExists = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME) !== null;
    const logExists = ss.getSheetByName(CONFIG.LOG_SHEET_NAME) !== null;
    
    Logger.log(`Existing sheets check: Config exists: ${configExists}, Log exists: ${logExists}`);
    
    if (configExists || logExists) {
      let existingSheets = [];
      if (configExists) existingSheets.push(CONFIG.CONFIG_SHEET_NAME);
      if (logExists) existingSheets.push(CONFIG.LOG_SHEET_NAME);
      
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
        if (configExists) {
          backupSheet(ss, CONFIG.CONFIG_SHEET_NAME);
        }
        
        if (logExists) {
          backupSheet(ss, CONFIG.LOG_SHEET_NAME);
        }
        
        ui.alert('Backup Complete', 'Backup copies of the existing sheets have been created. Original sheets were not modified.', ui.ButtonSet.OK);
        return;
      } else if (response === ui.Button.NO) {
        // Backup and replace
        Logger.log("Creating backups and replacing existing sheets");
        if (configExists) {
          backupSheet(ss, CONFIG.CONFIG_SHEET_NAME);
        }
        
        if (logExists) {
          backupSheet(ss, CONFIG.LOG_SHEET_NAME);
        }
        
        // Continue to create/replace sheets
      }
    }
    
    // Create or replace the sheets
    Logger.log("Creating/updating configuration sheet");
    createCfgIngestSheet();
    
    Logger.log("Creating/updating log sheet");
    createLogSheet();
    
    ui.alert('Setup Complete', 'The configuration and log sheets have been created/updated successfully.', ui.ButtonSet.OK);
    Logger.log("setupSheets function completed successfully");
  }
  
  /**
   * Creates a backup of a sheet
   * @param {Spreadsheet} ss - The spreadsheet
   * @param {string} sheetName - Name of the sheet to back up
   * @returns {Sheet} The backup sheet
   */
  function backupSheet(ss, sheetName) {
    Logger.log(`Creating backup of sheet: ${sheetName}`);
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
      Logger.log(`Sheet ${sheetName} not found, cannot create backup`);
      return null;
    }
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupName = `${sheetName}-backup-${timestamp}`;
    const backupSheet = sheet.copyTo(ss).setName(backupName);
    Logger.log(`Created backup sheet: ${backupName}`);
    return backupSheet;
  }
  
  /**
   * Main function to run all active ingest rules
   */
  function runAll() {
    Logger.log("Starting runAll function");
    const sessionId = generateUniqueID();
    logOperation(sessionId, "START", "Starting full ingest process");
    
    try {
      ingestData(sessionId);
      logOperation(sessionId, "COMPLETE", "Ingest process completed successfully");
      Logger.log("runAll function completed successfully");
    } catch (error) {
      const errorMsg = `Error in main process: ${error.message}`;
      Logger.log(`Error in runAll: ${errorMsg}`);
      logOperation(sessionId, "ERROR", errorMsg);
      sendErrorNotification("Ingest Process Error", errorMsg);
    }
  }
  
  /**
   * Runs only the selected rules in the configuration sheet
   */
  function runSelectedRules() {
    Logger.log("Starting runSelectedRules function");
    const ui = SpreadsheetApp.getUi();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const configSheet = ss.getSheetByName(CONFIG.CONFIG_SHEET_NAME);
    
    if (!configSheet) {
      Logger.log("Configuration sheet not found");
      ui.alert('Error', 'Configuration sheet not found. Please create it first.', ui.ButtonSet.OK);
      return;
    }
    
    // Get the selected ranges
    const selectedRanges = ss.getActiveRangeList();
    if (!selectedRanges) {
      Logger.log("No ranges selected");
      ui.alert('Error', 'Please select one or more rows in the configuration sheet.', ui.ButtonSet.OK);
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
      Logger.log("Selection not on config sheet");
      ui.alert('Error', 'Please select rows in the configuration sheet.', ui.ButtonSet.OK);
      return;
    }
    
    if (selectedRows.length === 0) {
      Logger.log("No valid rows selected (header only)");
      ui.alert('Error', 'Please select one or more data rows (not the header row).', ui.ButtonSet.OK);
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
      return;
    }
    
    // Run the selected rules
    const sessionId = generateUniqueID();
    Logger.log(`Starting to run ${selectedRows.length} selected rules with session ID: ${sessionId}`);
    logOperation(sessionId, "START", `Running ${selectedRows.length} selected rule(s)`);
    
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
    
    for (const rowNum of selectedRows) {
      const rowData = configData[rowNum - 1]; // -1 because configData is 0-indexed
      const ruleId = `Rule #${rowNum}`;
      
      Logger.log(`Processing ${ruleId}`);
      
      try {
        // Check if rule is active
        const ruleActive = headerMap[ruleActiveKey] !== undefined ? 
                           rowData[headerMap[ruleActiveKey]] : false;
        
        Logger.log(`${ruleId} active status: ${ruleActive}`);
                           
        if (!ruleActive) {
          Logger.log(`${ruleId} is not active, skipping`);
          logOperation(sessionId, "SKIPPED", `${ruleId} is not active. Enable it to run.`);
          skippedCount++;
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
        
        // Process based on ingest method
        if (ingestMethod === 'email') {
          processEmailIngest(sessionId, rowData, headerMap, sheetHandlingMode);
        } else if (ingestMethod === 'gSheet') {
          processGSheetIngest(sessionId, rowData, headerMap, sheetHandlingMode);
        } else if (ingestMethod === 'push') {
          processSheetPush(sessionId, rowData, headerMap, sheetHandlingMode);
        } else {
          throw new Error(`Unknown ingest method: ${ingestMethod}`);
        }
        
        // Update status in the sheet
        updateRuleStatus(configSheet, rowNum, "SUCCESS", "Processed successfully");
        successCount++;
        Logger.log(`${ruleId} processed successfully`);
        logOperation(sessionId, "SUCCESS", `${ruleId} processed successfully`);
      } catch (error) {
        // Update status in the sheet
        Logger.log(`Error processing ${ruleId}: ${error.message}`);
        updateRuleStatus(configSheet, rowNum, "ERROR", error.message);
        errorCount++;
        logOperation(sessionId, "ERROR", `Error processing ${ruleId}: ${error.message}`);
      }
    }
    
    // Show summary
    const summary = `Run completed: ${successCount} successful, ${errorCount} failed, ${skippedCount} skipped`;
    Logger.log(summary);
    logOperation(sessionId, "COMPLETE", summary);
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
    
    // Process each row in the config
    for (let i = 1; i < configData.length; i++) {
      const row = configData[i];
      const rowNum = i + 1;
      Logger.log(`Checking row ${rowNum}`);
      
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
        
        if (ingestMethod === 'email') {
          processEmailIngest(sessionId, row, headerMap, sheetHandlingMode);
        } else if (ingestMethod === 'gSheet') {
          processGSheetIngest(sessionId, row, headerMap, sheetHandlingMode);
        } else if (ingestMethod === 'push') {
          processSheetPush(sessionId, row, headerMap, sheetHandlingMode);
        } else {
          throw new Error(`Unknown ingest method: ${ingestMethod}`);
        }
        
        updateRuleStatus(configSheet, rowNum, "SUCCESS", `Processed ${ingestMethod} rule successfully`);
        Logger.log(`${ruleId} processed successfully`);
        successCount++;
      } catch (error) {
        Logger.log(`Error processing ${ruleId}: ${error.message}`);
        updateRuleStatus(configSheet, rowNum, "ERROR", error.message);
        logOperation(sessionId, "ERROR", `Error processing ${ruleId}: ${error.message}`);
        sendErrorNotification(`Error in ${ruleId}`, error.message);
        errorCount++;
      }
    }
    
    const summary = `Run completed: ${successCount} successful, ${errorCount} failed, ${skippedCount} skipped`;
    Logger.log(summary);
    logOperation(sessionId, "SUMMARY", summary);
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
   * Process email ingestion rule
   * @param {string} sessionId - Session identifier
   * @param {Array} row - Configuration row
   * @param {Object} headerMap - Map of header names to column indices
   * @param {string} sheetHandlingMode - How to handle existing sheets
   */
  function processEmailIngest(sessionId, row, headerMap, sheetHandlingMode) {
    const startTime = new Date();
    Logger.log(`Starting email ingest process`);
    
    const searchString = getRequiredValue(row, headerMap, 'in_email_searchString');
    Logger.log(`Email search string: ${searchString}`);
    
    const attachmentPattern = getRequiredValue(row, headerMap, 'in_email_attachmentPattern');
    Logger.log(`Attachment pattern: ${attachmentPattern}`);
    
    const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
    Logger.log(`Destination sheet ID: ${destSheetId}`);
    
    const destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName');
    Logger.log(`Destination tab name: ${destSheetTabName}`);
    
    logOperation(sessionId, "EMAIL_SEARCH", `Searching emails with: ${searchString}`);
    
    try {
      Logger.log(`Executing Gmail search: ${searchString}`);
      const threads = GmailApp.search(searchString);
      Logger.log(`Found ${threads.length} matching threads`);
      
      const emails = GmailApp.getMessagesForThreads(threads).flat();
      Logger.log(`Found ${emails.length} total emails within matching threads`);
      
      logOperation(sessionId, "EMAIL_FOUND", `Found ${emails.length} emails matching search criteria`);
      
      if (emails.length === 0) {
        throw new Error("No emails found matching the search criteria");
      }
      
      const attachmentRegex = new RegExp(attachmentPattern);
      let processedAttachment = false;
      let messagesChecked = 0;
      
      for (const email of emails) {
        messagesChecked++;
        Logger.log(`Checking email ${messagesChecked}/${emails.length} - Subject: "${email.getSubject()}" from: ${email.getFrom()}`);
        
        const attachments = email.getAttachments();
        Logger.log(`Email has ${attachments.length} attachments`);
        
        if (attachments.length > 0) {
          const matchedAttachments = attachments.filter(att => {
            const matches = attachmentRegex.test(att.getName());
            Logger.log(`Attachment "${att.getName()}" matches pattern: ${matches}`);
            return matches;
          });
          
          if (matchedAttachments.length === 1) {
            const attachment = matchedAttachments[0];
            Logger.log(`Processing matching attachment: ${attachment.getName()} (${attachment.getSize()} bytes)`);
            logOperation(sessionId, "ATTACHMENT_FOUND", `Found matching attachment: ${attachment.getName()}`);
            
            Logger.log(`Opening destination sheet: ${destSheetId}, tab: ${destSheetTabName}, mode: ${sheetHandlingMode}`);
            const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);
            
            Logger.log(`Parsing CSV data from attachment`);
            const data = Utilities.parseCsv(attachment.getDataAsString());
            Logger.log(`Parsed CSV with ${data.length} rows and ${data.length > 0 ? data[0].length : 0} columns`);
            
            if (data.length === 0 || data[0].length === 0) {
              throw new Error("Attachment contains no data");
            }
            
            Logger.log(`Writing data to destination sheet`);
            destSheet.getRange(1, 1, data.length, data[0].length).setValues(data);
            
            // Auto-resize columns for better display
            Logger.log(`Auto-resizing columns`);
            for (let i = 1; i <= data[0].length; i++) {
              destSheet.autoResizeColumn(i);
            }
            
            logOperation(sessionId, "DATA_WRITTEN", `Wrote ${data.length} rows to destination sheet`);
            Logger.log(`Data successfully written to destination sheet`);
            
            processedAttachment = true;
            break; // Process only the first matching email
          } else if (matchedAttachments.length > 1) {
            Logger.log(`Multiple matching attachments found (${matchedAttachments.length}), throwing error`);
            throw new Error(`Multiple attachments matching the pattern ${attachmentPattern} found in email`);
          }
        }
      }
      
      if (!processedAttachment) {
        Logger.log(`No matching attachments found in any email`);
        throw new Error(`No attachment matching the pattern ${attachmentPattern} found in any email`);
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Email ingest completed successfully in ${duration} seconds`);
    } catch (error) {
      Logger.log(`Email ingest error: ${error.message}`);
      throw new Error(`Email ingest error: ${error.message}`);
    }
  }
  
  /**
   * Process Google Sheet ingestion rule
   * @param {string} sessionId - Session identifier
   * @param {Array} row - Configuration row
   * @param {Object} headerMap - Map of header names to column indices
   * @param {string} sheetHandlingMode - How to handle existing sheets
   */
  function processGSheetIngest(sessionId, row, headerMap, sheetHandlingMode) {
    const startTime = new Date();
    Logger.log(`Starting Google Sheet ingest process`);
    
    const sourceSheetId = getResourceId(row, headerMap, 'in_gsheet_sheetId', 'in_gsheet_sheetURL');
    Logger.log(`Source sheet ID: ${sourceSheetId}`);
    
    const sourceTabName = getRequiredValue(row, headerMap, 'in_gsheet_tabName');
    Logger.log(`Source tab name: ${sourceTabName}`);
    
    const destSheetId = getResourceId(row, headerMap, 'dest_sheetId', 'dest_sheetUrl');
    Logger.log(`Destination sheet ID: ${destSheetId}`);
    
    const destSheetTabName = getRequiredValue(row, headerMap, 'dest_sheet_tabName');
    Logger.log(`Destination tab name: ${destSheetTabName}`);
    
    logOperation(sessionId, "SHEET_ACCESS", `Accessing source sheet: ${sourceSheetId}, tab: ${sourceTabName}`);
    
    try {
      Logger.log(`Opening source spreadsheet`);
      const sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
      Logger.log(`Source spreadsheet opened: ${sourceSpreadsheet.getName()}`);
      
      Logger.log(`Accessing source tab: ${sourceTabName}`);
      const sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
      
      if (!sourceSheet) {
        Logger.log(`Source tab "${sourceTabName}" not found`);
        throw new Error(`Source sheet tab "${sourceTabName}" not found`);
      }
      
      Logger.log(`Opening destination spreadsheet`);
      const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
      Logger.log(`Destination spreadsheet opened: ${destSpreadsheet.getName()}`);
      
      if (sheetHandlingMode === 'copyFormat') {
        Logger.log(`Using "copyFormat" mode - copying sheet with formatting`);
        
        // Copy the sheet and rename it (handles formatting)
        Logger.log(`Copying source sheet to destination spreadsheet`);
        const tempSheet = sourceSheet.copyTo(destSpreadsheet);
        
        // If the destination already exists, delete it
        const existingSheet = destSpreadsheet.getSheetByName(destSheetTabName);
        if (existingSheet && existingSheet.getSheetId() !== tempSheet.getSheetId()) {
          Logger.log(`Deleting existing destination tab: ${destSheetTabName}`);
          destSpreadsheet.deleteSheet(existingSheet);
        }
        
        Logger.log(`Renaming copied sheet to: ${destSheetTabName}`);
        tempSheet.setName(destSheetTabName);
        logOperation(sessionId, "SHEET_COPIED", `Copied sheet with formatting to destination`);
      } else {
        // Just copy the data
        Logger.log(`Using "${sheetHandlingMode}" mode - copying data only`);
        
        Logger.log(`Opening/creating destination sheet`);
        const destSheet = openDestinationSheet(destSheetId, destSheetTabName, sheetHandlingMode);
        
        Logger.log(`Getting data from source sheet`);
        const sourceData = sourceSheet.getDataRange().getValues();
        Logger.log(`Source data has ${sourceData.length} rows and ${sourceData.length > 0 ? sourceData[0].length : 0} columns`);
        
        if (sheetHandlingMode === 'append') {
          // Find the last row with data
          const lastRow = destSheet.getLastRow();
          Logger.log(`Destination sheet last row: ${lastRow}`);
          
          if (lastRow === 0) {
            // If sheet is empty, just set all data
            Logger.log(`Destination sheet is empty, copying all data`);
            destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
          } else {
            // Skip header row when appending
            Logger.log(`Appending data starting at row ${lastRow + 1}, skipping header row`);
            destSheet.getRange(lastRow + 1, 1, sourceData.length - 1, sourceData[0].length)
                   .setValues(sourceData.slice(1));
          }
          logOperation(sessionId, "DATA_APPENDED", `Appended ${sourceData.length - 1} rows to destination`);
        } else {
          // For clearAndReuse or recreate, set all data
          Logger.log(`Setting all data in destination sheet`);
          destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
          logOperation(sessionId, "DATA_WRITTEN", `Wrote ${sourceData.length} rows to destination`);
        }
        
        // Auto-resize columns for better display
        if (sourceData.length > 0) {
          Logger.log(`Auto-resizing columns`);
          for (let i = 1; i <= sourceData[0].length; i++) {
            destSheet.autoResizeColumn(i);
          }
        }
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Google Sheet ingest completed successfully in ${duration} seconds`);
    } catch (error) {
      Logger.log(`Google Sheet ingest error: ${error.message}`);
      throw new Error(`Google Sheet ingest error: ${error.message}`);
    }
  }
  
  /**
   * Process sheet push rule
   * @param {string} sessionId - Session identifier
   * @param {Array} row - Configuration row
   * @param {Object} headerMap - Map of header names to column indices
   * @param {string} sheetHandlingMode - How to handle existing sheets
   */
  function processSheetPush(sessionId, row, headerMap, sheetHandlingMode) {
    const startTime = new Date();
    Logger.log(`Starting sheet push process`);
    
    const sourceTabName = getRequiredValue(row, headerMap, 'pushSourceTabName');
    Logger.log(`Source tab name: ${sourceTabName}`);
    
    const destSheetId = getResourceId(row, headerMap, 'pushDestinationSheetId', 'pushDestinationSheetUrl');
    Logger.log(`Destination sheet ID: ${destSheetId}`);
    
    const destTabName = getRequiredValue(row, headerMap, 'pushDestinationTabName');
    Logger.log(`Destination tab name: ${destTabName}`);
    
    logOperation(sessionId, "PUSH_INIT", `Pushing from tab ${sourceTabName} to destination ${destSheetId}:${destTabName}`);
    
    try {
      Logger.log(`Getting active spreadsheet`);
      const sourceSpreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      Logger.log(`Current spreadsheet: ${sourceSpreadsheet.getName()}`);
      
      Logger.log(`Accessing source tab: ${sourceTabName}`);
      const sourceSheet = sourceSpreadsheet.getSheetByName(sourceTabName);
      
      if (!sourceSheet) {
        Logger.log(`Source tab "${sourceTabName}" not found in active spreadsheet`);
        throw new Error(`Source sheet "${sourceTabName}" not found in active spreadsheet`);
      }
      
      Logger.log(`Opening/creating destination sheet with mode: ${sheetHandlingMode}`);
      const destSheet = openDestinationSheet(destSheetId, destTabName, sheetHandlingMode);
      
      Logger.log(`Getting data from source sheet`);
      const sourceData = sourceSheet.getDataRange().getValues();
      Logger.log(`Source data has ${sourceData.length} rows and ${sourceData.length > 0 ? sourceData[0].length : 0} columns`);
      
      if (sheetHandlingMode === 'append') {
        // Find the last row with data
        const lastRow = destSheet.getLastRow();
        Logger.log(`Destination sheet last row: ${lastRow}`);
        
        if (lastRow === 0) {
          // If sheet is empty, just set all data
          Logger.log(`Destination sheet is empty, copying all data`);
          destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
        } else {
          // Skip header row when appending
          Logger.log(`Appending data starting at row ${lastRow + 1}, skipping header row`);
          destSheet.getRange(lastRow + 1, 1, sourceData.length - 1, sourceData[0].length)
                 .setValues(sourceData.slice(1));
        }
        logOperation(sessionId, "DATA_APPENDED", `Appended ${sourceData.length - 1} rows to destination`);
      } else {
        // For clearAndReuse or recreate, set all data
        Logger.log(`Setting all data in destination sheet`);
        destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
        logOperation(sessionId, "DATA_WRITTEN", `Wrote ${sourceData.length} rows to destination`);
      }
      
      // Auto-resize columns for better display
      if (sourceData.length > 0) {
        Logger.log(`Auto-resizing columns`);
        for (let i = 1; i <= sourceData[0].length; i++) {
          destSheet.autoResizeColumn(i);
        }
      }
      
      const duration = (new Date() - startTime) / 1000;
      Logger.log(`Sheet push completed successfully in ${duration} seconds`);
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
          spreadsheet.deleteSheet(sheet);
          sheet = spreadsheet.insertSheet(tabName);
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
   * Creates or updates the ingestion configuration sheet
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
  
    // Check if the sheet already exists
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName);
    
    if (sheet) {
      // Delete the existing sheet if we're replacing it
      Logger.log(`Deleting existing sheet: ${sheetName}`);
      ss.deleteSheet(sheet);
    }
    
    // Create the sheet
    Logger.log(`Creating new sheet: ${sheetName}`);
    sheet = ss.insertSheet(sheetName);
    
    // Set headers with formatting
    Logger.log(`Setting headers and formatting`);
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setValues([headers]);
    headerRange.setBackground(CONFIG.HEADER_BG_COLOR);
    headerRange.setFontWeight('bold');
  
    // Add sample data to first row and specified number of empty rows
    Logger.log(`Adding sample data to first row`);
    sheet.getRange(2, 1, 1, Math.min(sampleData.length, headers.length)).setValues([sampleData]);
    
    // Add empty rows 
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
    
    // Add checkbox for active rule
    if (ruleActiveCol > 0) {
      Logger.log(`Adding checkboxes to rule active column`);
      sheet.getRange(2, ruleActiveCol, CONFIG.NEW_SHEET_ROWS, 1).insertCheckboxes();
    }
    
    // Format date column
    if (lastRunTimeCol > 0) {
      Logger.log(`Formatting last run time column`);
      sheet.getRange(2, lastRunTimeCol, CONFIG.NEW_SHEET_ROWS, 1)
           .setNumberFormat(CONFIG.TIMESTAMP_FORMAT);
    }
    
    // Set column widths
    Logger.log(`Setting column widths`);
    if (ruleActiveCol > 0) sheet.setColumnWidth(ruleActiveCol, 90);
    if (ingestMethodCol > 0) sheet.setColumnWidth(ingestMethodCol, 120);
    if (sheetHandlingModeCol > 0) sheet.setColumnWidth(sheetHandlingModeCol, 150);
    if (lastRunTimeCol > 0) sheet.setColumnWidth(lastRunTimeCol, 150);
    if (lastRunStatusCol > 0) sheet.setColumnWidth(lastRunStatusCol, 100);
    if (lastRunMessageCol > 0) sheet.setColumnWidth(lastRunMessageCol, 200);
    
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
    
    const duration = (new Date() - startTime) / 1000;
    Logger.log(`Created configuration sheet successfully in ${duration} seconds`);
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
      
      // Add rules to the array
      rules.push(successRule);
      rules.push(errorRule);
    }
    
    // Create alternating row colors for readability
    Logger.log(`Creating alternating row colors`);
    const dataRange = sheet.getDataRange();
    const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied("=ISEVEN(ROW())")
      .setBackground(CONFIG.ALTERNATE_ROW_COLOR)  // Light blue
      .setRanges([dataRange])
      .build();
    
    rules.push(alternatingRule);
    
    // Set all the rules at once
    if (rules.length > 0) {
      Logger.log(`Applying ${rules.length} conditional formatting rules`);
      sheet.setConditionalFormatRules(rules);
    }
  }
  
  /**
   * Creates or updates the log sheet
   */
  function createLogSheet() {
    const startTime = new Date();
    const sheetName = CONFIG.LOG_SHEET_NAME;
    Logger.log(`Creating/updating log sheet: ${sheetName}`);
    
    const headers = ['Timestamp', 'SessionID', 'EventType', 'Message'];
    
    // Check if the sheet already exists
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(sheetName);
    
    if (sheet) {
      // Delete the existing sheet if we're replacing it
      Logger.log(`Deleting existing log sheet: ${sheetName}`);
      ss.deleteSheet(sheet);
    }
    
    // Create the sheet
    Logger.log(`Creating new log sheet: ${sheetName}`);
    sheet = ss.insertSheet(sheetName);
    
    // Set headers with formatting
    Logger.log(`Setting headers and formatting`);
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setValues([headers]);
    headerRange.setBackground(CONFIG.HEADER_BG_COLOR);
    headerRange.setFontWeight('bold');
    
    // Format timestamp column
    Logger.log(`Formatting timestamp column`);
    sheet.getRange(2, 1, CONFIG.NEW_SHEET_ROWS, 1).setNumberFormat(CONFIG.TIMESTAMP_FORMAT);
    
    // Set column widths
    Logger.log(`Setting column widths`);
    sheet.setColumnWidth(1, 180); // Timestamp
    sheet.setColumnWidth(2, 180); // SessionID
    sheet.setColumnWidth(3, 120); // EventType
    sheet.setColumnWidth(4, 500); // Message
    
    // Freeze the header row
    Logger.log(`Freezing header row`);
    sheet.setFrozenRows(1);
    
    // Add alternating row colors
    Logger.log(`Adding alternating row colors`);
    const dataRange = sheet.getDataRange();
    const alternatingRule = SpreadsheetApp.newConditionalFormatRule()
      .whenFormulaSatisfied("=ISEVEN(ROW())")
      .setBackground(CONFIG.ALTERNATE_ROW_COLOR)  // Light blue
      .setRanges([dataRange])
      .build();
    
    sheet.setConditionalFormatRules([alternatingRule]);
    
    // Auto-resize all columns for better display
    Logger.log(`Auto-resizing columns`);
    for (let i = 1; i <= headers.length; i++) {
      sheet.autoResizeColumn(i);
    }
    
    const duration = (new Date() - startTime) / 1000;
    Logger.log(`Created log sheet successfully in ${duration} seconds`);
  }
  
  /**
   * Logs an operation to the log sheet
   * @param {string} sessionId - Session identifier
   * @param {string} eventType - Type of event
   * @param {string} message - Log message
   */
  function logOperation(sessionId, eventType, message) {
    Logger.log(`Logging operation: [${sessionId}] ${eventType}: ${message}`);
    
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      let logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
      
      // Create log sheet if it doesn't exist
      if (!logSheet) {
        Logger.log(`Log sheet not found, creating new one`);
        createLogSheet();
        logSheet = ss.getSheetByName(CONFIG.LOG_SHEET_NAME);
      }
      
      // Get the timestamp
      const timestamp = new Date();
      
      // Add log entry to the top (row 2, just below headers)
      Logger.log(`Adding log entry to sheet`);
      logSheet.insertRowAfter(1);
      const newRow = logSheet.getRange(2, 1, 1, 4);
      newRow.setValues([[timestamp, sessionId, eventType, message]]);
      
      // Format timestamp
      logSheet.getRange(2, 1).setNumberFormat(CONFIG.TIMESTAMP_FORMAT);
      
      // Trim log to maximum entries
      const totalRows = logSheet.getLastRow();
      if (totalRows > CONFIG.MAX_LOG_ENTRIES + 1) {
        const deleteCount = totalRows - CONFIG.MAX_LOG_ENTRIES - 1;
        Logger.log(`Trimming log, deleting ${deleteCount} oldest entries`);
        logSheet.deleteRows(CONFIG.MAX_LOG_ENTRIES + 2, deleteCount);
      }
      
      // Auto-resize if needed
      logSheet.autoResizeColumn(4); // Message column
    } catch (error) {
      // Fall back to Logger if we can't write to the log sheet
      Logger.log(`ERROR LOGGING: ${error.message}`);
      Logger.log(`Original log: [${sessionId}] ${eventType}: ${message}`);
    }
  }
  
  /**
   * Sends an error notification email
   * @param {string} subject - Email subject
   * @param {string} message - Email message
   */
  function sendErrorNotification(subject, message) {
    Logger.log(`Preparing to send error notification: ${subject}`);
    
    try {
      if (CONFIG.EMAIL_NOTIFICATIONS && CONFIG.EMAIL_NOTIFICATIONS.length > 0) {
        const fullMessage = `Error in Data Ingest System:
        
  Time: ${new Date().toLocaleString()}
  Error: ${message}
  
  Please check the log sheet for details.`;
        
        // Handle both single email and array of emails
        const emails = Array.isArray(CONFIG.EMAIL_NOTIFICATIONS) 
          ? CONFIG.EMAIL_NOTIFICATIONS 
          : [CONFIG.EMAIL_NOTIFICATIONS];
        
        Logger.log(`Sending error notification to ${emails.length} recipient(s): ${emails.join(', ')}`);
        
        // Send to each email
        emails.forEach(email => {
          if (email && email.trim() !== '') {
            GmailApp.sendEmail(email, subject, fullMessage);
            Logger.log(`Notification sent to: ${email}`);
          }
        });
      } else {
        Logger.log(`No notification recipients configured, skipping email notification`);
      }
    } catch (error) {
      Logger.log(`ERROR SENDING NOTIFICATION: ${error.message}`);
    }
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