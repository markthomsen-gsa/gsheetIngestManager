/**
 * Data Ingest Manager - Rule Processing
 * 
 * Handles the execution of ingest rules and data processing.
 */

/**
 * Runs a set of rules by their IDs
 * @param {Array} ruleIds - Array of rule IDs to run
 * @return {Object} Execution results with success and error counts
 */
function runRules(ruleIds) {
  const sessionId = createLogSession(`Running ${ruleIds.length} rule(s)`);
  const config = getConfig();
  
  let successCount = 0;
  let errorCount = 0;
  
  for (const ruleId of ruleIds) {
    const rule = config.rules.find(r => r.id === ruleId);
    
    if (!rule) {
      addLogEvent(sessionId, 'ERROR', `Rule ${ruleId} not found`);
      errorCount++;
      continue;
    }
    
    if (rule.active === false) {
      addLogEvent(sessionId, 'SKIPPED', `Rule ${rule.description || ruleId} is not active`);
      continue;
    }
    
    try {
      addLogEvent(sessionId, 'PROCESSING', `Processing ${rule.description || ruleId}`);
      
      // Process based on method
      if (rule.method === 'email') {
        processEmailRule(rule, sessionId);
      } else if (rule.method === 'gSheet') {
        processGSheetRule(rule, sessionId);
      } else {
        throw new Error(`Unsupported method: ${rule.method}`);
      }
      
      // Update rule status
      rule.status = {
        lastRun: new Date().toISOString(),
        result: 'SUCCESS',
        message: 'Processed successfully'
      };
      
      addLogEvent(sessionId, 'SUCCESS', `Rule ${rule.description || ruleId} processed successfully`);
      successCount++;
    } catch (error) {
      rule.status = {
        lastRun: new Date().toISOString(),
        result: 'ERROR',
        message: error.message
      };
      
      addLogEvent(sessionId, 'ERROR', `Error processing ${rule.description || ruleId}: ${error.message}`);
      errorCount++;
    }
  }
  
  // Save updated rule statuses
  saveConfig(config);
  
  const summary = `Completed: ${successCount} successful, ${errorCount} failed`;
  addLogEvent(sessionId, 'COMPLETE', summary);
  
  return {
    sessionId: sessionId,
    successCount: successCount,
    errorCount: errorCount,
    message: summary
  };
}

/**
 * Processes an email ingest rule
 * @param {Object} rule - The rule configuration object
 * @param {string} sessionId - The log session ID
 * @return {Object} Processing results with success status and details
 */
function processEmailRule(rule, sessionId) {
  const startTime = new Date();
  Logger.log(`Starting email ingest process`);
  
  // Create a unique processing ID for tracking progress and cancellation
  const processingId = 'process-' + new Date().getTime();
  
  // Initialize progress tracking
  initializeProcessTracking(processingId, 'email');
  
  const searchString = rule.source.emailSearch;
  Logger.log(`Email search string: ${searchString}`);
  
  const attachmentPattern = rule.source.attachmentPattern;
  Logger.log(`Attachment pattern: ${attachmentPattern}`);
  
  addLogEvent(sessionId, 'EMAIL_SEARCH', `Searching emails with: ${searchString}`);
  updateProcessProgress(processingId, 5, 'Searching for matching emails...');
  
  try {
    Logger.log(`Executing Gmail search: ${searchString}`);
    const threads = GmailApp.search(searchString);
    Logger.log(`Found ${threads.length} matching threads`);
    
  if (threads.length === 0) {
      Logger.log(`No matching threads found`);
      updateProcessProgress(processingId, 100, 'Complete - No emails found');
    throw new Error('No emails found matching the search criteria');
  }
  
    if (threads.length > 1) {
      Logger.log(`Multiple matching threads found (${threads.length}), throwing error`);
      updateProcessProgress(processingId, 100, 'Complete - Error: Multiple emails found');
      throw new Error(`Multiple emails found matching the search criteria. Expected exactly one result.`);
    }
    
    updateProcessProgress(processingId, 10, 'Found matching email, checking for attachments...');
  const emails = GmailApp.getMessagesForThreads(threads).flat();
    Logger.log(`Found ${emails.length} total emails within matching threads`);
  addLogEvent(sessionId, 'EMAIL_FOUND', `Found ${emails.length} emails matching search criteria`);
  
    const attachmentRegex = new RegExp(attachmentPattern);
  let processedAttachment = false;
    let messagesChecked = 0;
  
  for (const email of emails) {
      // Check if the process has been cancelled
      if (isProcessCancelled(processingId)) {
        updateProcessProgress(processingId, 100, 'Complete - Process cancelled by user');
        addLogEvent(sessionId, 'CANCELLED', 'Process was cancelled by user');
        return {
          success: false,
          cancelled: true,
          message: 'Process cancelled by user'
        };
      }
      
      messagesChecked++;
      const emailSubject = email.getSubject();
      const emailFrom = email.getFrom();
      const emailDate = email.getDate().toLocaleString();
      
      Logger.log(`Checking email ${messagesChecked}/${emails.length} - Subject: "${emailSubject}" from: ${emailFrom}`);
      addLogEvent(sessionId, 'EMAIL_CHECK', 
        `Checking email - Subject: "${emailSubject}" from: ${emailFrom} (${emailDate})`);
      
    const attachments = email.getAttachments();
      Logger.log(`Email has ${attachments.length} attachments`);
      
      if (attachments.length === 0) {
        Logger.log(`No attachments in email "${emailSubject}"`);
        addLogEvent(sessionId, 'EMAIL_SKIP', `No attachments in email "${emailSubject}"`);
        continue;
      }
      
      updateProcessProgress(processingId, 15, 
        `Found ${attachments.length} attachments, checking for matches...`);
      
      Logger.log(`Checking ${attachments.length} attachments for pattern match`);
      addLogEvent(sessionId, 'ATTACHMENT_CHECK', 
        `Found ${attachments.length} attachments in email "${emailSubject}"`);
      
      const matchedAttachments = attachments.filter(att => {
        const matches = attachmentRegex.test(att.getName());
        Logger.log(`Attachment "${att.getName()}" matches pattern: ${matches}`);
        addLogEvent(sessionId, 'ATTACHMENT_MATCH', 
          `Attachment "${att.getName()}" matches pattern: ${matches}`);
        return matches;
      });
    
    if (matchedAttachments.length === 1) {
      const attachment = matchedAttachments[0];
        Logger.log(`Processing matching attachment: ${attachment.getName()} (${attachment.getSize()} bytes)`);
        addLogEvent(sessionId, 'ATTACHMENT_FOUND', 
          `Processing matching attachment: ${attachment.getName()} (${attachment.getSize()} bytes) from email "${emailSubject}"`);
        
        updateProcessProgress(processingId, 20, 
          `Found matching attachment: ${attachment.getName()}, preparing to process...`);
        
        try {
          // Get current spreadsheet and destination sheet
          Logger.log(`Opening destination sheet`);
          const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
      const destSheet = openOrCreateDestinationSheet(
            spreadsheet.getId(), 
        rule.destination.tabName, 
        rule.destination.handlingMode || 'clearAndReuse'
      );
      
          updateProcessProgress(processingId, 25, 'Preparing destination sheet...');
          
          // Process the attachment in batches
          const result = processEmailAttachmentInBatches(
            attachment, 
            destSheet, 
            sessionId, 
            processingId
          );
          
      processedAttachment = true;
          
          const duration = (new Date() - startTime) / 1000;
          Logger.log(`Data successfully written to destination sheet`);
          
          // Create completion message with validation information
          let completionMessage = `Wrote ${result.totalRows} rows to destination sheet in ${duration.toFixed(2)} seconds`;
          
          // Add validation information
          if (result.rowCountValidation) {
            const validation = result.rowCountValidation;
            const isOffByOne = Math.abs(validation.finalCount - validation.initialCount) === 1;
            
            if (validation.match || isOffByOne) {
              // For exact matches or off-by-one differences, show success message
              completionMessage += `. Row validation: ${isOffByOne ? 'ACCEPTABLE' : 'SUCCESS'} (${validation.finalCount}/${validation.initialCount})`;
            } else {
              // For significant mismatches, show warning
              completionMessage += `. Row validation: WARNING (${validation.finalCount}/${validation.initialCount})`;
            }
          }
          
          addLogEvent(sessionId, 'DATA_WRITTEN', completionMessage);
          
          let progressMessage = `Complete - Successfully imported ${result.totalRows} rows in ${duration.toFixed(2)} seconds`;
          
          // Add validation info to progress message too
          if (result.rowCountValidation) {
            const validation = result.rowCountValidation;
            const isOffByOne = Math.abs(validation.finalCount - validation.initialCount) === 1;
            
            if (!validation.match && !isOffByOne) {
              progressMessage += ` (WARNING: Row count mismatch! ${validation.finalCount}/${validation.initialCount})`;
            } else if (isOffByOne) {
              progressMessage += ` (Note: Minor row count difference ${validation.finalCount}/${validation.initialCount})`;
            }
          }
          
          updateProcessProgress(processingId, 100, progressMessage);
          
      break;
        } catch (error) {
          Logger.log(`Error processing attachment: ${error.message}`);
          addLogEvent(sessionId, 'ERROR', 
            `Error processing attachment from email "${emailSubject}": ${error.message}`);
          
          updateProcessProgress(processingId, 100, `Complete - Error: ${error.message}`);
          
          throw error;
        }
    } else if (matchedAttachments.length > 1) {
        Logger.log(`Multiple matching attachments found (${matchedAttachments.length}), throwing error`);
        updateProcessProgress(processingId, 100, 'Complete - Error: Multiple matching attachments found');
        throw new Error(`Multiple attachments matching the pattern found in email "${emailSubject}"`);
    }
  }
  
  if (!processedAttachment) {
      Logger.log(`No matching attachments found in any email`);
      updateProcessProgress(processingId, 100, 'Complete - No matching attachments found');
      throw new Error('No attachment matching the pattern found in the email');
    }
    
    const totalDuration = (new Date() - startTime) / 1000;
    Logger.log(`Email ingest completed successfully in ${totalDuration} seconds`);
    addLogEvent(sessionId, 'COMPLETE', 
      `Email processing completed in ${totalDuration.toFixed(2)} seconds`);
    
    return {
      success: true,
      message: `Email ingest completed successfully in ${totalDuration.toFixed(2)} seconds`,
      processingId: processingId
    };
      
  } catch (error) {
    Logger.log(`Email ingest error: ${error.message}`);
    addLogEvent(sessionId, 'ERROR', `Email processing error: ${error.message}`);
    
    // Ensure progress is updated to 100% even on error
    if (getProcessProgress(processingId).progress < 100) {
      updateProcessProgress(processingId, 100, `Complete - Error: ${error.message}`);
    }
    
    return {
      success: false,
      message: error.message,
      processingId: processingId
    };
  }
}

/**
 * Processes an email attachment in batches to handle large files
 * @param {Attachment} attachment - The email attachment to process
 * @param {Sheet} destSheet - The destination sheet
 * @param {string} sessionId - The log session ID
 * @param {string} processingId - The processing ID for tracking progress
 * @return {Object} Processing results with row counts and status
 */
function processEmailAttachmentInBatches(attachment, destSheet, sessionId, processingId) {
  const BATCH_SIZE = 1000; // Process 1000 rows at a time
  
  try {
    // Get the CSV content as a string
    updateProcessProgress(processingId, 30, 'Reading attachment data...');
    addLogEvent(sessionId, 'PROCESSING', `Reading attachment data from ${attachment.getName()}`);
    
    const csvData = attachment.getDataAsString();
    
    // Split into lines
    updateProcessProgress(processingId, 35, 'Parsing CSV data...');
    addLogEvent(sessionId, 'PROCESSING', `Parsing CSV data`);
    
    const lines = csvData.split('\n');
    if (lines.length === 0) {
      throw new Error('No data in CSV file');
    }
    
    // Store the initial count of rows to be processed
    const initialRowCount = lines.length - 1; // Subtract 1 for header
    addLogEvent(sessionId, 'VALIDATION', `Initial row count: ${initialRowCount} rows to be processed`);
    
    // Parse headers
    const headers = Utilities.parseCsv(lines[0])[0];
    
    // Write headers
    updateProcessProgress(processingId, 40, 'Writing headers...');
    addLogEvent(sessionId, 'PROCESSING', `Writing headers to destination sheet`);
    
    destSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    
    // Process the data in batches
    let startRow = 1; // Skip header row
    let processedRows = 0;
    let totalRows = lines.length - 1;
    let batchNumber = 0;
    
    addLogEvent(sessionId, 'PROCESSING', `Processing ${totalRows} rows in batches of ${BATCH_SIZE}`);
    
    while (startRow < lines.length) {
      // Check if the process has been cancelled
      if (isProcessCancelled(processingId)) {
        addLogEvent(sessionId, 'CANCELLED', `Processing cancelled after ${processedRows} rows`);
        return {
          success: false,
          cancelled: true,
          processedRows: processedRows,
          totalRows: totalRows
        };
      }
      
      batchNumber++;
      
      // Determine the batch size
      const endRow = Math.min(startRow + BATCH_SIZE, lines.length);
      const batchLines = lines.slice(startRow, endRow);
      const batchSize = batchLines.length;
      
      // Update progress - scale between 40-95%
      const progressPercentage = 40 + Math.round((processedRows / totalRows) * 55);
      updateProcessProgress(processingId, progressPercentage, 
        `Processing batch ${batchNumber}/${Math.ceil(totalRows/BATCH_SIZE)}: rows ${processedRows+1}-${processedRows+batchSize} of ${totalRows} (${Math.round((processedRows/totalRows)*100)}% complete)`);
      
      addLogEvent(sessionId, 'BATCH_PROCESSING', 
        `Processing batch ${batchNumber}: ${batchSize} rows (${startRow}-${endRow-1})`);
      
      // Parse the batch
      const batchData = Utilities.parseCsv(batchLines.join('\n'));
      
      // Write the batch to the sheet
      if (batchData.length > 0) {
        destSheet.getRange(
          processedRows + 2, // +2 because we start after the header (row 1) and rows are 1-indexed
          1, 
          batchData.length, 
          headers.length
        ).setValues(batchData);
        
        processedRows += batchData.length;
      }
      
      // Log progress
      const progress = Math.round((processedRows / totalRows) * 100);
      addLogEvent(sessionId, 'PROGRESS', `Processed ${processedRows}/${totalRows} rows (${progress}%)`);
      
      // Move to the next batch
      startRow = endRow;
      
      // Add a small pause to allow for cancellation checks and to prevent timeout
      Utilities.sleep(50);
    }
    
    // Verify row count after processing
    const finalRowCount = destSheet.getLastRow() - 1; // Subtract 1 for header
    const rowCountMatch = (finalRowCount === initialRowCount);

    // Check if we're off by exactly 1 row (common edge case with CSV parsing)
    const isOffByOne = Math.abs(finalRowCount - initialRowCount) === 1;
    const rowCountValidationMessage = isOffByOne ? 
      `Minor difference in row count (${finalRowCount} vs ${initialRowCount}) - likely due to blank rows or CSV parsing` :
      `${rowCountMatch ? 'MATCH' : 'MISMATCH'}: Found ${finalRowCount} rows, expected ${initialRowCount}`;

    addLogEvent(sessionId, isOffByOne ? 'INFO' : (rowCountMatch ? 'VALIDATION' : 'WARNING'), rowCountValidationMessage);

    // Only show warning for significant mismatches
    if (!rowCountMatch && !isOffByOne) {
      addLogEvent(sessionId, 'WARNING', 
        `Row count mismatch! Expected ${initialRowCount} rows but found ${finalRowCount} rows in destination.`);
    }
    
    // Auto-resize columns for better display
    updateProcessProgress(processingId, 95, 'Finalizing import...');
    addLogEvent(sessionId, 'FINALIZING', `Auto-resizing columns for better display`);
    
    for (let i = 1; i <= headers.length; i++) {
      destSheet.autoResizeColumn(i);
    }
    
    return {
      success: true,
      processedRows: processedRows,
      totalRows: totalRows,
      batchCount: batchNumber,
      rowCountValidation: {
        initialCount: initialRowCount,
        finalCount: finalRowCount,
        match: rowCountMatch,
        isOffByOne: isOffByOne
      }
    };
  } catch (error) {
    addLogEvent(sessionId, 'ERROR', `Batch processing error: ${error.message}`);
    throw error;
  }
}

/**
 * Processes a Google Sheet ingest rule
 * @param {Object} rule - The rule configuration object
 * @param {string} sessionId - The log session ID
 */
function processGSheetRule(rule, sessionId) {
  const startTime = new Date();
  addLogEvent(sessionId, 'SHEET_ACCESS', `Accessing source sheet: ${rule.source.sheetUrl}`);
  
  try {
  // Get source spreadsheet
  const sourceSheetId = extractSheetId(rule.source.sheetUrl);
  const sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
  const sourceSheet = sourceSpreadsheet.getSheetByName(rule.source.tabName);
  
  if (!sourceSheet) {
    throw new Error(`Source sheet tab "${rule.source.tabName}" not found`);
  }
  
    // Get source data info
    const dataRange = sourceSheet.getDataRange();
    const numRows = dataRange.getNumRows();
    const numCols = dataRange.getNumColumns();
    
    addLogEvent(sessionId, 'SHEET_INFO', 
      `Source sheet has ${numRows} rows and ${numCols} columns`);
    
    // Validate source data
    if (numRows === 0 || numCols === 0) {
      throw new Error('Source sheet contains no data');
    }
    
    // Get current spreadsheet
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const handlingMode = rule.destination.handlingMode || 'clearAndReuse';
  
    // Process based on handling mode
    switch (handlingMode) {
      case 'copyFormat':
        processCopyFormatMode(sourceSheet, spreadsheet.getId(), rule.destination.tabName, sessionId);
        break;
        
      case 'recreate':
        processRecreateMode(sourceSheet, spreadsheet.getId(), rule.destination.tabName, sessionId);
        break;
        
      case 'append':
        processAppendMode(sourceSheet, spreadsheet.getId(), rule.destination.tabName, sessionId);
        break;
        
      case 'clearAndReuse':
      default:
        processClearAndReuseMode(sourceSheet, spreadsheet.getId(), rule.destination.tabName, sessionId);
    }
    
    const duration = (new Date() - startTime) / 1000;
    addLogEvent(sessionId, 'COMPLETE', 
      `Sheet processing completed in ${duration.toFixed(2)} seconds`);
      
  } catch (error) {
    addLogEvent(sessionId, 'ERROR', `Error processing sheet: ${error.message}`);
    throw error;
  }
}

/**
 * Processes sheet in copyFormat mode - preserves formatting
 */
function processCopyFormatMode(sourceSheet, destSheetId, tabName, sessionId) {
  addLogEvent(sessionId, 'SHEET_COPY', 'Starting copy with format preservation');
  
    const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
    
    // Copy sheet with formatting
    const tempSheet = sourceSheet.copyTo(destSpreadsheet);
    
    // Delete existing sheet if it exists
  const existingSheet = destSpreadsheet.getSheetByName(tabName);
    if (existingSheet && existingSheet.getSheetId() !== tempSheet.getSheetId()) {
      destSpreadsheet.deleteSheet(existingSheet);
    }
    
    // Rename copied sheet
  tempSheet.setName(tabName);
  
  // Get data range for logging
  const dataRange = tempSheet.getDataRange();
  addLogEvent(sessionId, 'SHEET_COPIED', 
    `Copied sheet with formatting: ${dataRange.getNumRows()} rows, ${dataRange.getNumColumns()} columns`);
}

/**
 * Processes sheet in recreate mode - deletes and recreates
 */
function processRecreateMode(sourceSheet, destSheetId, tabName, sessionId) {
  addLogEvent(sessionId, 'SHEET_RECREATE', 'Starting sheet recreation');
  
  const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
  
  // Delete existing sheet if it exists
  const existingSheet = destSpreadsheet.getSheetByName(tabName);
  if (existingSheet) {
    destSpreadsheet.deleteSheet(existingSheet);
  }
  
  // Create new sheet
  const newSheet = destSpreadsheet.insertSheet(tabName);
  
  // Copy data
    const sourceData = sourceSheet.getDataRange().getValues();
  if (sourceData.length > 0) {
    newSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
    
    // Auto-resize columns
    for (let i = 1; i <= sourceData[0].length; i++) {
      newSheet.autoResizeColumn(i);
    }
  }
  
  addLogEvent(sessionId, 'SHEET_RECREATED', 
    `Recreated sheet with ${sourceData.length} rows`);
}

/**
 * Processes sheet in append mode - adds to existing data
 */
function processAppendMode(sourceSheet, destSheetId, tabName, sessionId) {
  addLogEvent(sessionId, 'SHEET_APPEND', 'Starting data append');
  
  const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
  let destSheet = destSpreadsheet.getSheetByName(tabName);
  
  // Create sheet if it doesn't exist
  if (!destSheet) {
    destSheet = destSpreadsheet.insertSheet(tabName);
  }
  
  // Get source and destination data
  const sourceData = sourceSheet.getDataRange().getValues();
      const lastRow = destSheet.getLastRow();
  
  if (sourceData.length === 0) {
    addLogEvent(sessionId, 'WARNING', 'No data to append from source sheet');
    return;
  }
      
      if (lastRow === 0) {
        // If sheet is empty, just set all data
        destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
    addLogEvent(sessionId, 'DATA_WRITTEN', 
      `Wrote ${sourceData.length} rows to empty sheet`);
      } else {
        // Skip header row when appending
    const rowsToAppend = sourceData.length - 1;
    if (rowsToAppend > 0) {
      destSheet.getRange(lastRow + 1, 1, rowsToAppend, sourceData[0].length)
               .setValues(sourceData.slice(1));
      addLogEvent(sessionId, 'DATA_APPENDED', 
        `Appended ${rowsToAppend} rows to existing sheet (now ${lastRow + rowsToAppend} total rows)`);
    } else {
      addLogEvent(sessionId, 'WARNING', 'No new data rows to append');
    }
  }
  
  // Auto-resize columns
  for (let i = 1; i <= sourceData[0].length; i++) {
    destSheet.autoResizeColumn(i);
  }
}

/**
 * Processes sheet in clearAndReuse mode - clears existing data
 */
function processClearAndReuseMode(sourceSheet, destSheetId, tabName, sessionId) {
  addLogEvent(sessionId, 'SHEET_CLEAR', 'Starting clear and reuse');
  
  const destSpreadsheet = SpreadsheetApp.openById(destSheetId);
  let destSheet = destSpreadsheet.getSheetByName(tabName);
  
  // Create sheet if it doesn't exist
  if (!destSheet) {
    destSheet = destSpreadsheet.insertSheet(tabName);
    } else {
    // Clear existing data
    destSheet.clear();
  }
  
  // Copy data
  const sourceData = sourceSheet.getDataRange().getValues();
  if (sourceData.length > 0) {
      destSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
    
    // Auto-resize columns
    for (let i = 1; i <= sourceData[0].length; i++) {
      destSheet.autoResizeColumn(i);
    }
    
    addLogEvent(sessionId, 'DATA_WRITTEN', 
      `Wrote ${sourceData.length} rows to cleared sheet`);
  } else {
    addLogEvent(sessionId, 'WARNING', 'No data to write from source sheet');
  }
}

/**
 * Extracts sheet ID from a Google Sheets URL
 * @param {string} url - Google Sheets URL or ID
 * @return {string} The extracted sheet ID
 */
function extractSheetId(url) {
  // Try different URL formats
  
  // Format 1: Standard Google Sheets URL
  let match = url.match(/\/d\/([^\/]+)/);
  if (match && match[1]) {
    return match[1];
  }
  
  // Format 2: Just the ID
  if (/^[a-zA-Z0-9_-]+$/.test(url)) {
    return url;
  }
  
  throw new Error(`Could not extract sheet ID from URL: ${url}`);
}

/**
 * Opens or creates a destination sheet based on handling mode
 * @param {string} sheetId - The spreadsheet ID
 * @param {string} tabName - The tab name
 * @param {string} handlingMode - The handling mode (clearAndReuse, recreate, append)
 * @return {Sheet} The destination sheet
 */
function openOrCreateDestinationSheet(sheetId, tabName, handlingMode) {
  const spreadsheet = SpreadsheetApp.openById(sheetId);
  let sheet = spreadsheet.getSheetByName(tabName);
  
  if (!sheet) {
    // Create new sheet if it doesn't exist
    sheet = spreadsheet.insertSheet(tabName);
  } else {
    // Handle existing sheet based on mode
    switch (handlingMode) {
      case 'clearAndReuse':
        sheet.clear();
        break;
      case 'recreate':
        spreadsheet.deleteSheet(sheet);
        sheet = spreadsheet.insertSheet(tabName);
        break;
      case 'append':
        // Do nothing - data will be appended
        break;
    }
  }
  
  return sheet;
}

/**
 * Rule Execution Test Module
 * 
 * This module contains functions to test the execution of rules.
 */

/**
 * Executes a test rule to verify rule processing
 * @param {string} ruleId - ID of the rule to execute
 * @return {Object} Execution result
 */
function testExecuteRule(ruleId) {
  try {
    // Create log session
    const sessionId = createLogSession(`Testing rule execution: ${ruleId}`);
    
    // Get the rule
    const config = getConfig();
    const rule = config.rules.find(r => r.id === ruleId);
    
    if (!rule) {
      addLogEvent(sessionId, "ERROR", `Rule not found: ${ruleId}`);
      return {
        success: false,
        message: "Rule not found"
      };
    }
    
    // Log the start of execution
    addLogEvent(sessionId, "INFO", `Starting test execution of rule: ${rule.description}`);
    
    // Update rule status to processing
    rule.status = {
      lastRun: new Date().toISOString(),
      result: "PROCESSING",
      message: "Execution in progress"
    };
    
    // Save the status update
    updateRuleInConfig(rule);
    
    // Execute based on method
    let result;
    if (rule.method === 'email') {
      result = testEmailRule(rule, sessionId);
    } else if (rule.method === 'gSheet') {
      result = testGSheetRule(rule, sessionId);
    } else {
      throw new Error(`Unsupported method: ${rule.method}`);
    }
    
    // Update rule status based on result
    rule.status = {
      lastRun: new Date().toISOString(),
      result: result.success ? "SUCCESS" : "ERROR",
      message: result.message
    };
    
    // Save the status update
    updateRuleInConfig(rule);
    
    // Log completion
    addLogEvent(sessionId, result.success ? "SUCCESS" : "ERROR", result.message);
    
    return result;
  } catch (error) {
    // Log error
    if (sessionId) {
      addLogEvent(sessionId, "ERROR", `Test execution failed: ${error.message}`);
    }
    
    // Update rule status if we have a rule
    if (rule) {
      rule.status = {
        lastRun: new Date().toISOString(),
        result: "ERROR",
        message: error.message
      };
      
      // Save the status update
      updateRuleInConfig(rule);
    }
    
    return {
      success: false,
      message: "Test execution failed: " + error.message
    };
  }
}

/**
 * Tests an email rule
 * @param {Object} rule - The rule to test
 * @param {string} sessionId - The log session ID
 * @return {Object} Test result
 */
function testEmailRule(rule, sessionId) {
  addLogEvent(sessionId, "EMAIL_TEST", `Email search: ${rule.source.emailSearch}`);
  addLogEvent(sessionId, "EMAIL_TEST", `Attachment pattern: ${rule.source.attachmentPattern}`);
  
  // Verify email settings
  if (!rule.source.emailSearch) {
    return {
      success: false,
      message: "Email search query is required"
    };
  }
  
  if (!rule.source.attachmentPattern) {
    return {
      success: false,
      message: "Attachment pattern is required"
    };
  }
  
  // Test Gmail search - only check if we get any threads, don't actually process
  try {
    const threads = GmailApp.search(rule.source.emailSearch, 0, 1);
    addLogEvent(sessionId, "EMAIL_TEST", `Gmail search returned ${threads.length} threads`);
    
    return {
      success: true,
      message: `Email rule test successful - found ${threads.length} matching threads`,
      threadsFound: threads.length
    };
  } catch (error) {
    return {
      success: false,
      message: `Email search error: ${error.message}`
    };
  }
}

/**
 * Tests a Google Sheet rule
 * @param {Object} rule - The rule to test
 * @param {string} sessionId - The log session ID
 * @return {Object} Test result
 */
function testGSheetRule(rule, sessionId) {
  addLogEvent(sessionId, "SHEET_TEST", `Source sheet: ${rule.source.sheetUrl}`);
  addLogEvent(sessionId, "SHEET_TEST", `Source tab: ${rule.source.tabName}`);
  
  // Verify Google Sheet settings
  if (!rule.source.sheetUrl) {
    return {
      success: false,
      message: "Source sheet URL is required"
    };
  }
  
  if (!rule.source.tabName) {
    return {
      success: false,
      message: "Source tab name is required"
    };
  }
  
  // Test source sheet access
  try {
    const sourceSheetId = extractSheetId(rule.source.sheetUrl);
    const sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
    const sourceSheet = sourceSpreadsheet.getSheetByName(rule.source.tabName);
    
    if (!sourceSheet) {
      return {
        success: false,
        message: `Source tab '${rule.source.tabName}' not found`
      };
    }
    
    // Get source data info
    const dataRange = sourceSheet.getDataRange();
    const numRows = dataRange.getNumRows();
    const numCols = dataRange.getNumColumns();
    
    addLogEvent(sessionId, "SHEET_TEST", `Source data has ${numRows} rows and ${numCols} columns`);
    
    return {
      success: true,
      message: `Google Sheet rule test successful - source data has ${numRows} rows and ${numCols} columns`,
      sourceRows: numRows,
      sourceCols: numCols
    };
  } catch (error) {
    return {
      success: false,
      message: `Google Sheet access error: ${error.message}`
    };
  }
}

/**
 * Updates a rule in the configuration
 * @param {Object} rule - The rule to update
 */
function updateRuleInConfig(rule) {
  const config = getConfig();
  const index = config.rules.findIndex(r => r.id === rule.id);
  
  if (index >= 0) {
    config.rules[index] = rule;
    saveConfig(config);
  }
}

/**
 * Test Module for Data Ingest
 * 
 * This module contains functions to test the data ingest functionality
 * independently of the UI.
 */

/**
 * Tests email ingestion with specified parameters
 * @param {Object} params - Test parameters
 * @param {string} params.emailSearch - Gmail search query
 * @param {string} params.attachmentPattern - Regular expression pattern for attachments
 * @param {string} params.destinationTab - Name of destination tab
 * @param {string} [params.handlingMode='clearAndReuse'] - How to handle existing data
 * @return {Object} Test results
 */
function testEmailIngest(params) {
  console.log('=== Starting Email Ingest Test ===');
  console.log('Test Parameters:', JSON.stringify(params, null, 2));
  
  const sessionId = createLogSession('Testing email ingest');
  console.log('Created log session:', sessionId);
  
  try {
    console.log('Creating test rule...');
    // Create a test rule
    const testRule = {
      id: 'test-email-' + new Date().getTime(),
      active: true,
      description: 'Test Email Ingest',
      method: 'email',
      source: {
        emailSearch: params.emailSearch,
        attachmentPattern: params.attachmentPattern
      },
      destination: {
        tabName: params.destinationTab,
        handlingMode: params.handlingMode || 'clearAndReuse'
      }
    };
    console.log('Test rule created:', JSON.stringify(testRule, null, 2));
    
    console.log('Starting email rule processing...');
    // Process the rule
    processEmailRule(testRule, sessionId);
    console.log('Email rule processing completed');
    
    const result = {
      success: true,
      message: 'Email ingest test completed successfully',
      sessionId: sessionId
    };
    console.log('Test completed successfully:', JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    console.error('Test failed with error:', error);
    console.error('Error stack:', error.stack);
    addLogEvent(sessionId, 'ERROR', `Test failed: ${error.message}`);
    
    const result = {
      success: false,
      message: `Email ingest test failed: ${error.message}`,
      sessionId: sessionId,
      error: error.toString()
    };
    console.log('Test failed:', JSON.stringify(result, null, 2));
    return result;
  }
}

/**
 * Tests Google Sheet ingestion with specified parameters
 * @param {Object} params - Test parameters
 * @param {string} params.sourceSheetUrl - URL of source spreadsheet
 * @param {string} params.sourceTabName - Name of source tab
 * @param {string} params.destinationTab - Name of destination tab
 * @param {string} [params.handlingMode='clearAndReuse'] - How to handle existing data
 * @return {Object} Test results
 */
function testSheetIngest(params) {
  console.log('=== Starting Sheet Ingest Test ===');
  console.log('Test Parameters:', JSON.stringify(params, null, 2));
  
  const sessionId = createLogSession('Testing sheet ingest');
  console.log('Created log session:', sessionId);
  
  try {
    console.log('Creating test rule...');
    // Create a test rule
    const testRule = {
      id: 'test-sheet-' + new Date().getTime(),
      active: true,
      description: 'Test Sheet Ingest',
      method: 'gSheet',
      source: {
        sheetUrl: params.sourceSheetUrl,
        tabName: params.sourceTabName
      },
      destination: {
        tabName: params.destinationTab,
        handlingMode: params.handlingMode || 'clearAndReuse'
      }
    };
    console.log('Test rule created:', JSON.stringify(testRule, null, 2));
    
    console.log('Extracting source sheet ID...');
    const sourceSheetId = extractSheetId(params.sourceSheetUrl);
    console.log('Source sheet ID:', sourceSheetId);
    
    console.log('Attempting to open source spreadsheet...');
    const sourceSpreadsheet = SpreadsheetApp.openById(sourceSheetId);
    console.log('Source spreadsheet opened successfully');
    
    console.log('Checking source tab...');
    const sourceSheet = sourceSpreadsheet.getSheetByName(params.sourceTabName);
    if (!sourceSheet) {
      throw new Error(`Source tab "${params.sourceTabName}" not found`);
    }
    console.log('Source tab found');
    
    console.log('Getting source data info...');
    const dataRange = sourceSheet.getDataRange();
    const numRows = dataRange.getNumRows();
    const numCols = dataRange.getNumColumns();
    console.log(`Source data dimensions: ${numRows} rows x ${numCols} columns`);
    
    console.log('Starting sheet rule processing...');
    // Process the rule
    processGSheetRule(testRule, sessionId);
    console.log('Sheet rule processing completed');
    
    const result = {
      success: true,
      message: 'Sheet ingest test completed successfully',
      sessionId: sessionId,
      sourceInfo: {
        rows: numRows,
        columns: numCols
      }
    };
    console.log('Test completed successfully:', JSON.stringify(result, null, 2));
    return result;
  } catch (error) {
    console.error('Test failed with error:', error);
    console.error('Error stack:', error.stack);
    addLogEvent(sessionId, 'ERROR', `Test failed: ${error.message}`);
    
    const result = {
      success: false,
      message: `Sheet ingest test failed: ${error.message}`,
      sessionId: sessionId,
      error: error.toString()
    };
    console.log('Test failed:', JSON.stringify(result, null, 2));
    return result;
  }
}

/**
 * Runs a complete test of both ingest methods
 * @param {Object} params - Test parameters for both methods
 * @return {Object} Combined test results
 */
function runIngestTests(params) {
  console.log('=== Starting Combined Ingest Tests ===');
  console.log('Test Parameters:', JSON.stringify(params, null, 2));
  
  const results = {
    email: null,
    sheet: null,
    timestamp: new Date().toISOString()
  };
  
  // Test email ingest if parameters are provided
  if (params.email) {
    console.log('Starting email test...');
    results.email = testEmailIngest(params.email);
    console.log('Email test completed:', JSON.stringify(results.email, null, 2));
  }
  
  // Test sheet ingest if parameters are provided
  if (params.sheet) {
    console.log('Starting sheet test...');
    results.sheet = testSheetIngest(params.sheet);
    console.log('Sheet test completed:', JSON.stringify(results.sheet, null, 2));
  }
  
  console.log('All tests completed:', JSON.stringify(results, null, 2));
  return results;
}

/**
 * Example usage of the test functions
 */
function testIngestExamples() {
  console.log('=== Starting Test Examples ===');
  
  // Example 1: Test email ingest
  console.log('\n--- Example 1: Email Ingest Test ---');
  const emailTest = testEmailIngest({
    emailSearch: 'subject:(Monthly Report) from:example.com',
    attachmentPattern: 'Monthly_Report_.*\\.csv',
    destinationTab: 'EmailTest',
    handlingMode: 'clearAndReuse'
  });
  
  console.log('Email Test Results:', JSON.stringify(emailTest, null, 2));
  
  // Example 2: Test sheet ingest
  console.log('\n--- Example 2: Sheet Ingest Test ---');
  const sheetTest = testSheetIngest({
    sourceSheetUrl: 'https://docs.google.com/spreadsheets/d/1234567890/edit',
    sourceTabName: 'SourceData',
    destinationTab: 'SheetTest',
    handlingMode: 'clearAndReuse'
  });
  
  console.log('Sheet Test Results:', JSON.stringify(sheetTest, null, 2));
  
  // Example 3: Test both methods together
  console.log('\n--- Example 3: Combined Tests ---');
  const combinedTest = runIngestTests({
    email: {
      emailSearch: 'subject:(Monthly Report) from:example.com',
      attachmentPattern: 'Monthly_Report_.*\\.csv',
      destinationTab: 'CombinedEmailTest'
    },
    sheet: {
      sourceSheetUrl: 'https://docs.google.com/spreadsheets/d/1234567890/edit',
      sourceTabName: 'SourceData',
      destinationTab: 'CombinedSheetTest'
    }
  });
  
  console.log('Combined Test Results:', JSON.stringify(combinedTest, null, 2));
  console.log('=== Test Examples Completed ===');
}

// =================================================================
// Process tracking functions for progress updates and cancellation
// =================================================================

/**
 * Initialize tracking for a processing job
 * @param {string} processingId - The unique ID for this processing job
 * @param {string} processType - The type of process ('email', 'gSheet', etc.)
 */
function initializeProcessTracking(processingId, processType) {
  const scriptProperties = PropertiesService.getScriptProperties();
  const trackingData = {
    id: processingId,
    type: processType,
    startTime: new Date().toISOString(),
    progress: 0,
    status: 'Starting...',
    cancelled: false
  };
  
  scriptProperties.setProperty('process_' + processingId, JSON.stringify(trackingData));
  
  // Set an expiration trigger to clean up tracking data after 30 minutes
  ScriptApp.newTrigger('cleanupProcessTracking')
    .timeBased()
    .after(30 * 60 * 1000) // 30 minutes
    .create();
}

/**
 * Update the progress of a processing job
 * @param {string} processingId - The unique ID for this processing job
 * @param {number} progress - Progress percentage (0-100)
 * @param {string} status - Status message
 */
function updateProcessProgress(processingId, progress, status) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const trackingDataStr = scriptProperties.getProperty('process_' + processingId);
    
    if (!trackingDataStr) return;
    
    const trackingData = JSON.parse(trackingDataStr);
    trackingData.progress = progress;
    
    // Add elapsed time to status message
    const startTime = new Date(trackingData.startTime);
    const now = new Date();
    const elapsedSeconds = Math.round((now - startTime)/1000);
    const elapsedFormatted = 
      elapsedSeconds > 60 ? 
      `${Math.floor(elapsedSeconds/60)}m ${elapsedSeconds%60}s` : 
      `${elapsedSeconds}s`;
    
    trackingData.status = `${status} (Elapsed: ${elapsedFormatted})`;
    trackingData.lastUpdate = now.toISOString();
    
    // Log the update for debugging
    Logger.log(`Updating process ${processingId}: ${progress}% - ${status}`);
    
    scriptProperties.setProperty('process_' + processingId, JSON.stringify(trackingData));
  } catch (error) {
    Logger.log(`Error updating process progress: ${error.message}`);
  }
}

/**
 * Get the current progress of a processing job
 * @param {string} processingId - The unique ID for this processing job
 * @return {Object} The progress information
 */
function getProcessProgress(processingId) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const trackingDataStr = scriptProperties.getProperty('process_' + processingId);
    
    if (!trackingDataStr) {
      return { 
        progress: 100, 
        status: 'Complete or not found', 
        found: false 
      };
    }
    
    return JSON.parse(trackingDataStr);
  } catch (error) {
    Logger.log(`Error getting process progress: ${error.message}`);
    return { 
      progress: 0, 
      status: 'Error: ' + error.message, 
      error: true 
    };
  }
}

/**
 * Check if a process has been cancelled by the user
 * @param {string} processingId - The unique ID for this processing job
 * @return {boolean} True if the process was cancelled
 */
function isProcessCancelled(processingId) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const trackingDataStr = scriptProperties.getProperty('process_' + processingId);
    
    if (!trackingDataStr) return false;
    
    const trackingData = JSON.parse(trackingDataStr);
    return trackingData.cancelled === true;
  } catch (error) {
    Logger.log(`Error checking if process is cancelled: ${error.message}`);
    return false;
  }
}

/**
 * Cancel a processing job
 * @param {string} processingId - The unique ID for this processing job
 * @return {Object} Result with success flag and message
 */
function cancelProcessing(processingId) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const trackingDataStr = scriptProperties.getProperty('process_' + processingId);
    
    if (!trackingDataStr) {
      return {
        success: false,
        message: 'Process not found or already completed'
      };
    }
    
    const trackingData = JSON.parse(trackingDataStr);
    trackingData.cancelled = true;
    trackingData.status = 'Cancelling...';
    
    scriptProperties.setProperty('process_' + processingId, JSON.stringify(trackingData));
    
    return {
      success: true,
      message: 'Process cancellation requested'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error cancelling process: ' + error.message
    };
  }
}

/**
 * Clean up old process tracking data
 */
function cleanupProcessTracking() {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const allProps = scriptProperties.getProperties();
    
    for (const key in allProps) {
      if (key.startsWith('process_')) {
        try {
          const trackingData = JSON.parse(allProps[key]);
          const startTime = new Date(trackingData.startTime);
          const now = new Date();
          
          // If process data is older than 30 minutes, delete it
          if ((now - startTime) > 30 * 60 * 1000) {
            scriptProperties.deleteProperty(key);
          }
        } catch (e) {
          // If there's an error parsing, just delete the property
          scriptProperties.deleteProperty(key);
        }
      }
    }
  } catch (error) {
    Logger.log(`Error cleaning up process tracking data: ${error.message}`);
  }
}

/**
 * Delete a specific process tracking record
 * @param {string} processingId - The unique ID for the processing job to delete
 * @return {Object} Result with success flag and message
 */
function deleteProcessTracking(processingId) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const trackingDataStr = scriptProperties.getProperty('process_' + processingId);
    
    if (!trackingDataStr) {
      return {
        success: false,
        message: 'Process not found or already deleted'
      };
    }
    
    // Delete the property
    scriptProperties.deleteProperty('process_' + processingId);
    
    return {
      success: true,
      message: 'Process tracking data deleted successfully'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error deleting process tracking data: ' + error.message
    };
  }
}

/**
 * Debug function to list all active processes
 * @return {Object} Object containing all active processes
 */
function debugProcessTracking() {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const allProps = scriptProperties.getProperties();
    const activeProcesses = {};
    
    for (const key in allProps) {
      if (key.startsWith('process_')) {
        try {
          const processId = key.substring(8); // Remove 'process_' prefix
          const data = JSON.parse(allProps[key]);
          
          // Add additional info about how long ago this was updated
          if (data.lastUpdate) {
            const lastUpdate = new Date(data.lastUpdate);
            const now = new Date();
            data._secondsSinceUpdate = Math.round((now - lastUpdate)/1000);
          }
          
          activeProcesses[processId] = data;
        } catch (e) {
          activeProcesses[key] = {error: e.message, data: allProps[key]};
        }
      }
    }
    
    return activeProcesses;
  } catch (error) {
    Logger.log(`Error in debugProcessTracking: ${error.message}`);
    return {error: error.message};
  }
}