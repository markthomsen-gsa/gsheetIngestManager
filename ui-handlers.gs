/**
 * Data Ingest Manager - UI Handlers
 * 
 * Server-side handlers for UI interactions with the sidebar.
 */

/**
 * Shows the Data Ingest Manager sidebar
 */
function showSidebar() {
  // Create HTML output from the Sidebar.html file
  const html = HtmlService.createTemplateFromFile('Sidebar')
    .evaluate()
    .setTitle('Data Ingest Manager')
    .setWidth(300);
  
  // Display the sidebar
  SpreadsheetApp.getUi().showSidebar(html);
}

/**
 * Includes an HTML or JavaScript file in the HTML template
 * @param {string} filename - The name of the file to include
 * @return {string} The content of the file
 */
function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename)
      .getContent();
}

/**
 * Gets all data needed for sidebar initialization
 * @return {Object} Configuration, logs and user info
 */
function getSidebarData() {
  return {
    config: getConfig(),
    logs: getLogs().slice(0, 10), // Get latest 10 sessions
    currentUser: Session.getActiveUser().getEmail(),
    spreadsheetUrl: SpreadsheetApp.getActiveSpreadsheet().getUrl(),
    spreadsheetName: SpreadsheetApp.getActiveSpreadsheet().getName(),
    availableTabs: getAvailableTabs()
  };
}

/**
 * Gets all available tabs in the current spreadsheet
 * @return {Array} List of tab names
 */
function getAvailableTabs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  return sheets.map(sheet => sheet.getName());
}

/**
 * Validates a rule configuration
 * @param {Object} rule - The rule to validate
 * @return {Object} Validation result with valid flag and message
 */
function validateRule(rule) {
  try {
    // Basic validation
    if (!rule.description) {
      return { valid: false, message: 'Description is required' };
    }
    
    if (!rule.method) {
      return { valid: false, message: 'Ingest method is required' };
    }
    
    // Method-specific validation
    if (rule.method === 'email') {
      if (!rule.source || !rule.source.emailSearch) {
        return { valid: false, message: 'Email search query is required' };
      }
      if (!rule.source || !rule.source.attachmentPattern) {
        return { valid: false, message: 'Attachment pattern is required' };
      }
    } else if (rule.method === 'gSheet') {
      if (!rule.source || !rule.source.sheetUrl) {
        return { valid: false, message: 'Source sheet URL is required' };
      }
      if (!rule.source || !rule.source.tabName) {
        return { valid: false, message: 'Source tab name is required' };
      }
    }
    
    // Destination validation - only check tab name since we're using current spreadsheet
    if (!rule.destination || !rule.destination.tabName) {
      return { valid: false, message: 'Destination tab name is required' };
    }
    
    return { valid: true, message: 'Rule configuration is valid' };
  } catch (error) {
    return { 
      valid: false, 
      message: 'Validation error: ' + error.message 
    };
  }
}

/**
 * Saves a rule
 * @param {Object} rule - The rule to save
 * @return {Object} Result with success flag and message
 */
function saveRule(rule) {
  try {
    // Validate the rule
    const validation = validateRule(rule);
    if (!validation.valid) {
      return {
        success: false,
        message: validation.message
      };
    }
    
    // Get current configuration
    const config = getConfig();
    
    // Ensure destination object exists
    rule.destination = rule.destination || {};
    
    // Find existing rule or add new one
    const existingIndex = config.rules.findIndex(r => r.id === rule.id);
    if (existingIndex >= 0) {
      // Preserve status if it exists
      if (config.rules[existingIndex].status) {
        rule.status = config.rules[existingIndex].status;
      }
      config.rules[existingIndex] = rule;
    } else {
      // Initialize status for new rules
      rule.status = {
        lastRun: null,
        result: 'NEW',
        message: 'Rule has not been run yet'
      };
      config.rules.push(rule);
    }
    
    // Save updated configuration
    saveConfig(config);
    
    return {
      success: true,
      message: 'Rule saved successfully',
      rule: rule
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error saving rule: ' + error.message
    };
  }
}

/**
 * Deletes a rule
 * @param {string} ruleId - ID of the rule to delete
 * @return {Object} Result with success flag and message
 */
function deleteRule(ruleId) {
  try {
    // Get current configuration
    const config = getConfig();
    
    // Find and remove the rule
    const initialCount = config.rules.length;
    config.rules = config.rules.filter(rule => rule.id !== ruleId);
    
    // Check if rule was found and removed
    if (config.rules.length === initialCount) {
      return {
        success: false,
        message: 'Rule not found'
      };
    }
    
    // Save updated configuration
    saveConfig(config);
    
    return {
      success: true,
      message: 'Rule deleted successfully'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error deleting rule: ' + error.message
    };
  }
}

/**
 * Gets a specific rule
 * @param {string} ruleId - ID of the rule to get
 * @return {Object} The rule object or null if not found
 */
function getRule(ruleId) {
  const config = getConfig();
  return config.rules.find(rule => rule.id === ruleId) || null;
}

/**
 * Gets all rules
 * @return {Array} Array of all rules
 */
function getAllRules() {
  const config = getConfig();
  return config.rules || [];
}

/**
 * Runs selected rules
 * @param {Array} ruleIds - Array of rule IDs to run
 * @return {Object} Result with success flag and message
 */
function runSelectedRules(ruleIds) {
  try {
    // Check if there are any rules to run
    if (!ruleIds || ruleIds.length === 0) {
      return {
        success: false,
        message: 'No rules selected'
      };
    }
    
    // Create a log session
    const sessionId = createLogSession(`Running ${ruleIds.length} selected rule(s)`);
    console.log(`Created log session: ${sessionId} for ${ruleIds.length} rule(s)`);
    
    // Get all rules
    const config = getConfig();
    
    // Filter rules by IDs
    const rulesToRun = config.rules.filter(rule => ruleIds.includes(rule.id) && rule.active !== false);
    
    if (rulesToRun.length === 0) {
      addLogEvent(sessionId, 'WARNING', 'No active rules found to run');
      return {
        success: false,
        message: 'No active rules found to run'
      };
    }
    
    addLogEvent(sessionId, 'INFO', `Found ${rulesToRun.length} active rules to run`);
    console.log(`Found ${rulesToRun.length} active rules to run`);
    
    // Run each rule
    let successCount = 0;
    let errorCount = 0;
    
    for (const rule of rulesToRun) {
      try {
        addLogEvent(sessionId, 'PROCESSING', `Processing rule: ${rule.description}`);
        console.log(`Processing rule: ${rule.description} (${rule.id})`);
        
        // CHANGED: Actually process the rule instead of just testing it
        if (rule.method === 'email') {
          console.log('Processing email rule...');
          processEmailRule(rule, sessionId);  // This fully processes the email including attachments
        } else if (rule.method === 'gSheet') {
          console.log('Processing sheet rule...');
          processGSheetRule(rule, sessionId);  // This fully processes the Google Sheet
        } else {
          throw new Error(`Unsupported method: ${rule.method}`);
        }
        
        // Update rule status
        rule.status = {
          lastRun: new Date().toISOString(),
          result: 'SUCCESS',
          message: 'Processed successfully'
        };
        
        // Save the updated status
        updateRuleInConfig(rule);
        
        successCount++;
        addLogEvent(sessionId, 'SUCCESS', `Rule executed successfully: ${rule.description}`);
        console.log(`Rule executed successfully: ${rule.description}`);
      } catch (error) {
        errorCount++;
        console.error(`Error processing rule: ${error.message}`);
        console.error(`Error stack: ${error.stack}`);
        
        // Update rule status to reflect the error
        rule.status = {
          lastRun: new Date().toISOString(),
          result: 'ERROR',
          message: error.message
        };
        
        // Save the updated status
        updateRuleInConfig(rule);
        
        addLogEvent(sessionId, 'ERROR', `Error processing rule ${rule.id}: ${error.message}`);
      }
    }
    
    // Log completion
    const summary = `Completed: ${successCount} successful, ${errorCount} failed`;
    addLogEvent(sessionId, 'COMPLETE', summary);
    console.log(summary);
    
    return {
      success: true,
      message: summary,
      successCount: successCount,
      errorCount: errorCount
    };
  } catch (error) {
    console.error('Error running rules:', error);
    return {
      success: false,
      message: 'Error running rules: ' + error.message
    };
  }
}

/**
 * Runs all active rules
 * @return {Object} Result with success flag and message
 */
function runAllRules() {
  try {
    // Get all rules
    const config = getConfig();
    
    // Filter active rules
    const activeRuleIds = config.rules
      .filter(rule => rule.active !== false)
      .map(rule => rule.id);
    
    if (activeRuleIds.length === 0) {
      return {
        success: false,
        message: 'No active rules found'
      };
    }
    
    // Run all active rules
    return runSelectedRules(activeRuleIds);
  } catch (error) {
    return {
      success: false,
      message: 'Error running rules: ' + error.message
    };
  }
}

/**
 * Clears all logs
 * @return {Object} Result with success flag and message
 */
function clearAllLogs() {
  try {
    // Clear logs
    saveLogs([]);
    
    return {
      success: true,
      message: 'All logs cleared'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error clearing logs: ' + error.message
    };
  }
}

/**
 * Saves schedule settings
 * @param {Object} schedule - The schedule settings
 * @return {Object} Result with success flag and message
 */
function saveSchedule(schedule) {
  try {
    // Get current configuration
    const config = getConfig();
    
    // Initialize preferences if needed
    if (!config.preferences) {
      config.preferences = {};
    }
    
    // Update schedule settings
    config.preferences.schedule = schedule;
    
    // Save updated configuration
    saveConfig(config);
    
    return {
      success: true,
      message: 'Schedule settings saved successfully'
    };
  } catch (error) {
    return {
      success: false,
      message: 'Error saving schedule: ' + error.message
    };
  }
}

/**
 * Exports configuration to a sheet
 * @return {Object} Result with success flag and message
 */
function exportConfigToSheet() {
  try {
    const config = getConfig();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Create or get export sheet
    let sheet = ss.getSheetByName('Config Export');
    if (!sheet) {
      sheet = ss.insertSheet('Config Export');
    } else {
      sheet.clear();
    }
    
    // Format configuration as rows for display
    const rows = [['ID', 'Description', 'Method', 'Active', 'Source', 'Destination', 'Last Run', 'Status']];
    
    config.rules.forEach(rule => {
      rows.push([
        rule.id,
        rule.description,
        rule.method,
        rule.active ? 'Yes' : 'No',
        JSON.stringify(rule.source),
        JSON.stringify(rule.destination),
        rule.status?.lastRun ? new Date(rule.status.lastRun).toLocaleString() : 'Never',
        rule.status?.result || 'NEW'
      ]);
    });
    
    // Write to sheet
    sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(1, 1, 1, rows[0].length).setFontWeight('bold');
    
    // Resize columns
    for (let i = 1; i <= rows[0].length; i++) {
      sheet.autoResizeColumn(i);
    }
    
    return { 
      success: true, 
      message: 'Configuration exported to sheet', 
      sheetId: sheet.getSheetId() 
    };
  } catch (error) {
    return { 
      success: false, 
      message: 'Error exporting configuration: ' + error.message 
    };
  }
}

/**
 * Exports logs to a sheet
 * @return {Object} Result with success flag and message
 */
function exportLogsToSheet() {
  try {
    const logs = getLogs();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Create or get export sheet
    let sheet = ss.getSheetByName('Logs Export');
    if (!sheet) {
      sheet = ss.insertSheet('Logs Export');
    } else {
      sheet.clear();
    }
    
    // Format logs as rows for display
    const rows = [['Session ID', 'Timestamp', 'Event Type', 'Message']];
    
    logs.forEach(session => {
      // Add session header
      rows.push([
        session.sessionId,
        new Date(session.timestamp).toLocaleString(),
        'SESSION_START',
        'Session started'
      ]);
      
      // Add events
      session.events.forEach(event => {
        rows.push([
          '',
          new Date(event.timestamp).toLocaleString(),
          event.type,
          event.message
        ]);
      });
      
      // Add separator
      rows.push(['', '', '', '']);
    });
    
    // Write to sheet
    sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
    sheet.getRange(1, 1, 1, rows[0].length).setFontWeight('bold');
    
    // Resize columns
    for (let i = 1; i <= rows[0].length; i++) {
      sheet.autoResizeColumn(i);
    }
    
    return { 
      success: true, 
      message: 'Logs exported to sheet', 
      sheetId: sheet.getSheetId() 
    };
  } catch (error) {
    return { 
      success: false, 
      message: 'Error exporting logs: ' + error.message 
    };
  }
}