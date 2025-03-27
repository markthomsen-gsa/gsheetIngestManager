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
  // Get logs from the LogHandler
  let logs = [];
  try {
    logs = getLogSessionSummaries();
    console.log("Retrieved " + logs.length + " log sessions");
  } catch (e) {
    console.error("Error getting log summaries: " + e);
    logs = [];
  }

  return {
    config: getConfig(),
    logs: logs.slice(0, 20), // Get latest 20 sessions
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
 * Gets the progress of a processing job
 * @param {string} processingId - The ID of the processing job
 * @return {Object} The progress information
 */
function getProcessingProgress(processingId) {
  return getProcessProgress(processingId);
}

/**
 * Cancels an in-progress processing job
 * @param {string} processingId - The ID of the processing job to cancel
 * @return {Object} Result with success flag and message
 */
function cancelProcessingJob(processingId) {
  if (!processingId) {
    return {
      success: false,
      message: 'No processing ID provided'
    };
  }
  
  try {
    const result = cancelProcessing(processingId);
    return result;
  } catch (error) {
    return {
      success: false,
      message: 'Error cancelling process: ' + error.message
    };
  }
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
    
    // For single rule execution, process it immediately and return the processing ID
    if (rulesToRun.length === 1) {
      console.log(`Processing single rule: ${rulesToRun[0].description} (${rulesToRun[0].id})`);
      
      try {
        // Process the rule based on method
        let result;
        
        if (rulesToRun[0].method === 'email') {
          result = processEmailRule(rulesToRun[0], sessionId);
        } else if (rulesToRun[0].method === 'gSheet') {
          result = processGSheetRule(rulesToRun[0], sessionId);
        } else {
          throw new Error(`Unsupported method: ${rulesToRun[0].method}`);
        }
        
        // Update rule status
        rulesToRun[0].status = {
          lastRun: new Date().toISOString(),
          result: result.success ? 'SUCCESS' : (result.cancelled ? 'CANCELLED' : 'ERROR'),
          message: result.message
        };
        
        // Save the updated status
        updateRuleInConfig(rulesToRun[0]);
        
        // Return result with processing ID for progress tracking
        return {
          success: result.success,
          message: result.message,
          processingId: result.processingId,
          ruleId: rulesToRun[0].id
        };
      } catch (error) {
        console.error(`Error processing rule: ${error.message}`);
        console.error(`Error stack: ${error.stack}`);
        
        // Update rule status
        rulesToRun[0].status = {
          lastRun: new Date().toISOString(),
          result: 'ERROR',
          message: error.message
        };
        
        // Save the updated status
        updateRuleInConfig(rulesToRun[0]);
        
        return {
          success: false,
          message: `Error processing rule: ${error.message}`,
          ruleId: rulesToRun[0].id
        };
      }
    }
    
    // Multiple rules - process normally
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
    // Create the schedule using the scheduling module function
    return createSchedule(schedule);
  } catch (error) {
    console.error('Error saving schedule:', error);
    return {
      success: false,
      message: 'Error saving schedule: ' + error.message
    };
  }
}

/**
 * Gets the status of all schedule triggers
 * @return {Object} Status information about schedule triggers
 */
function getScheduleStatus() {
  try {
    // Call the scheduling module function
    const config = getConfig();
    const scheduleSettings = config.preferences?.schedule || { frequency: 'manual' };
    
    // Get project triggers
    const triggers = ScriptApp.getProjectTriggers();
    const scheduleTriggers = triggers.filter(trigger => 
      trigger.getHandlerFunction() === 'runScheduledIngest' || 
      trigger.getHandlerFunction() === 'checkForMonthlyRun'
    );
    
    return {
      active: scheduleTriggers.length > 0,
      triggers: scheduleTriggers.map(trigger => ({
        id: trigger.getUniqueId(),
        function: trigger.getHandlerFunction(),
        type: trigger.getTriggerSource()
      })),
      settings: scheduleSettings,
      nextRunTime: getNextRunTime(scheduleSettings)
    };
  } catch (error) {
    console.error('Error getting schedule status:', error);
    return {
      active: false,
      error: error.message
    };
  }
}

/**
 * Runs all scheduled rules now
 * @return {Object} Result with success flag and message
 */
function runScheduleNow() {
  try {
    // Run the scheduled ingest function
    const result = runScheduledIngest();
    return {
      success: true,
      message: 'Scheduled tasks executed manually',
      result: result
    };
  } catch (error) {
    console.error('Error running scheduled rules:', error);
    return {
      success: false,
      message: 'Error running scheduled rules: ' + error.message
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
    const rows = [['Session ID', 'Timestamp', 'Event Type', 'Message', 'Description', 'Summary']];
    
    logs.forEach(session => {
      // Find the session description from the START event
      let sessionDescription = 'Session started';
      if (session.events && session.events.length > 0) {
        const startEvent = session.events.find(event => event.type === 'START');
        if (startEvent) {
          sessionDescription = startEvent.message;
        }
      }
      
      // Create event type summary for this session
      let eventSummary = {};
      session.events.forEach(event => {
        eventSummary[event.type] = (eventSummary[event.type] || 0) + 1;
      });
      
      // Format the event summary
      const summaryText = Object.entries(eventSummary)
        .map(([type, count]) => `${type}: ${count}`)
        .join(', ');
      
      // Calculate duration if session has more than one event
      let durationText = '';
      if (session.events && session.events.length > 1) {
        const firstEvent = new Date(session.events[0].timestamp);
        const lastEvent = new Date(session.events[session.events.length - 1].timestamp);
        const durationMs = lastEvent - firstEvent;
        const durationSec = Math.round(durationMs / 1000);
        
        if (durationSec < 60) {
          durationText = `Duration: ${durationSec}s`;
        } else {
          const min = Math.floor(durationSec / 60);
          const sec = durationSec % 60;
          durationText = `Duration: ${min}m ${sec}s`;
        }
      }
      
      // Add session header with summary information
      rows.push([
        session.sessionId,
        new Date(session.timestamp).toLocaleString(),
        'SESSION_START',
        'Session started',
        sessionDescription,
        `${summaryText}${durationText ? ' | ' + durationText : ''}`
      ]);
      
      // Add events (sorted by timestamp for clarity)
      const sortedEvents = [...session.events].sort((a, b) => 
        new Date(a.timestamp) - new Date(b.timestamp)
      );
      
      sortedEvents.forEach(event => {
        // Skip the START event as it's redundant with the session header
        if (event.type === 'START') return;
        
        rows.push([
          '',
          new Date(event.timestamp).toLocaleString(),
          event.type,
          event.message,
          '',
          ''
        ]);
      });
      
      // Add separator
      rows.push(['', '', '', '', '', '']);
    });
    
    // Write to sheet
    sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
    
    // Format headers
    sheet.getRange(1, 1, 1, rows[0].length).setFontWeight('bold');
    
    // Format session headers to be more visible
    for (let i = 2; i < rows.length; i++) {
      if (rows[i][2] === 'SESSION_START') {
        const range = sheet.getRange(i, 1, 1, rows[0].length);
        range.setBackground('#e6f2ff');
        range.setFontWeight('bold');
      }
    }
    
    // Add color coding for event types
    for (let i = 2; i < rows.length; i++) {
      const eventType = rows[i][2];
      if (!eventType || eventType === 'SESSION_START') continue;
      
      let color = '';
      switch (eventType) {
        case 'SUCCESS':
        case 'COMPLETE':
          color = '#e6ffe6'; // Light green
          break;
        case 'ERROR':
          color = '#ffe6e6'; // Light red
          break;
        case 'WARNING':
          color = '#fff8e6'; // Light yellow
          break;
        case 'PROCESSING':
        case 'INFO':
          color = '#e6f0ff'; // Light blue
          break;
      }
      
      if (color) {
        sheet.getRange(i, 3, 1, 1).setBackground(color);
      }
    }
    
    // Resize columns
    for (let i = 1; i <= rows[0].length; i++) {
      sheet.autoResizeColumn(i);
    }
    
    // Add a timestamp for when this export was created
    const timestamp = new Date().toLocaleString();
    sheet.getRange(rows.length + 2, 1, 1, 2).setValues([['Exported at:', timestamp]]);
    
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

/**
 * Gets all active processes
 * @return {Object} Object containing all active processes
 */
function getAllActiveProcesses() {
  try {
    return debugProcessTracking();
  } catch (error) {
    console.error('Error getting active processes:', error);
    return {
      success: false,
      message: 'Error getting active processes: ' + error.message,
      processes: {}
    };
  }
}

/**
 * Deletes a processing job's tracking data
 * @param {string} processingId - The ID of the processing job to delete
 * @return {Object} Result with success flag and message
 */
function deleteProcessingJob(processingId) {
  try {
    return deleteProcessTracking(processingId);
  } catch (error) {
    console.error('Error deleting process:', error);
    return {
      success: false,
      message: 'Error deleting process: ' + error.message
    };
  }
}

/**
 * Calculates the next estimated run time based on schedule settings
 * @param {Object} settings - The schedule settings
 * @return {string} The next run time as a string
 */
function getNextRunTime(settings) {
  if (!settings || settings.frequency === 'manual') {
    return 'No automatic schedule set';
  }
  
  const now = new Date();
  let nextRun = new Date(now);
  
  try {
    switch (settings.frequency) {
      case 'hourly':
        // Next hour
        nextRun.setHours(now.getHours() + 1);
        nextRun.setMinutes(0);
        nextRun.setSeconds(0);
        break;
        
      case 'daily':
        // Next occurrence of the specified time
        const [hours, minutes] = settings.time.split(':').map(Number);
        nextRun.setHours(hours);
        nextRun.setMinutes(minutes);
        nextRun.setSeconds(0);
        
        // If the time has already passed today, move to tomorrow
        if (nextRun <= now) {
          nextRun.setDate(nextRun.getDate() + 1);
        }
        break;
        
      case 'weekly':
        // Next occurrence of the specified day of week at the specified time
        const [weekHours, weekMinutes] = settings.time.split(':').map(Number);
        const dayOfWeek = parseInt(settings.dayOfWeek);
        
        nextRun.setHours(weekHours);
        nextRun.setMinutes(weekMinutes);
        nextRun.setSeconds(0);
        
        // Calculate days until next occurrence
        let daysUntil = dayOfWeek - now.getDay();
        if (daysUntil <= 0 || (daysUntil === 0 && nextRun <= now)) {
          daysUntil += 7;
        }
        
        nextRun.setDate(nextRun.getDate() + daysUntil);
        break;
        
      case 'monthly':
        // Next occurrence of the specified day of month at the specified time
        const [monthHours, monthMinutes] = settings.time.split(':').map(Number);
        nextRun.setHours(monthHours);
        nextRun.setMinutes(monthMinutes);
        nextRun.setSeconds(0);
        
        if (settings.dayOfMonth === 'last') {
          // Last day of the current month
          nextRun = new Date(now.getFullYear(), now.getMonth() + 1, 0);
          nextRun.setHours(monthHours);
          nextRun.setMinutes(monthMinutes);
          nextRun.setSeconds(0);
          
          // If the last day of this month has passed, get the last day of next month
          if (nextRun <= now) {
            nextRun = new Date(now.getFullYear(), now.getMonth() + 2, 0);
            nextRun.setHours(monthHours);
            nextRun.setMinutes(monthMinutes);
            nextRun.setSeconds(0);
          }
        } else {
          // Specific day of month
          const dayOfMonth = parseInt(settings.dayOfMonth);
          nextRun.setDate(dayOfMonth);
          
          // If the day has already passed this month, move to next month
          if (nextRun <= now) {
            nextRun.setMonth(nextRun.getMonth() + 1);
          }
        }
        break;
    }
    
    return nextRun.toLocaleString();
  } catch (error) {
    console.error('Error calculating next run time:', error);
    return 'Error calculating next run time';
  }
}

/**
 * Exports a single log session to a sheet for detailed analysis
 * @param {string} sessionId - The ID of the session to export
 * @return {Object} Result with success flag and message
 */
function exportLogSessionToSheet(sessionId) {
  try {
    if (!sessionId) {
      return {
        success: false,
        message: 'No session ID provided'
      };
    }
    
    const logs = getLogs();
    const session = logs.find(s => s.sessionId === sessionId);
    
    if (!session) {
      return {
        success: false,
        message: 'Session not found'
      };
    }
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Create a sheet name based on the session timestamp
    const sessionDate = new Date(session.timestamp);
    const sheetName = `Log Session ${sessionDate.toLocaleDateString().replace(/\//g, '-')}`;
    
    // Create or get export sheet
    let sheet = ss.getSheetByName(sheetName);
    let sheetIndex = 0;
    let uniqueSheetName = sheetName;
    
    // Handle duplicate sheet names by adding index
    while (sheet) {
      sheetIndex++;
      uniqueSheetName = `${sheetName} (${sheetIndex})`;
      sheet = ss.getSheetByName(uniqueSheetName);
    }
    
    sheet = ss.insertSheet(uniqueSheetName);
    
    // Find the session description from the START event
    let sessionDescription = 'Session details';
    if (session.events && session.events.length > 0) {
      const startEvent = session.events.find(event => event.type === 'START');
      if (startEvent) {
        sessionDescription = startEvent.message;
      }
    }
    
    // Add session header
    sheet.getRange(1, 1, 1, 2).setValues([['Session Details', sessionDescription]]);
    sheet.getRange(2, 1, 1, 2).setValues([['Session ID', session.sessionId]]);
    sheet.getRange(3, 1, 1, 2).setValues([['Start Time', new Date(session.timestamp).toLocaleString()]]);
    
    // Calculate session duration if more than one event
    if (session.events && session.events.length > 1) {
      const firstEvent = new Date(session.events[0].timestamp);
      const lastEvent = new Date(session.events[session.events.length - 1].timestamp);
      const durationMs = lastEvent - firstEvent;
      const durationSec = Math.round(durationMs / 1000);
      
      let durationText = '';
      if (durationSec < 60) {
        durationText = `${durationSec} seconds`;
      } else {
        const min = Math.floor(durationSec / 60);
        const sec = durationSec % 60;
        durationText = `${min} minutes, ${sec} seconds`;
      }
      
      sheet.getRange(4, 1, 1, 2).setValues([['Duration', durationText]]);
    }
    
    // Add event summary
    let eventSummary = {};
    session.events.forEach(event => {
      eventSummary[event.type] = (eventSummary[event.type] || 0) + 1;
    });
    
    const eventSummaryRows = Object.entries(eventSummary).map(([type, count]) => [type, count]);
    if (eventSummaryRows.length > 0) {
      const startRow = 6;
      sheet.getRange(startRow, 1, 1, 2).setValues([['Event Type', 'Count']]);
      sheet.getRange(startRow + 1, 1, eventSummaryRows.length, 2).setValues(eventSummaryRows);
    }
    
    // Add detailed events
    const headers = ['Timestamp', 'Event Type', 'Message'];
    const startDetailRow = 6 + eventSummaryRows.length + 2;
    
    sheet.getRange(startDetailRow, 1, 1, 3).setValues([headers]);
    
    // Sort events by timestamp
    const sortedEvents = [...session.events].sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );
    
    const eventRows = sortedEvents.map(event => [
      new Date(event.timestamp).toLocaleString(),
      event.type,
      event.message
    ]);
    
    if (eventRows.length > 0) {
      sheet.getRange(startDetailRow + 1, 1, eventRows.length, 3).setValues(eventRows);
    }
    
    // Format the sheet
    sheet.getRange(1, 1, 1, 2).setFontWeight('bold').setBackground('#e6f2ff');
    sheet.getRange(6, 1, 1, 2).setFontWeight('bold').setBackground('#f0f0f0');
    sheet.getRange(startDetailRow, 1, 1, 3).setFontWeight('bold').setBackground('#f0f0f0');
    
    // Color code event types
    for (let i = 0; i < eventRows.length; i++) {
      const eventType = eventRows[i][1];
      let color = '';
      
      switch (eventType) {
        case 'SUCCESS':
        case 'COMPLETE':
          color = '#e6ffe6'; // Light green
          break;
        case 'ERROR':
          color = '#ffe6e6'; // Light red
          break;
        case 'WARNING':
          color = '#fff8e6'; // Light yellow
          break;
        case 'PROCESSING':
        case 'INFO':
          color = '#e6f0ff'; // Light blue
          break;
      }
      
      if (color) {
        sheet.getRange(startDetailRow + 1 + i, 2, 1, 1).setBackground(color);
      }
    }
    
    // Resize columns
    for (let i = 1; i <= 3; i++) {
      sheet.autoResizeColumn(i);
    }
    
    return { 
      success: true, 
      message: 'Log session exported to sheet', 
      sheetId: sheet.getSheetId(),
      sheetName: uniqueSheetName
    };
  } catch (error) {
    return { 
      success: false, 
      message: 'Error exporting log session: ' + error.message 
    };
  }
}