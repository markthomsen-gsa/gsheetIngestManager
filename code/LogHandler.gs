/**
 * Log Handler for Data Ingest Manager
 * 
 * This file contains server-side functions for managing logs,
 * including retrieving logs, exporting them to sheets, and clearing logs.
 */

/**
 * Gets all logs from storage
 * @return {Array} Array of log session objects
 */
function getLogs() {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    const logsJson = scriptProperties.getProperty('LOGS');
    
    if (!logsJson) {
      return [];
    }
    
    return JSON.parse(logsJson);
  } catch (error) {
    console.error('Error getting logs:', error);
    return [];
  }
}

/**
 * Saves logs to storage
 * @param {Array} logs - Array of log session objects
 */
function saveLogs(logs) {
  try {
    const scriptProperties = PropertiesService.getScriptProperties();
    scriptProperties.setProperty('LOGS', JSON.stringify(logs));
  } catch (error) {
    console.error('Error saving logs:', error);
  }
}

/**
 * Gets log session summaries for UI display
 * @return {Array} Array of log session summary objects
 */
function getLogSessionSummaries() {
  try {
    // Get raw logs from storage
    const logs = getLogs();
    
    // Process each log to create summaries
    return logs.map(session => {
      // Count event types
      const typeCounts = {
        SUCCESS: 0,
        ERROR: 0,
        WARNING: 0,
        INFO: 0
      };
      
      // Process each event to count types
      session.events.forEach(event => {
        const type = event.type;
        
        // Map similar types together
        if (type === 'COMPLETE') {
          typeCounts.SUCCESS++;
        } else if (type === 'PROCESSING' || type === 'START') {
          typeCounts.INFO++;
        } else if (typeCounts[type] !== undefined) {
          typeCounts[type]++;
        } else {
          typeCounts.INFO++;
        }
      });
      
      // Calculate overall status based on event counts
      let status = 'COMPLETE';
      if (typeCounts.ERROR > 0) {
        status = 'ERROR';
      } else if (typeCounts.WARNING > 0) {
        status = 'COMPLETE WITH WARNINGS';
      }
      
      // For in-progress sessions, override status
      const lastEvent = session.events[session.events.length - 1];
      if (lastEvent && lastEvent.type === 'PROCESSING') {
        status = 'IN PROGRESS';
      }
      
      // Calculate duration
      let durationMs = 0;
      if (session.events.length > 1) {
        const firstEventTime = new Date(session.events[0].timestamp);
        const lastEventTime = new Date(session.events[session.events.length - 1].timestamp);
        durationMs = lastEventTime - firstEventTime;
      }
      
      // Find session description from the START event
      let description = 'Session started';
      if (session.events && session.events.length > 0) {
        const startEvent = session.events.find(event => event.type === 'START');
        if (startEvent) {
          description = startEvent.message;
        }
      }
      
      // Calculate progress percentage for in-progress sessions
      let progress = 100;
      if (status === 'IN PROGRESS') {
        // This is a simplified approach - in a real implementation,
        // you would use actual progress data from processing jobs
        const totalEvents = session.events.length;
        const processingIndex = session.events.findIndex(event => event.type === 'PROCESSING');
        if (processingIndex > 0) {
          progress = Math.min(95, Math.floor((processingIndex / totalEvents) * 100));
        } else {
          progress = Math.min(95, Math.floor(Math.random() * 100));
        }
      }
      
      // Return the summary object
      return {
        sessionId: session.sessionId,
        timestamp: session.timestamp,
        description: description,
        status: status,
        progress: progress,
        duration: durationMs,
        counts: typeCounts
      };
    });
  } catch (error) {
    console.error('Error getting log session summaries:', error);
    return [];
  }
}

/**
 * Clears all logs
 * @return {Object} Result with success flag and message
 */
function clearAllLogs() {
  try {
    saveLogs([]);
    return {
      success: true,
      message: 'All logs cleared successfully'
    };
  } catch (error) {
    console.error('Error clearing logs:', error);
    return {
      success: false,
      message: 'Error clearing logs: ' + error.message
    };
  }
}

/**
 * Exports a log session to a sheet
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
    
    // Get logs
    const logs = getLogs();
    const session = logs.find(s => s.sessionId === sessionId);
    
    if (!session) {
      return {
        success: false,
        message: 'Session not found'
      };
    }
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Find session description from the START event
    let sessionDescription = 'Log Session';
    if (session.events && session.events.length > 0) {
      const startEvent = session.events.find(event => event.type === 'START');
      if (startEvent) {
        sessionDescription = startEvent.message;
      }
    }
    
    // Create a sheet name based on the session timestamp and description
    const sessionDate = new Date(session.timestamp);
    const dateString = Utilities.formatDate(sessionDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
    // Limit the length of description in the sheet name
    const safeDescription = sessionDescription.substring(0, 20).replace(/[^a-zA-Z0-9 ]/g, '');
    const sheetName = `Log ${dateString} ${safeDescription}`;
    
    // Create or get export sheet, handling duplicates
    let sheet = ss.getSheetByName(sheetName);
    let sheetIndex = 0;
    let uniqueSheetName = sheetName;
    
    while (sheet) {
      sheetIndex++;
      uniqueSheetName = `${sheetName} (${sheetIndex})`;
      sheet = ss.getSheetByName(uniqueSheetName);
    }
    
    sheet = ss.insertSheet(uniqueSheetName);
    
    // ==== Section 1: Session Overview ====
    sheet.getRange(1, 1, 1, 2).setValues([['Session Overview', '']]);
    sheet.getRange(1, 1, 1, 2).merge();
    sheet.getRange(1, 1, 1, 2).setBackground('#4285f4').setFontColor('white').setFontWeight('bold');
    
    sheet.getRange(2, 1, 1, 2).setValues([['Session ID', session.sessionId]]);
    sheet.getRange(3, 1, 1, 2).setValues([['Description', sessionDescription]]);
    sheet.getRange(4, 1, 1, 2).setValues([['Start Time', Utilities.formatDate(sessionDate, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss")]]);
    
    // Calculate session duration if more than one event
    if (session.events && session.events.length > 1) {
      const firstEvent = new Date(session.events[0].timestamp);
      const lastEvent = new Date(session.events[session.events.length - 1].timestamp);
      const durationMs = lastEvent - firstEvent;
      
      sheet.getRange(5, 1, 1, 2).setValues([['Duration', formatDuration(durationMs)]]);
    }
    
    // ==== Section 2: Event Summary ====
    sheet.getRange(7, 1, 1, 2).setValues([['Event Summary', '']]);
    sheet.getRange(7, 1, 1, 2).merge();
    sheet.getRange(7, 1, 1, 2).setBackground('#f1f3f4').setFontWeight('bold');
    
    // Count event types
    const eventCounts = {};
    session.events.forEach(event => {
      eventCounts[event.type] = (eventCounts[event.type] || 0) + 1;
    });
    
    // Add event counts
    const eventCountsRows = Object.entries(eventCounts).map(([type, count]) => [type, count]);
    if (eventCountsRows.length > 0) {
      sheet.getRange(8, 1, 1, 2).setValues([['Event Type', 'Count']]);
      sheet.getRange(8, 1, 1, 2).setFontWeight('bold');
      sheet.getRange(9, 1, eventCountsRows.length, 2).setValues(eventCountsRows);
    }
    
    // ==== Section 3: Detailed Events ====
    const detailStartRow = 9 + eventCountsRows.length + 2;
    sheet.getRange(detailStartRow, 1, 1, 3).setValues([['Detailed Events', '', '']]);
    sheet.getRange(detailStartRow, 1, 1, 3).merge();
    sheet.getRange(detailStartRow, 1, 1, 3).setBackground('#f1f3f4').setFontWeight('bold');
    
    const headers = ['Timestamp', 'Event Type', 'Message'];
    sheet.getRange(detailStartRow + 1, 1, 1, 3).setValues([headers]);
    sheet.getRange(detailStartRow + 1, 1, 1, 3).setFontWeight('bold');
    
    // Sort events by timestamp
    const sortedEvents = [...session.events].sort((a, b) => 
      new Date(a.timestamp) - new Date(b.timestamp)
    );
    
    const eventRows = sortedEvents.map(event => [
      Utilities.formatDate(new Date(event.timestamp), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"),
      event.type,
      event.message
    ]);
    
    if (eventRows.length > 0) {
      sheet.getRange(detailStartRow + 2, 1, eventRows.length, 3).setValues(eventRows);
    }
    
    // Apply color coding to event types
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
        case 'START':
          color = '#e6f0ff'; // Light blue
          break;
      }
      
      if (color) {
        sheet.getRange(detailStartRow + 2 + i, 2, 1, 1).setBackground(color);
      }
    }
    
    // Format the sheet
    sheet.autoResizeColumns(1, 3);
    sheet.setColumnWidth(3, Math.max(400, sheet.getColumnWidth(3))); // Set message column wider
    
    return { 
      success: true, 
      message: 'Log session exported to sheet', 
      sheetId: sheet.getSheetId(),
      sheetName: uniqueSheetName
    };
  } catch (error) {
    console.error('Error exporting log session:', error);
    return { 
      success: false, 
      message: 'Error exporting log session: ' + error.message 
    };
  }
}

/**
 * Exports all logs to a sheet
 * @return {Object} Result with success flag and message
 */
function exportAllLogsToSheet() {
  try {
    const logs = getLogs();
    
    if (!logs || logs.length === 0) {
      return {
        success: false,
        message: 'No logs to export'
      };
    }
    
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Create a sheet name with current date
    const currentDate = new Date();
    const dateString = Utilities.formatDate(currentDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
    const sheetName = `All Logs ${dateString}`;
    
    // Create or get export sheet, handling duplicates
    let sheet = ss.getSheetByName(sheetName);
    let sheetIndex = 0;
    let uniqueSheetName = sheetName;
    
    while (sheet) {
      sheetIndex++;
      uniqueSheetName = `${sheetName} (${sheetIndex})`;
      sheet = ss.getSheetByName(uniqueSheetName);
    }
    
    sheet = ss.insertSheet(uniqueSheetName);
    
    // Add title row
    sheet.getRange(1, 1, 1, 5).setValues([['Log Export Summary', '', '', '', '']]);
    sheet.getRange(1, 1, 1, 5).merge();
    sheet.getRange(1, 1, 1, 5).setBackground('#4285f4').setFontColor('white').setFontWeight('bold');
    
    // Add export date
    sheet.getRange(2, 1, 1, 2).setValues([['Export Date', Utilities.formatDate(currentDate, Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss")]]);
    sheet.getRange(3, 1, 1, 2).setValues([['Total Sessions', logs.length]]);
    
    // Add table headers
    const headers = ['Session ID', 'Timestamp', 'Description', 'Status', 'Event Count'];
    sheet.getRange(5, 1, 1, 5).setValues([headers]);
    sheet.getRange(5, 1, 1, 5).setFontWeight('bold').setBackground('#f1f3f4');
    
    // Process each log session for the summary table
    const summaryRows = logs.map(session => {
      // Find session description from the START event
      let description = 'Session started';
      if (session.events && session.events.length > 0) {
        const startEvent = session.events.find(event => event.type === 'START');
        if (startEvent) {
          description = startEvent.message;
        }
      }
      
      // Calculate status
      let status = 'COMPLETE';
      const hasError = session.events.some(event => event.type === 'ERROR');
      const hasWarning = session.events.some(event => event.type === 'WARNING');
      const hasProcessing = session.events.some(event => event.type === 'PROCESSING');
      
      if (hasError) {
        status = 'ERROR';
      } else if (hasWarning) {
        status = 'WARNING';
      } else if (hasProcessing) {
        status = 'IN PROGRESS';
      }
      
      return [
        session.sessionId,
        Utilities.formatDate(new Date(session.timestamp), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"),
        description,
        status,
        session.events.length
      ];
    });
    
    // Add summary rows
    if (summaryRows.length > 0) {
      sheet.getRange(6, 1, summaryRows.length, 5).setValues(summaryRows);
    }
    
    // Apply color coding to status column
    for (let i = 0; i < summaryRows.length; i++) {
      const status = summaryRows[i][3];
      let color = '';
      
      switch (status) {
        case 'COMPLETE':
          color = '#e6ffe6'; // Light green
          break;
        case 'ERROR':
          color = '#ffe6e6'; // Light red
          break;
        case 'WARNING':
          color = '#fff8e6'; // Light yellow
          break;
        case 'IN PROGRESS':
          color = '#e6f0ff'; // Light blue
          break;
      }
      
      if (color) {
        sheet.getRange(6 + i, 4, 1, 1).setBackground(color);
      }
    }
    
    // Format the sheet
    sheet.autoResizeColumns(1, 5);
    sheet.setColumnWidth(3, Math.max(300, sheet.getColumnWidth(3))); // Set description column wider
    
    return { 
      success: true, 
      message: 'All logs exported to sheet', 
      sheetId: sheet.getSheetId(),
      sheetName: uniqueSheetName
    };
  } catch (error) {
    console.error('Error exporting all logs:', error);
    return { 
      success: false, 
      message: 'Error exporting all logs: ' + error.message 
    };
  }
}

/**
 * Format duration in milliseconds to a readable string
 * @param {number} durationMs - Duration in milliseconds
 * @return {string} Formatted duration string
 */
function formatDuration(durationMs) {
  const seconds = Math.floor(durationMs / 1000);
  
  if (seconds < 60) {
    return `${seconds}s`;
  }
  
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  
  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  
  return `${hours}h ${remainingMinutes}m ${remainingSeconds}s`;
}

/**
 * Adds a log event to a session
 * @param {string} sessionId - The ID of the session
 * @param {string} type - The type of event (INFO, ERROR, SUCCESS, etc.)
 * @param {string} message - The message to log
 * @return {Object} Result with success flag
 */
function addLogEvent(sessionId, type, message) {
  try {
    if (!sessionId) {
      console.error('No session ID provided for log event');
      return { success: false };
    }
    
    const logs = getLogs();
    const sessionIndex = logs.findIndex(s => s.sessionId === sessionId);
    
    if (sessionIndex === -1) {
      console.error(`Session not found: ${sessionId}`);
      return { success: false };
    }
    
    // Add the event
    logs[sessionIndex].events.push({
      timestamp: new Date().toISOString(),
      type: type,
      message: message
    });
    
    // Save the updated logs
    saveLogs(logs);
    
    return { success: true };
  } catch (error) {
    console.error('Error adding log event:', error);
    return { success: false };
  }
}

/**
 * Creates a new log session
 * @param {string} description - Description of the session
 * @return {Object} The new session object with ID
 */
function createLogSession(description) {
  try {
    const sessionId = 'session-' + Date.now();
    const timestamp = new Date().toISOString();
    
    const newSession = {
      sessionId: sessionId,
      timestamp: timestamp,
      events: [
        {
          timestamp: timestamp,
          type: 'START',
          message: description || 'Session started'
        }
      ]
    };
    
    // Add to existing logs
    const logs = getLogs();
    logs.push(newSession);
    
    // Keep only the most recent 100 sessions
    if (logs.length > 100) {
      logs.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
      logs.splice(100);
    }
    
    saveLogs(logs);
    
    return newSession;
  } catch (error) {
    console.error('Error creating log session:', error);
    return null;
  }
}

/**
 * Gets data for the sidebar log display.
 * 
 * @return {Object} Object containing log data for the sidebar
 */
function getLogSidebarData() {
  return {
    logs: getLogSessionSummaries()
  };
} 