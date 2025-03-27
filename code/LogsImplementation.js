/**
 * Log Card Implementation for Data Ingest Manager
 * 
 * This file contains functions for fetching log sessions, summarizing their content,
 * and exporting them to sheets.
 */

/**
 * Gets log sessions with summary information
 * @return {Array} Array of log session objects with summary data
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
        progress = Math.min(95, Math.floor(Math.random() * 100));
      }
      
      // Return the summary object
      return {
        sessionId: session.sessionId,
        timestamp: session.timestamp,
        description: description,
        status: status,
        progress: progress,
        duration: durationMs,
        counts: typeCounts,
        // Include original events for drill-down
        events: session.events
      };
    });
  } catch (error) {
    console.error('Error getting log session summaries:', error);
    return [];
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
    const dateString = sessionDate.toLocaleDateString().replace(/\//g, '-');
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
    sheet.getRange(1, 1, 1, 2).setValues([['Session Overview', sessionDescription]]);
    sheet.getRange(1, 1, 1, 2).merge();
    sheet.getRange(1, 1, 1, 2).setBackground('#4285f4').setFontColor('white').setFontWeight('bold');
    
    sheet.getRange(2, 1, 1, 2).setValues([['Session ID', session.sessionId]]);
    sheet.getRange(3, 1, 1, 2).setValues([['Start Time', new Date(session.timestamp).toLocaleString()]]);
    
    // Calculate session duration if more than one event
    if (session.events && session.events.length > 1) {
      const firstEvent = new Date(session.events[0].timestamp);
      const lastEvent = new Date(session.events[session.events.length - 1].timestamp);
      const durationMs = lastEvent - firstEvent;
      
      sheet.getRange(4, 1, 1, 2).setValues([['Duration', formatDuration(durationMs)]]);
    }
    
    // ==== Section 2: Event Summary ====
    sheet.getRange(6, 1, 1, 2).setValues([['Event Summary', '']]);
    sheet.getRange(6, 1, 1, 2).merge();
    sheet.getRange(6, 1, 1, 2).setBackground('#f1f3f4').setFontWeight('bold');
    
    // Count event types
    const eventCounts = {};
    session.events.forEach(event => {
      eventCounts[event.type] = (eventCounts[event.type] || 0) + 1;
    });
    
    // Add event counts
    const eventCountsRows = Object.entries(eventCounts).map(([type, count]) => [type, count]);
    if (eventCountsRows.length > 0) {
      sheet.getRange(7, 1, 1, 2).setValues([['Event Type', 'Count']]);
      sheet.getRange(7, 1, 1, 2).setFontWeight('bold');
      sheet.getRange(8, 1, eventCountsRows.length, 2).setValues(eventCountsRows);
    }
    
    // ==== Section 3: Detailed Events ====
    const detailStartRow = 8 + eventCountsRows.length + 2;
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
      new Date(event.timestamp).toLocaleString(),
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
    return { 
      success: false, 
      message: 'Error exporting log session: ' + error.message 
    };
  }
}

/**
 * Updates the UI with log session cards
 * @param {Array} logSummaries - Array of log session summaries
 */
function updateLogSessionCards(logSummaries) {
  const container = document.getElementById('logSessionsContainer');
  if (!container) return;
  
  // Clear existing logs
  container.innerHTML = '';
  
  // Show empty state if no logs
  if (!logSummaries || logSummaries.length === 0) {
    document.querySelector('.log-cards').style.display = 'none';
    document.querySelector('.empty-state').style.display = 'block';
    return;
  }
  
  document.querySelector('.log-cards').style.display = 'grid';
  document.querySelector('.empty-state').style.display = 'none';
  
  // Add each log session card
  logSummaries.forEach(session => {
    // Determine status color for progress bar
    let progressClass = '';
    if (session.status === 'ERROR') {
      progressClass = 'progress-error';
    } else if (session.status === 'COMPLETE WITH WARNINGS') {
      progressClass = 'progress-warning';
    } else if (session.status === 'COMPLETE') {
      progressClass = 'progress-complete';
    }
    
    // Format duration
    const durationText = formatDuration(session.duration);
    
    // Create card element
    const card = document.createElement('div');
    card.className = 'log-card';
    card.setAttribute('data-session-id', session.sessionId);
    
    // Create card content
    card.innerHTML = `
      <div class="card-header">
        <div>
          <h3 class="card-title">${session.description}</h3>
          <div class="card-time">${new Date(session.timestamp).toLocaleString()}</div>
          <div class="session-id">ID: ${session.sessionId}</div>
        </div>
      </div>
      <div class="card-body">
        <div class="status-summary">
          <div class="status-item">
            <div class="status-count status-success">${session.counts.SUCCESS}</div>
            <div class="status-label">SUCCESS</div>
          </div>
          <div class="status-item">
            <div class="status-count status-error">${session.counts.ERROR}</div>
            <div class="status-label">ERROR</div>
          </div>
          <div class="status-item">
            <div class="status-count status-warning">${session.counts.WARNING}</div>
            <div class="status-label">WARNING</div>
          </div>
          <div class="status-item">
            <div class="status-count status-info">${session.counts.INFO}</div>
            <div class="status-label">INFO</div>
          </div>
        </div>
        <div class="progress-container">
          <div class="progress-bar ${progressClass}" style="width: ${session.progress}%"></div>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #666;">
          <div>Duration: ${durationText}${session.status === 'IN PROGRESS' ? ' (running)' : ''}</div>
          <div>Status: ${session.status}${session.status === 'IN PROGRESS' ? ` (${session.progress}%)` : ''}</div>
        </div>
      </div>
      <div class="card-footer">
        <button class="btn btn-secondary detail-btn">
          <i class="bi bi-eye btn-icon"></i>Details
        </button>
        <button class="btn btn-primary export-btn" ${session.status === 'IN PROGRESS' ? 'disabled' : ''}>
          <i class="bi bi-file-earmark-spreadsheet btn-icon"></i>Export
        </button>
      </div>
    `;
    
    // Add to container
    container.appendChild(card);
  });
  
  // Add event listeners to export buttons
  document.querySelectorAll('.export-btn').forEach(button => {
    button.addEventListener('click', function() {
      if (this.disabled) return;
      
      const card = this.closest('.log-card');
      const sessionId = card.getAttribute('data-session-id');
      
      // Show spinner during export
      this.innerHTML = '<i class="bi bi-arrow-repeat btn-icon spinning"></i>Exporting...';
      this.disabled = true;
      
      // Call server-side function to export
      google.script.run
        .withSuccessHandler(function(result) {
          // Update button when complete
          button.innerHTML = '<i class="bi bi-file-earmark-spreadsheet btn-icon"></i>Export';
          button.disabled = false;
          
          if (result.success) {
            // Show success message
            showToast(`Log exported to sheet "${result.sheetName}"`);
          } else {
            // Show error message
            showToast(`Export failed: ${result.message}`, true);
          }
        })
        .withFailureHandler(function(error) {
          // Update button when failed
          button.innerHTML = '<i class="bi bi-file-earmark-spreadsheet btn-icon"></i>Export';
          button.disabled = false;
          
          // Show error message
          showToast(`Export failed: ${error}`, true);
        })
        .exportLogSessionToSheet(sessionId);
    });
  });
  
  // Add event listeners to detail buttons
  document.querySelectorAll('.detail-btn').forEach(button => {
    button.addEventListener('click', function() {
      const card = this.closest('.log-card');
      const sessionId = card.getAttribute('data-session-id');
      
      // In a real implementation, this would show a modal with details
      // For this mockup, we'll just show a message
      alert(`Details would be shown for session: ${sessionId}`);
    });
  });
}

/**
 * Shows a toast message
 * @param {string} message - The message to show
 * @param {boolean} isError - Whether the message is an error
 */
function showToast(message, isError = false) {
  // Create toast element if it doesn't exist
  let toast = document.getElementById('toast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'toast';
    toast.style.position = 'fixed';
    toast.style.bottom = '20px';
    toast.style.right = '20px';
    toast.style.padding = '10px 20px';
    toast.style.borderRadius = '4px';
    toast.style.color = 'white';
    toast.style.fontSize = '14px';
    toast.style.zIndex = '1000';
    toast.style.transition = 'opacity 0.5s';
    document.body.appendChild(toast);
  }
  
  // Set toast style based on message type
  if (isError) {
    toast.style.backgroundColor = '#db4437';
  } else {
    toast.style.backgroundColor = '#0f9d58';
  }
  
  // Set message and show toast
  toast.textContent = message;
  toast.style.opacity = '1';
  
  // Hide toast after 3 seconds
  setTimeout(() => {
    toast.style.opacity = '0';
  }, 3000);
}

/**
 * Refreshes log data from the server
 */
function refreshLogs() {
  // Show loading spinner
  document.getElementById('refreshBtn').innerHTML = '<i class="bi bi-arrow-repeat spinning"></i>';
  document.getElementById('refreshBtn').disabled = true;
  
  // Call server-side function to get log data
  google.script.run
    .withSuccessHandler(function(data) {
      // Reset button
      document.getElementById('refreshBtn').innerHTML = '<i class="bi bi-arrow-clockwise btn-icon"></i>Refresh';
      document.getElementById('refreshBtn').disabled = false;
      
      // Update logs in UI
      updateLogSessionCards(data);
    })
    .withFailureHandler(function(error) {
      // Reset button
      document.getElementById('refreshBtn').innerHTML = '<i class="bi bi-arrow-clockwise btn-icon"></i>Refresh';
      document.getElementById('refreshBtn').disabled = false;
      
      // Show error message
      showToast(`Error refreshing logs: ${error}`, true);
    })
    .getLogSessionSummaries();
} 