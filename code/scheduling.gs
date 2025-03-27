/**
 * Data Ingest Manager - Scheduling Module
 * 
 * Provides functionality for scheduling automatic rule execution.
 * 
 * NOTE: This module is for future implementation. When scheduling is ready,
 * the functions in this module can be activated.
 */

/**
 * Creates a schedule based on the provided settings
 * @param {Object} scheduleSettings - The schedule settings
 * @return {Object} Result with success status and message
 */
function createSchedule(scheduleSettings) {
  try {
    // Validate settings
    if (!scheduleSettings.frequency) {
      return { 
        success: false, 
        message: 'Frequency is required' 
      };
    }
    
    // Save schedule settings
    const config = getConfig();
    config.preferences = config.preferences || {};
    config.preferences.schedule = scheduleSettings;
    saveConfig(config);
    
    // Create trigger
    const triggerResult = createScheduleTrigger(scheduleSettings);
    
    return { 
      success: true, 
      message: 'Schedule created successfully',
      triggerId: triggerResult.triggerId
    };
  } catch (error) {
    console.error('Error creating schedule:', error);
    return { 
      success: false, 
      message: 'Error creating schedule: ' + error.message 
    };
  }
}

/**
 * Gets the current schedule settings
 * @return {Object} The schedule settings
 */
function getSchedule() {
  const config = getConfig();
  return config.preferences?.schedule || { frequency: 'manual' };
}

/**
 * Deletes the schedule
 * @return {Object} Result with success status and message
 */
function deleteSchedule() {
  try {
    // Delete triggers
    deleteScheduleTriggers();
    
    // Remove schedule settings
    const config = getConfig();
    if (config.preferences) {
      delete config.preferences.schedule;
      saveConfig(config);
    }
    
    return { 
      success: true, 
      message: 'Schedule deleted successfully' 
    };
  } catch (error) {
    console.error('Error deleting schedule:', error);
    return { 
      success: false, 
      message: 'Error deleting schedule: ' + error.message 
    };
  }
}

/**
 * Creates a time-based trigger for scheduled execution
 * @param {Object} settings - The schedule settings
 * @return {Object} Result with success status and message
 */
function createScheduleTrigger(settings) {
  // Delete any existing schedule triggers
  deleteScheduleTriggers();
  
  // Create new trigger based on frequency
  let trigger;
  
  switch (settings.frequency) {
    case 'one-time':
      // Parse the date and time for one-time trigger
      if (!settings.date || !settings.time) {
        return { 
          success: false, 
          message: 'Date and time are required for one-time schedule' 
        };
      }
      
      try {
        const [year, month, day] = settings.date.split('-').map(Number);
        const [hours, minutes] = settings.time.split(':').map(Number);
        
        // Create Date object (note: month is 0-indexed in JavaScript)
        const triggerDate = new Date(year, month - 1, day, hours, minutes, 0);
        
        // Check if date is in the future
        if (triggerDate <= new Date()) {
          return { 
            success: false, 
            message: 'The scheduled time must be in the future' 
          };
        }
        
        trigger = ScriptApp.newTrigger('runScheduledIngest')
          .timeBased()
          .at(triggerDate)
          .create();
      } catch (error) {
        console.error('Error creating one-time trigger:', error);
        return { 
          success: false, 
          message: 'Error creating one-time trigger: ' + error.message 
        };
      }
      break;
      
    case 'hourly':
      trigger = ScriptApp.newTrigger('runScheduledIngest')
        .timeBased()
        .everyHours(1)
        .create();
      break;
      
    case 'daily':
      trigger = ScriptApp.newTrigger('runScheduledIngest')
        .timeBased()
        .atHour(parseInt(settings.time.split(':')[0]))
        .nearMinute(parseInt(settings.time.split(':')[1]))
        .everyDays(1)
        .create();
      break;
      
    case 'weekly':
      trigger = ScriptApp.newTrigger('runScheduledIngest')
        .timeBased()
        .atHour(parseInt(settings.time.split(':')[0]))
        .nearMinute(parseInt(settings.time.split(':')[1]))
        .onWeekDay(parseInt(settings.dayOfWeek))
        .create();
      break;
      
    case 'monthly':
      // For monthly, we'll use a daily trigger and custom logic to check the day
      trigger = ScriptApp.newTrigger('checkForMonthlyRun')
        .timeBased()
        .atHour(parseInt(settings.time.split(':')[0]))
        .nearMinute(parseInt(settings.time.split(':')[1]))
        .everyDays(1)
        .create();
      break;
  }
  
  // Save trigger ID to properties
  if (trigger) {
    settings.triggerId = trigger.getUniqueId();
    
    const config = getConfig();
    config.preferences = config.preferences || {};
    config.preferences.schedule = settings;
    saveConfig(config);
    
    return { success: true, message: 'Schedule trigger created', triggerId: trigger.getUniqueId() };
  }
  
  return { success: false, message: 'Failed to create schedule trigger' };
}

/**
 * Deletes all schedule-related triggers
 * @return {Object} Result with success status and message
 */
function deleteScheduleTriggers() {
  try {
  const triggers = ScriptApp.getProjectTriggers();
  
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === 'runScheduledIngest' || 
        trigger.getHandlerFunction() === 'checkForMonthlyRun') {
      ScriptApp.deleteTrigger(trigger);
    }
  });
  
  // Update config
  const config = getConfig();
  if (config.preferences?.schedule) {
    delete config.preferences.schedule.triggerId;
    saveConfig(config);
  }
    
    return { success: true, message: 'Schedule triggers deleted' };
  } catch (error) {
    console.error('Error deleting triggers:', error);
    return { success: false, message: 'Error deleting triggers: ' + error.message };
  }
}

/**
 * Runs all active rules based on schedule
 */
function runScheduledIngest() {
  const config = getConfig();
  const scheduleSettings = config.preferences?.schedule;
  
  if (!scheduleSettings || scheduleSettings.frequency === 'manual') {
    // No schedule or manual mode, do nothing
    console.log('No schedule configured or in manual mode');
    return;
  }
  
  // Create a log session for this scheduled run
  const sessionId = createLogSession(`Scheduled run (${scheduleSettings.frequency})`);
  addLogEvent(sessionId, 'INFO', `Starting scheduled data ingest (${scheduleSettings.frequency} schedule)`);
  
  // Run all active rules
  let result;
  try {
    // Get active rules
    const activeRuleIds = config.rules
      .filter(rule => rule.active !== false)
      .map(rule => rule.id);
    
    if (activeRuleIds.length === 0) {
      addLogEvent(sessionId, 'WARNING', 'No active rules found to run');
      result = {
        success: false,
        message: 'No active rules found',
        successCount: 0,
        errorCount: 0
      };
    } else {
      addLogEvent(sessionId, 'INFO', `Found ${activeRuleIds.length} active rules to run`);
      
      // Process each rule
      let successCount = 0;
      let errorCount = 0;
      
      for (const ruleId of activeRuleIds) {
        const rule = config.rules.find(r => r.id === ruleId);
        
        try {
          addLogEvent(sessionId, 'PROCESSING', `Processing rule: ${rule.description}`);
          
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
          
          successCount++;
          addLogEvent(sessionId, 'SUCCESS', `Rule executed successfully: ${rule.description}`);
        } catch (error) {
          errorCount++;
          
          // Update rule status
          rule.status = {
            lastRun: new Date().toISOString(),
            result: 'ERROR',
            message: error.message
          };
          
          addLogEvent(sessionId, 'ERROR', `Error processing rule ${rule.id}: ${error.message}`);
        }
      }
      
      // Save updated rule statuses
      saveConfig(config);
      
      // Set result
      result = {
        success: true,
        message: `Completed: ${successCount} successful, ${errorCount} failed`,
        successCount: successCount,
        errorCount: errorCount
      };
      
      // Add completion event
      addLogEvent(sessionId, 'COMPLETE', result.message);
    }
  } catch (error) {
    console.error('Error in scheduled run:', error);
    addLogEvent(sessionId, 'ERROR', `Scheduled run error: ${error.message}`);
    
    result = {
      success: false,
      message: 'Error in scheduled run: ' + error.message,
      successCount: 0,
      errorCount: 0
    };
  }
  
  // Send email notification if configured
  if (scheduleSettings.notificationEmail) {
    const recipients = scheduleSettings.notificationEmail.split(',').map(email => email.trim());
    
    sendEmailNotification(
      'Data Ingest Manager - Scheduled Run Results',
      `Scheduled data ingest completed at ${new Date().toLocaleString()}\n\n` +
      `Result: ${result.successCount} successful, ${result.errorCount} failed\n\n` +
      `Log into the spreadsheet to view details.`,
      recipients
    );
  }
  
  return result;
}

/**
 * Checks if today is the day to run monthly schedule
 */
function checkForMonthlyRun() {
  const config = getConfig();
  const scheduleSettings = config.preferences?.schedule;
  
  if (!scheduleSettings || scheduleSettings.frequency !== 'monthly') {
    // Not monthly schedule, do nothing
    return;
  }
  
  const today = new Date();
  const dayOfMonth = scheduleSettings.dayOfMonth;
  
  let shouldRun = false;
  
  if (dayOfMonth === 'last') {
    // Check if today is the last day of the month
    const tomorrow = new Date(today);
    tomorrow.setDate(today.getDate() + 1);
    shouldRun = tomorrow.getMonth() !== today.getMonth();
  } else {
    // Check if today matches the specified day of month
    shouldRun = today.getDate() === parseInt(dayOfMonth);
  }
  
  if (shouldRun) {
    console.log('Running monthly scheduled task');
    runScheduledIngest();
  } else {
    console.log('Not the right day for monthly schedule');
  }
}

/**
 * Gets the status of all schedule triggers
 * @return {Object} Status information about all schedule triggers
 */
function getScheduleStatus() {
  try {
    const triggers = ScriptApp.getProjectTriggers();
    const scheduleTriggers = triggers.filter(trigger => 
      trigger.getHandlerFunction() === 'runScheduledIngest' || 
      trigger.getHandlerFunction() === 'checkForMonthlyRun'
    );
    
    const config = getConfig();
    const scheduleSettings = config.preferences?.schedule || { frequency: 'manual' };
    
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
      case 'one-time':
        // For one-time schedule, parse the date and time
        if (!settings.date || !settings.time) {
          return 'Incomplete one-time schedule';
        }
        
        const [oneTimeYear, oneTimeMonth, oneTimeDay] = settings.date.split('-').map(Number);
        const [oneTimeHours, oneTimeMinutes] = settings.time.split(':').map(Number);
        
        // Create Date object (note: month is 0-indexed in JavaScript)
        nextRun = new Date(oneTimeYear, oneTimeMonth - 1, oneTimeDay, oneTimeHours, oneTimeMinutes, 0);
        
        // If the time has already passed, indicate that
        if (nextRun <= now) {
          return 'Scheduled time has passed';
        }
        break;
        
      case 'hourly':
        // Next hour
        nextRun.setHours(now.getHours() + 1);
        nextRun.setMinutes(0);
        nextRun.setSeconds(0);
        break;
        
      case 'daily':
        // Next occurrence of the specified time
        const [dailyHours, dailyMinutes] = settings.time.split(':').map(Number);
        nextRun.setHours(dailyHours);
        nextRun.setMinutes(dailyMinutes);
        nextRun.setSeconds(0);
        
        // If the time has already passed today, move to tomorrow
        if (nextRun <= now) {
          nextRun.setDate(nextRun.getDate() + 1);
        }
        break;
        
      case 'weekly':
        // Next occurrence of the specified day of week at the specified time
        const [weeklyHours, weeklyMinutes] = settings.time.split(':').map(Number);
        const dayOfWeek = parseInt(settings.dayOfWeek);
        
        nextRun.setHours(weeklyHours);
        nextRun.setMinutes(weeklyMinutes);
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
        const [monthlyHours, monthlyMinutes] = settings.time.split(':').map(Number);
        nextRun.setHours(monthlyHours);
        nextRun.setMinutes(monthlyMinutes);
        nextRun.setSeconds(0);
        
        if (settings.dayOfMonth === 'last') {
          // Last day of the current month
          nextRun = new Date(now.getFullYear(), now.getMonth() + 1, 0);
          nextRun.setHours(monthlyHours);
          nextRun.setMinutes(monthlyMinutes);
          nextRun.setSeconds(0);
          
          // If the last day of this month has passed, get the last day of next month
          if (nextRun <= now) {
            nextRun = new Date(now.getFullYear(), now.getMonth() + 2, 0);
            nextRun.setHours(monthlyHours);
            nextRun.setMinutes(monthlyMinutes);
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
 * Manually triggers a scheduled run
 * @return {Object} Result of the run
 */
function runScheduleNow() {
  try {
    const result = runScheduledIngest();
    return {
      success: true,
      message: 'Scheduled tasks executed manually',
      result: result
    };
  } catch (error) {
    console.error('Error running schedule now:', error);
    return {
      success: false,
      message: 'Error running scheduled tasks: ' + error.message
    };
  }
}