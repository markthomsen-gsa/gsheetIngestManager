/**
 * Data Ingest Manager - Properties Management
 * 
 * Handles all interactions with the Document Properties service for
 * storing configuration and log data.
 */

/**
 * Gets the full configuration from Document Properties
 * @return {Object} The complete configuration object
 */
function getConfig() {
  const docProps = PropertiesService.getDocumentProperties();
  const configJson = docProps.getProperty('ingestConfig');
  return configJson ? JSON.parse(configJson) : { rules: [], preferences: {} };
}

/**
 * Saves the configuration to Document Properties
 * @param {Object} config - The configuration object to save
 */
function saveConfig(config) {
  const docProps = PropertiesService.getDocumentProperties();
  docProps.setProperty('ingestConfig', JSON.stringify(config));
}

/**
 * Gets a single rule by ID
 * @param {string} ruleId - The ID of the rule to retrieve
 * @return {Object|null} The rule object or null if not found
 */
function getRule(ruleId) {
  const config = getConfig();
  return config.rules.find(rule => rule.id === ruleId) || null;
}

/**
 * Updates a rule's status
 * @param {string} ruleId - The ID of the rule to update
 * @param {Object} status - The new status object
 */
function updateRuleStatus(ruleId, status) {
  const config = getConfig();
  const ruleIndex = config.rules.findIndex(rule => rule.id === ruleId);
  
  if (ruleIndex >= 0) {
    config.rules[ruleIndex].status = status;
    saveConfig(config);
  }
}

/**
 * Gets log data from Document Properties
 * @return {Array} Array of log session objects
 */
function getLogs() {
  const docProps = PropertiesService.getDocumentProperties();
  const logsJson = docProps.getProperty('ingestLogs');
  return logsJson ? JSON.parse(logsJson) : [];
}

/**
 * Saves log data to Document Properties
 * @param {Array} logs - Array of log session objects
 */
function saveLogs(logs) {
  const docProps = PropertiesService.getDocumentProperties();
  docProps.setProperty('ingestLogs', JSON.stringify(logs));
}

/**
 * Creates a new log session
 * @param {string} message - Initial message for the session
 * @return {string} The session ID
 */
function createLogSession(message) {
  const sessionId = 'session-' + new Date().getTime();
  const logs = getLogs();
  
  logs.unshift({
    sessionId: sessionId,
    timestamp: new Date().toISOString(),
    events: [{
      type: 'START',
      message: message,
      timestamp: new Date().toISOString()
    }]
  });
  
  // Limit to 50 sessions
  if (logs.length > 50) logs.length = 50;
  
  saveLogs(logs);
  return sessionId;
}

/**
 * Adds an event to an existing log session
 * @param {string} sessionId - The session ID
 * @param {string} eventType - The type of event
 * @param {string} message - The event message
 */
function addLogEvent(sessionId, eventType, message) {
  const logs = getLogs();
  const sessionIndex = logs.findIndex(session => session.sessionId === sessionId);
  
  if (sessionIndex >= 0) {
    logs[sessionIndex].events.push({
      type: eventType,
      message: message,
      timestamp: new Date().toISOString()
    });
    
    saveLogs(logs);
  }
}

/**
 * Properties Service Test Module
 * 
 * This module contains functions to test the Document Properties service
 * for storing and retrieving configuration data.
 */

/**
 * Saves a test rule to verify Properties Service
 */
function testSaveRule() {
  // Create a test rule
  const testRule = {
    id: 'test-rule-' + new Date().getTime(),
    active: true,
    description: 'Test Rule ' + new Date().toLocaleTimeString(),
    method: 'gSheet',
    source: {
      sheetUrl: 'https://docs.google.com/spreadsheets/d/1234567890/edit',
      tabName: 'TestData',
      includeHeaders: true
    },
    destination: {
      sheetUrl: SpreadsheetApp.getActiveSpreadsheet().getUrl(),
      tabName: 'ImportedData',
      handlingMode: 'clearAndReuse'
    },
    status: {
      lastRun: null,
      result: 'NEW',
      message: 'Rule has not been run yet'
    }
  };
  
  // Get current config
  const config = getConfig();
  
  // Add rule to config
  config.rules.push(testRule);
  
  // Save updated config
  saveConfig(config);
  
  // Return the test rule ID for verification
  return testRule.id;
}

/**
 * Retrieves all rules to verify they were stored correctly
 */
function testGetRules() {
  const config = getConfig();
  return config.rules;
}

/**
 * Checks if a specific rule exists by ID
 */
function testRuleExists(ruleId) {
  const config = getConfig();
  const rule = config.rules.find(r => r.id === ruleId);
  return {
    exists: !!rule,
    rule: rule || null
  };
}

/**
 * Deletes a test rule by ID
 */
function testDeleteRule(ruleId) {
  const config = getConfig();
  const initialCount = config.rules.length;
  
  config.rules = config.rules.filter(r => r.id !== ruleId);
  
  saveConfig(config);
  
  return {
    deleted: config.rules.length < initialCount,
    initialCount: initialCount,
    newCount: config.rules.length
  };
}

/**
 * Runs a full test sequence for Properties Service
 * and returns detailed results
 */
function runPropertiesTest() {
  try {
    // Step 1: Save a test rule
    const ruleId = testSaveRule();
    
    // Step 2: Verify rule exists
    const verifyResult = testRuleExists(ruleId);
    
    // Step 3: Delete the test rule
    const deleteResult = testDeleteRule(ruleId);
    
    // Step 4: Verify rule was deleted
    const finalVerify = testRuleExists(ruleId);
    
    // Return comprehensive results
    return {
      success: verifyResult.exists && deleteResult.deleted && !finalVerify.exists,
      ruleId: ruleId,
      initialSave: verifyResult.exists,
      initialRule: verifyResult.rule,
      deleteSuccess: deleteResult.deleted,
      finalExists: finalVerify.exists,
      message: verifyResult.exists && deleteResult.deleted && !finalVerify.exists
        ? "Properties Service test completed successfully. Data was stored and retrieved correctly."
        : "Properties Service test failed. See details for more information."
    };
  } catch (error) {
    return {
      success: false,
      error: error.message,
      message: "Properties Service test encountered an error: " + error.message
    };
  }
}

/**
 * Shows test results in a dialog box
 */
function showPropertiesTestResults() {
  const ui = SpreadsheetApp.getUi();
  const results = runPropertiesTest();
  
  let message;
  if (results.success) {
    message = "✅ SUCCESS: Properties Service is working correctly!\n\n" +
              "The test rule was successfully:\n" +
              "- Created and stored\n" +
              "- Retrieved with all data intact\n" +
              "- Deleted successfully\n\n" +
              "Document Properties can be used reliably for data storage.";
  } else {
    message = "❌ ERROR: Properties Service test failed.\n\n" +
              (results.error ? "Error: " + results.error + "\n\n" : "") +
              "Details:\n" +
              "- Rule created: " + (results.initialSave ? "Yes" : "No") + "\n" +
              "- Rule deleted: " + (results.deleteSuccess ? "Yes" : "No") + "\n" +
              "- Final state correct: " + (!results.finalExists ? "Yes" : "No") + "\n\n" +
              "Please check the implementation of the Properties Service functions.";
  }
  
  ui.alert("Properties Service Test Results", message, ui.ButtonSet.OK);
}

/**
 * Adds a test menu item
 */
function addTestMenuItem() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('🧪 Development Tests')
    .addItem('🔍 Test Properties Service', 'showPropertiesTestResults')
    .addToUi();
}