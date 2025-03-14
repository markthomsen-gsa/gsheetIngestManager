/**
 * Data Ingest Manager - Main Module
 * 
 * Provides core functionality for the Data Ingest Manager sidebar application.
 * This module contains the onOpen trigger and main sidebar display functions.
 */

/**
 * Creates menu items when the spreadsheet is opened
 */

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Data Ingest')
    .addItem('Open Data Ingest Manager', 'showSidebar')
    .addSeparator()
    .addItem('Run All Rules', 'runAllRulesFromMenu')
    .addItem('Export Configuration to Sheet', 'exportConfigFromMenu')
    .addItem('Export Logs to Sheet', 'exportLogsFromMenu')
    .addToUi();
}


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
 * Handles running all rules from the menu (without UI)
 */
function runAllRulesFromMenu() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    'Run All Rules',
    'Are you sure you want to run all active rules?',
    ui.ButtonSet.YES_NO
  );
  
  if (response === ui.Button.YES) {
    try {
      const result = runAllRules();
      ui.alert(
        'Execution Complete',
        `Processed ${result.successCount} rule(s) successfully.\n` +
        `Failed: ${result.errorCount}`,
        ui.ButtonSet.OK
      );
    } catch (error) {
      ui.alert('Error', 'An error occurred: ' + error.message, ui.ButtonSet.OK);
    }
  }
}

/**
 * Runs all active rules
 * @return {Object} Execution results with success and error counts
 */
function runAllRules() {
  const config = getConfig();
  const activeRules = config.rules.filter(rule => rule.active !== false);
  return runRules(activeRules.map(rule => rule.id));
}

/**
 * Exports configuration to a sheet when called from menu
 */
function exportConfigFromMenu() {
  const ui = SpreadsheetApp.getUi();
  
  try {
    const result = exportConfigToSheet();
    
    if (result.success) {
      ui.alert(
        'Export Complete',
        'Configuration has been exported to the "Config Export" sheet.',
        ui.ButtonSet.OK
      );
    } else {
      ui.alert('Error', result.message, ui.ButtonSet.OK);
    }
  } catch (error) {
    ui.alert('Error', 'An error occurred: ' + error.message, ui.ButtonSet.OK);
  }
}

/**
 * Exports logs to a sheet when called from menu
 */
function exportLogsFromMenu() {
  const ui = SpreadsheetApp.getUi();
  
  try {
    const result = exportLogsToSheet();
    
    if (result.success) {
      ui.alert(
        'Export Complete',
        'Logs have been exported to the "Logs Export" sheet.',
        ui.ButtonSet.OK
      );
    } else {
      ui.alert('Error', result.message, ui.ButtonSet.OK);
    }
  } catch (error) {
    ui.alert('Error', 'An error occurred: ' + error.message, ui.ButtonSet.OK);
  }
}