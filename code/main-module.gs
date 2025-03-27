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
  ui.createMenu('🔄 Data Ingest')
    .addItem('🚀 Open Data Ingest Manager', 'showSidebar')
    .addSeparator()
    .addItem('▶️ Run All Rules', 'runAllRulesFromMenu')
    .addItem('📋 Export Configuration to Sheet', 'exportConfigFromMenu')
    .addItem('📊 Export Logs to Sheet', 'exportLogsFromMenu')
    .addSeparator()
    .addItem('🔍 Dump Properties Service Data', 'dumpPropertiesServiceToSheet')
    .addItem('📝 Create Rule Template Reference', 'createRuleTemplateSheet')
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

/**
 * Dumps all properties from the Document Properties Service to a new sheet
 * This helps with debugging and tracking the application state
 */
function dumpPropertiesServiceToSheet() {
  const ui = SpreadsheetApp.getUi();
  try {
    // Get current timestamp for sheet name
    const now = new Date();
    const timestamp = Utilities.formatDate(now, Session.getScriptTimeZone(), "yyyy-MM-dd_HH-mm-ss");
    const sheetName = `Properties_${timestamp}`;
    
    // Create a new sheet
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.insertSheet(sheetName);
    
    // Get all properties
    const docProps = PropertiesService.getDocumentProperties();
    const allProps = docProps.getProperties();
    
    // Prepare data for the sheet
    const headers = ['Property Key', 'Value', 'Type', 'Size (bytes)'];
    const rows = [headers];
    
    // Process each property
    for (const key in allProps) {
      const rawValue = allProps[key];
      let formattedValue = rawValue;
      let valueType = typeof rawValue;
      
      // Try to parse JSON values and pretty print them
      try {
        const parsed = JSON.parse(rawValue);
        if (parsed && typeof parsed === 'object') {
          formattedValue = JSON.stringify(parsed, null, 2);
          valueType = 'JSON Object';
        }
      } catch (e) {
        // Not a valid JSON, leave as is
      }
      
      // Add row for this property
      rows.push([
        key, 
        formattedValue, 
        valueType,
        rawValue ? rawValue.length : 0
      ]);
    }
    
    // Write data to sheet
    if (rows.length > 1) {
      sheet.getRange(1, 1, rows.length, headers.length).setValues(rows);
      
      // Format the sheet
      sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#f3f3f3');
      
      // Enable text wrapping for all cells in the Value column
      sheet.getRange(2, 2, rows.length - 1, 1).setWrap(true);
      
      // Set monospace font for JSON values to preserve formatting
      for (let i = 1; i < rows.length; i++) {
        if (rows[i][2] === 'JSON Object') {
          sheet.getRange(i + 1, 2).setFontFamily('Courier New');
        }
      }
      
      // Auto-size columns for better readability
      for (let i = 1; i <= headers.length; i++) {
        if (i !== 2) { // Skip auto-resize for Value column
          sheet.autoResizeColumn(i);
        }
      }
      
      // Set fixed width for Value column for better readability of JSON
      sheet.setColumnWidth(2, 600);
      
      // Add timestamp footer
      const footerRow = rows.length + 2;
      sheet.getRange(footerRow, 1, 1, 2).setValues([['Export created at:', now.toLocaleString()]]);
      
      // Add a summary row
      sheet.getRange(footerRow + 2, 1, 1, 2).setValues([['Total Properties:', rows.length - 1]]);
      
      // Show success message
      ui.alert(
        'Properties Exported',
        `Successfully exported ${rows.length - 1} properties to sheet "${sheetName}"`,
        ui.ButtonSet.OK
      );
    } else {
      // No properties found
      sheet.getRange(1, 1).setValue('No properties found in Document Properties Service');
      ui.alert('Properties Exported', 'No properties found in Document Properties Service', ui.ButtonSet.OK);
    }
    
    // Activate the new sheet
    sheet.activate();
    
  } catch (error) {
    ui.alert('Error', 'An error occurred while exporting properties: ' + error.message, ui.ButtonSet.OK);
    console.error('Error in dumpPropertiesServiceToSheet:', error);
  }
}

/**
 * Creates a template of the rules JSON object with example data for all potential fields
 * This helps users understand the structure and required fields for creating rules
 */
function createRuleTemplateSheet() {
  const ui = SpreadsheetApp.getUi();
  try {
    // Create a new sheet
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const timestamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
    const sheetName = `Rule_Template_${timestamp}`;
    const sheet = ss.insertSheet(sheetName);
    
    // Create a template rule object with example data
    const emailRuleTemplate = {
      id: "email-rule-example",
      description: "Daily Invoice Import from Accounting",
      active: true,
      method: "email",
      source: {
        emailSearch: "from:accounting@example.com subject:\"Daily Invoice Report\"",
        attachmentPattern: "*.csv",
        includeBody: false,
        maxResults: 5,
        onlyUnread: true,
        markAsRead: true,
        includeAttachments: true,
        bodyFormat: "plain",
        newestFirst: true
      },
      destination: {
        tabName: "Invoices",
        handlingMode: "appendToExisting",
        headerRow: true,
        startRow: 2,
        startCol: 1,
        includeTimestamp: true,
        timestampFormat: "yyyy-MM-dd HH:mm:ss",
        clearDestination: false
      },
      transform: {
        skipRows: 1,
        skipColumns: 0,
        mappings: [
          { source: "Invoice #", destination: "Invoice Number" },
          { source: "Amount", destination: "Total", transform: "parseFloat" },
          { source: "Date", destination: "Invoice Date", transform: "formatDate" }
        ]
      },
      status: {
        lastRun: "2023-12-15T10:30:45.000Z",
        result: "SUCCESS",
        message: "Processed 15 rows successfully"
      }
    };
    
    const sheetRuleTemplate = {
      id: "sheet-rule-example",
      description: "Monthly Sales Data Import",
      active: true,
      method: "gSheet",
      source: {
        sheetUrl: "https://docs.google.com/spreadsheets/d/1a2b3c4d5e6f7g8h9i0j/edit",
        tabName: "Sales Data",
        range: "A1:F100",
        includeHeaders: true,
        namedRange: "SalesData2023"
      },
      destination: {
        tabName: "Sales Summary",
        handlingMode: "clearAndReuse",
        headerRow: true,
        startRow: 1,
        startCol: 1,
        includeTimestamp: true,
        timestampFormat: "yyyy-MM-dd HH:mm:ss",
        clearDestination: true
      },
      transform: {
        skipRows: 0,
        skipColumns: 0,
        mappings: [
          { source: "Date", destination: "Sales Date" },
          { source: "Product ID", destination: "Product Code" },
          { source: "Amount", destination: "Revenue", transform: "parseFloat" }
        ]
      },
      schedule: {
        frequency: "monthly",
        dayOfMonth: "1",
        time: "09:00"
      },
      status: {
        lastRun: "2023-11-01T09:00:12.000Z",
        result: "SUCCESS",
        message: "Imported 42 rows successfully"
      }
    };
    
    // Create the section with email rule template
    sheet.getRange("A1").setValue("Email Rule Template").setFontWeight("bold").setFontSize(14);
    sheet.getRange("A2").setValue("This template shows all possible fields for a rule that imports data from email attachments.");
    sheet.getRange("A3").setValue("Example JSON Structure:").setFontWeight("bold");
    
    // Format and add the email rule template
    const emailRuleJson = JSON.stringify(emailRuleTemplate, null, 2);
    const emailRuleRange = sheet.getRange("A4");
    emailRuleRange.setValue(emailRuleJson);
    emailRuleRange.setFontFamily("Courier New").setWrap(true);
    
    // Add field descriptions
    sheet.getRange("A" + (emailRuleRange.getLastRow() + 3)).setValue("Field Descriptions:").setFontWeight("bold");
    
    const emailFieldDescriptions = [
      ["id", "Unique identifier for the rule", "String", "Required"],
      ["description", "Human-readable description of what the rule does", "String", "Required"],
      ["active", "Whether the rule is active and should be run", "Boolean", "Optional (defaults to true)"],
      ["method", "Data source method - must be 'email' for email rules", "String", "Required"],
      ["source.emailSearch", "Gmail search query to find messages", "String", "Required"],
      ["source.attachmentPattern", "Pattern to match attachment filenames (e.g., '*.csv')", "String", "Required if includeAttachments is true"],
      ["source.includeBody", "Whether to include the email body", "Boolean", "Optional"],
      ["source.maxResults", "Maximum number of emails to process", "Number", "Optional"],
      ["source.onlyUnread", "Only process unread emails", "Boolean", "Optional"],
      ["source.markAsRead", "Mark emails as read after processing", "Boolean", "Optional"],
      ["source.includeAttachments", "Whether to process email attachments", "Boolean", "Optional (defaults to true)"],
      ["source.bodyFormat", "Format of email body ('plain' or 'html')", "String", "Optional"],
      ["destination.tabName", "Name of the sheet tab where data will be written", "String", "Required"],
      ["destination.handlingMode", "How to handle existing data ('appendToExisting', 'clearAndReuse')", "String", "Optional"],
      ["destination.headerRow", "Whether the first row contains headers", "Boolean", "Optional"],
      ["destination.startRow", "Starting row for data import", "Number", "Optional"],
      ["destination.startCol", "Starting column for data import", "Number", "Optional"],
      ["destination.includeTimestamp", "Add timestamp column to imported data", "Boolean", "Optional"],
      ["transform.mappings", "Column mappings from source to destination", "Array", "Optional"]
    ];
    
    // Create descriptions table
    const descStartRow = emailRuleRange.getLastRow() + 4;
    sheet.getRange(descStartRow, 1, 1, 4).setValues([["Field", "Description", "Type", "Required/Optional"]]).setFontWeight("bold").setBackground("#f3f3f3");
    sheet.getRange(descStartRow + 1, 1, emailFieldDescriptions.length, 4).setValues(emailFieldDescriptions);
    
    // Create the section with Google Sheet rule template
    const sheetSectionStartRow = descStartRow + emailFieldDescriptions.length + 4;
    sheet.getRange(sheetSectionStartRow, 1).setValue("Google Sheet Rule Template").setFontWeight("bold").setFontSize(14);
    sheet.getRange(sheetSectionStartRow + 1, 1).setValue("This template shows all possible fields for a rule that imports data from another Google Sheet.");
    sheet.getRange(sheetSectionStartRow + 2, 1).setValue("Example JSON Structure:").setFontWeight("bold");
    
    // Format and add the sheet rule template
    const sheetRuleJson = JSON.stringify(sheetRuleTemplate, null, 2);
    const sheetRuleRange = sheet.getRange(sheetSectionStartRow + 3, 1);
    sheetRuleRange.setValue(sheetRuleJson);
    sheetRuleRange.setFontFamily("Courier New").setWrap(true);
    
    // Add field descriptions for sheet-specific fields
    sheet.getRange(sheetSectionStartRow + sheetRuleRange.getLastRow() + 3, 1).setValue("Additional Fields for Google Sheet Rules:").setFontWeight("bold");
    
    const sheetFieldDescriptions = [
      ["source.sheetUrl", "URL of the source Google Sheet", "String", "Required"],
      ["source.tabName", "Name of the tab in the source sheet", "String", "Required"],
      ["source.range", "Range of cells to import (e.g., 'A1:F100')", "String", "Optional"],
      ["source.includeHeaders", "Whether the first row contains headers", "Boolean", "Optional"],
      ["source.namedRange", "Named range to import instead of range address", "String", "Optional"],
      ["schedule.frequency", "How often to run the rule ('hourly', 'daily', 'weekly', 'monthly')", "String", "Optional"],
      ["schedule.dayOfMonth", "Day of month to run (for monthly schedule)", "String", "Optional"],
      ["schedule.dayOfWeek", "Day of week to run (for weekly schedule)", "String", "Optional"],
      ["schedule.time", "Time of day to run (for daily, weekly, monthly)", "String", "Optional"]
    ];
    
    // Create descriptions table for Sheet-specific fields
    const sheetDescStartRow = sheetSectionStartRow + sheetRuleRange.getLastRow() + 4;
    sheet.getRange(sheetDescStartRow, 1, 1, 4).setValues([["Field", "Description", "Type", "Required/Optional"]]).setFontWeight("bold").setBackground("#f3f3f3");
    sheet.getRange(sheetDescStartRow + 1, 1, sheetFieldDescriptions.length, 4).setValues(sheetFieldDescriptions);
    
    // Format the entire sheet
    sheet.getRange(1, 1, sheet.getLastRow(), 4).setBorder(true, true, true, true, true, true);
    sheet.setColumnWidth(1, 300);
    sheet.setColumnWidth(2, 400);
    sheet.setColumnWidth(3, 100);
    sheet.setColumnWidth(4, 150);
    
    // Add a usage note
    const usageNoteRow = sheetDescStartRow + sheetFieldDescriptions.length + 3;
    sheet.getRange(usageNoteRow, 1, 1, 4).merge().setValue("Usage Note: This template is for reference only. When creating rules through the Data Ingest Manager UI, many of these fields will be automatically populated or provided as options.")
      .setFontStyle("italic").setBackground("#e6f7ff");
    
    // Activate the new sheet
    sheet.activate();
    
    // Show success message
    ui.alert(
      'Template Created',
      `Rule template sheet "${sheetName}" has been created successfully.`,
      ui.ButtonSet.OK
    );
    
  } catch (error) {
    ui.alert('Error', 'An error occurred while creating the rule template: ' + error.message, ui.ButtonSet.OK);
    console.error('Error in createRuleTemplateSheet:', error);
  }
}