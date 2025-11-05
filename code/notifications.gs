/**
 * Email notification system with string-based templates
 * Handles all email communications for the system
 */

// Email template definitions
const EMAIL_TEMPLATES = {
  start: {
    subject: '[Data Ingest] Session Started - {{spreadsheetName}} - {{sessionId}}',
    body: `Data ingest session {{sessionId}} started.

Processing {{ruleCount}} active rules.
Started at: {{timestamp}}

{{rulesScheduled}}

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  },

  success: {
    subject: '[Data Ingest] Session Complete - {{successCount}}/{{ruleCount}} successful',
    body: `✅ Data ingest session {{sessionId}} completed successfully.

Summary:
- Rules processed: {{successCount}}/{{ruleCount}}
- Total rows processed: {{totalRows}}
- Execution time: {{executionTime}}
- Completed at: {{timestamp}}

{{rulesExecuted}}

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  },

  error: {
    subject: '[Data Ingest] Session Failed - {{sessionId}}',
    body: `❌ Data ingest session {{sessionId}} failed.

Error Details:
{{errorMessage}}

Summary:
- Rules processed: {{successCount}}/{{ruleCount}}
- Failed at: {{timestamp}}

{{rulesExecuted}}

Please check the logs sheet for detailed troubleshooting information.

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  },

  partial: {
    subject: '[Data Ingest] Session Partial Success - {{successCount}}/{{ruleCount}} completed',
    body: `⚠️ Data ingest session {{sessionId}} completed with partial success.

Summary:
- Successful rules: {{successCount}}/{{ruleCount}}
- Failed rules: {{errorCount}}
- Total rows processed: {{totalRows}}
- Execution time: {{executionTime}}
- Completed at: {{timestamp}}

{{rulesExecuted}}

Please check the logs sheet for details on failed rules.

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  }
};

/**
 * Get email content with variable substitution
 * Processes email templates and replaces placeholder variables
 * @param {string} type - Email template type (start, success, error, partial)
 * @param {Object} [variables={}] - Variables to substitute in template
 * @returns {Object} Email content
 * @returns {string} returns.subject - Processed email subject
 * @returns {string} returns.body - Processed email body
 * @throws {Error} If template type is not found
 */
function getEmailContent(type, variables = {}) {
  const template = EMAIL_TEMPLATES[type];
  if (!template) {
    throw new Error(`Email template not found: ${type}`);
  }

  let subject = template.subject;
  let body = template.body;

  // Simple string replacement for all variables
  Object.keys(variables).forEach(key => {
    const placeholder = `{{${key}}}`;
    const value = variables[key] || '';

    subject = subject.replace(new RegExp(placeholder, 'g'), value);
    body = body.replace(new RegExp(placeholder, 'g'), value);
  });

  return { subject, body };
}

/**
 * Send notification email
 * Sends email notification to specified recipients
 * @param {string} type - Email template type
 * @param {Array<string>} recipients - Email addresses to send to
 * @param {Object} variables - Variables for template substitution
 */
function sendNotificationEmail(type, recipients, variables) {
  if (!recipients || recipients.length === 0) {
    console.log(`No recipients configured for ${type} notification`);
    return;
  }

  try {
    const emailContent = getEmailContent(type, variables);

    GmailApp.sendEmail(
      recipients.join(','),
      emailContent.subject,
      emailContent.body
    );

    console.log(`${type} notification sent to: ${recipients.join(', ')}`);
  } catch (error) {
    console.error(`Failed to send ${type} notification:`, error.message);
  }
}

/**
 * Send session notification based on results
 * Sends appropriate notification email based on session outcome
 * @param {string} type - Notification type (start, success, error, partial)
 * @param {string} sessionId - Session identifier
 * @param {Object} data - Session data for template variables
 */
function sendSessionNotification(type, sessionId, data) {
  // Get all recipients from rules (collect unique emails)
  const recipients = getAllEmailRecipients();

  if (recipients.length === 0) {
    console.log('No email recipients configured');
    return;
  }

  const variables = {
    sessionId: sessionId,
    timestamp: formatTimestamp(new Date()),
    ...data
  };

  sendNotificationEmail(type, recipients, variables);
}

/**
 * Get direct URL to logs sheet tab
 * Generates URL that opens directly to the logs sheet tab
 * @returns {string} Direct URL to logs sheet tab
 */
function getLogsSheetUrl() {
  try {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const logsSheet = getSheet('logs');
    const baseUrl = spreadsheet.getUrl();
    const sheetId = logsSheet.getSheetId();

    // Generate direct URL to logs sheet tab
    return `${baseUrl}#gid=${sheetId}`;
  } catch (error) {
    console.error('Failed to generate logs sheet URL:', error.message);
    return SpreadsheetApp.getActiveSpreadsheet().getUrl(); // Fallback to main spreadsheet
  }
}

/**
 * Get direct URL to rules sheet tab
 * Generates URL that opens directly to the rules sheet tab
 * @returns {string} Direct URL to rules sheet tab
 */
function getRulesSheetUrl() {
  try {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const rulesSheet = getSheet('rules');
    const baseUrl = spreadsheet.getUrl();
    const sheetId = rulesSheet.getSheetId();

    // Generate direct URL to rules sheet tab
    return `${baseUrl}#gid=${sheetId}`;
  } catch (error) {
    console.error('Failed to generate rules sheet URL:', error.message);
    return SpreadsheetApp.getActiveSpreadsheet().getUrl(); // Fallback to main spreadsheet
  }
}

/**
 * Format rules for session start notification
 * Creates formatted text listing all rules scheduled for execution
 * @param {Array<Object>} rules - Array of rule objects
 * @returns {string} Formatted rules text for email
 */
function formatRulesForStart(rules) {
  if (!rules || rules.length === 0) {
    return 'No rules scheduled for execution.';
  }

  let formattedRules = 'Rules scheduled to execute:\n\n';

  rules.forEach((rule, index) => {
    formattedRules += `${index + 1}. ${rule.id} (${rule.method} method)\n`;

    // Method-specific details
    if (rule.method === 'email') {
      formattedRules += `   Gmail Query: ${rule.sourceQuery}\n`;
      if (rule.attachmentPattern) {
        formattedRules += `   Attachment Pattern: ${rule.attachmentPattern}\n`;
      }
      formattedRules += `   Gmail Search Link: ${createGmailSearchUrl(rule.sourceQuery)}\n`;
    } else if (rule.method === 'gSheet') {
      formattedRules += `   Source Sheet: ${convertSheetIdToUrl(rule.sourceQuery)}\n`;
      if (rule.sourceTab) {
        formattedRules += `   Source Tab: ${rule.sourceTab}\n`;
      } else {
        formattedRules += `   Source Tab: (first sheet - default)\n`;
      }
    } else if (rule.method === 'push') {
      formattedRules += `   Source: Current spreadsheet\n`;
    }

    formattedRules += `   Destination: ${convertSheetIdToUrl(rule.destination)}\n`;
    if (rule.destinationTab) {
      formattedRules += `   Sheet Tab: ${rule.destinationTab}\n`;
    }
    formattedRules += `   Mode: ${rule.mode}\n\n`;
  });

  return formattedRules;
}

/**
 * Format rule execution results
 * Creates formatted text showing results of rule execution
 * @param {Array<Object>} ruleResults - Array of rule execution results
 * @returns {string} Formatted results text for email
 */
function formatRulesResults(ruleResults) {
  if (!ruleResults || ruleResults.length === 0) {
    return 'No rules were executed.';
  }

  const successCount = ruleResults.filter(r => r.status === 'success').length;
  const totalCount = ruleResults.length;

  let formattedResults = `Execution Results${successCount < totalCount ? ` (${successCount}/${totalCount} successful)` : ''}:\n\n`;

  ruleResults.forEach((ruleResult, index) => {
    const { rule, result, status } = ruleResult;
    const statusIcon = status === 'success' ? '✅' : '❌';

    formattedResults += `${statusIcon} ${rule.id} (${rule.method} method)\n`;

    // Method-specific details
    if (rule.method === 'email') {
      formattedResults += `   Gmail Query: ${rule.sourceQuery}\n`;
      formattedResults += `   Gmail Search Link: ${createGmailSearchUrl(rule.sourceQuery)}\n`;

      // Email-specific details for successful processing
      if (status === 'success' && result.senderInfo) {
        formattedResults += `   Processed Email: "${result.senderInfo.subject}" from ${result.senderInfo.name} <${result.senderInfo.email}>\n`;
        formattedResults += `   Email Date: ${formatTimestamp(result.senderInfo.date)}\n`;
      }
      if (status === 'success' && result.filename) {
        formattedResults += `   Processed File: ${result.filename}\n`;
      }
    } else if (rule.method === 'gSheet') {
      const sourceUrl = convertSheetIdToUrl(rule.sourceQuery);
      formattedResults += `   Source: ${sourceUrl}\n`;

      // Add source tab information
      if (rule.sourceTab) {
        formattedResults += `   Source Tab: ${rule.sourceTab}\n`;
      } else {
        formattedResults += `   Source Tab: (first sheet - default)\n`;
      }

      // Add direct link to source tab if we have the GID
      if (status === 'success' && result.sourceSheetGid) {
        const sourceSheetId = rule.sourceQuery.includes('docs.google.com/spreadsheets')
          ? parseSheetUrl(rule.sourceQuery).sheetId
          : rule.sourceQuery;
        formattedResults += `   Direct Link: https://docs.google.com/spreadsheets/d/${sourceSheetId}/edit#gid=${result.sourceSheetGid}\n`;
      }
    } else if (rule.method === 'push') {
      formattedResults += `   Source: Current spreadsheet → ${rule.destinationTab || 'Default'} tab\n`;
    }

    formattedResults += `   Destination: ${convertSheetIdToUrl(rule.destination)}\n`;
    if (rule.destinationTab) {
      formattedResults += `   Sheet Tab: ${rule.destinationTab}\n`;
    }

    if (status === 'success') {
      formattedResults += `   Rows Processed: ${result.rowsProcessed || 0}\n`;
    } else {
      formattedResults += `   Error: ${result.error || 'Unknown error occurred'}\n`;
    }

    formattedResults += '\n';
  });

  return formattedResults;
}

/**
 * Convert sheet ID to full URL (helper function)
 * Converts sheet ID to full Google Sheets URL for display
 * @param {string} sheetId - Sheet ID or URL
 * @returns {string} Full Google Sheets URL
 */
function convertSheetIdToUrl(sheetId) {
  if (!sheetId) {
    // Handle empty destination = current spreadsheet URL
    return SpreadsheetApp.getActiveSpreadsheet().getUrl();
  }

  // If it's already a URL, return as-is
  if (sheetId.includes('docs.google.com/spreadsheets')) {
    return sheetId;
  }

  // If it's a 44-character sheet ID, convert to URL
  if (sheetId.length === 44) {
    return `https://docs.google.com/spreadsheets/d/${sheetId}/edit`;
  }

  // Fallback for unknown format
  return sheetId;
}

/**
 * Get all unique email recipients from active rules
 * Collects and validates email addresses from all active rules
 * @returns {Array<string>} Array of unique email addresses
 */
function getAllEmailRecipients() {
  try {
    const rules = getActiveRules();
    const allEmails = new Set();

    rules.forEach(rule => {
      if (rule.emailRecipients && rule.emailRecipients.trim()) {
        const emails = rule.emailRecipients.split(',').map(e => e.trim());
        emails.forEach(email => {
          if (isValidEmail(email)) {
            allEmails.add(email);
          }
        });
      }
    });

    return Array.from(allEmails);
  } catch (error) {
    console.error('Failed to get email recipients:', error.message);
    return [];
  }
}

/**
 * Send rule-specific notification
 * Sends notification email for a specific rule
 * @param {string} type - Notification type
 * @param {Object} rule - Rule configuration
 * @param {string} sessionId - Session identifier
 * @param {Object} data - Additional data for template
 */
function sendRuleNotification(type, rule, sessionId, data) {
  if (!rule.emailRecipients || !rule.emailRecipients.trim()) {
    return; // No recipients configured for this rule
  }

  const recipients = rule.emailRecipients.split(',').map(e => e.trim());
  const variables = {
    sessionId: sessionId,
    ruleId: rule.id,
    timestamp: formatTimestamp(new Date()),
    ...data
  };

  sendNotificationEmail(type, recipients, variables);
}

/**
 * Test email notification system
 * Creates test data and validates email templates
 * @function testEmailNotifications
 */
function testEmailNotifications() {
  const testRecipients = ['test@example.com']; // Update with real email for testing

  // Create test rules for demonstration
  const testRules = [
    {
      id: 'sales-import',
      method: 'email',
      sourceQuery: 'from:reports@company.com subject:"Daily Sales"',
      attachmentPattern: 'sales-.*\\.csv$',
      destination: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
      destinationTab: 'Daily Sales Data',
      mode: 'clearAndReuse'
    },
    {
      id: 'inventory-sync',
      method: 'gSheet',
      sourceQuery: '1SourceSheetId123456789012345678901234567890',
      destination: '1DestSheetId456789012345678901234567890123',
      destinationTab: 'Inventory',
      mode: 'append'
    }
  ];

  // Create test rule results for completion notifications
  const testRuleResults = [
    {
      rule: testRules[0],
      result: {
        rowsProcessed: 1250,
        senderInfo: {
          name: 'Reports Team',
          email: 'reports@company.com',
          subject: 'Daily Sales Report - 2024-01-15',
          date: new Date('2024-01-15T08:30:00')
        },
        filename: 'sales-2024-01-15.csv'
      },
      status: 'success'
    },
    {
      rule: testRules[1],
      result: {
        error: 'Source sheet not found or access denied'
      },
      status: 'error'
    }
  ];

  const testVariables = {
    sessionId: 'TEST123',
    ruleCount: 2,
    successCount: 1,
    errorCount: 1,
    totalRows: 1250,
    executionTime: '24.5 seconds',
    timestamp: formatTimestamp(new Date()),
    errorMessage: 'Test error for demonstration',
    spreadsheetName: 'Data Ingestion Control Panel',
    spreadsheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit',
    logsSheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=123456789',
    rulesSheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=987654321',
    rulesScheduled: formatRulesForStart(testRules),
    rulesExecuted: formatRulesResults(testRuleResults)
  };

  console.log('Testing email templates...');

  // Test each template
  Object.keys(EMAIL_TEMPLATES).forEach(type => {
    try {
      const content = getEmailContent(type, testVariables);
      console.log(`${type} template:`, content);
    } catch (error) {
      console.error(`Error testing ${type} template:`, error.message);
    }
  });
}