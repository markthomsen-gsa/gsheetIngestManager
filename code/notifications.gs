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

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  },

  success: {
    subject: '[Data Ingest] Session Complete - {{successCount}}/{{ruleCount}} successful',
    body: `✅ Data ingest session {{sessionId}} completed successfully.

Results:
- Rules processed: {{successCount}}/{{ruleCount}}
- Total rows processed: {{totalRows}}
- Execution time: {{executionTime}}
- Completed at: {{timestamp}}

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

Rules processed: {{successCount}}/{{ruleCount}}
Failed at: {{timestamp}}

Please check the logs sheet for detailed information.

Spreadsheet: {{spreadsheetName}}
View spreadsheet: {{spreadsheetUrl}}
View logs: {{logsSheetUrl}}
View rules: {{rulesSheetUrl}}

This is an automated notification.`
  },

  partial: {
    subject: '[Data Ingest] Session Partial Success - {{successCount}}/{{ruleCount}} completed',
    body: `⚠️ Data ingest session {{sessionId}} completed with partial success.

Results:
- Successful rules: {{successCount}}/{{ruleCount}}
- Failed rules: {{errorCount}}
- Total rows processed: {{totalRows}}
- Execution time: {{executionTime}}
- Completed at: {{timestamp}}

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
 * Get all unique email recipients from active rules
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
 */
function testEmailNotifications() {
  const testRecipients = ['test@example.com']; // Update with real email for testing
  const testVariables = {
    sessionId: 'TEST123',
    ruleCount: 3,
    successCount: 2,
    errorCount: 1,
    totalRows: 15000,
    executionTime: '24.5 seconds',
    timestamp: formatTimestamp(new Date()),
    errorMessage: 'Test error for demonstration',
    spreadsheetName: 'Data Ingestion Control Panel',
    spreadsheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit',
    logsSheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=123456789',
    rulesSheetUrl: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit#gid=987654321'
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