/**
 * Data Ingest Manager - Server Utilities
 * 
 * Helper functions for server-side code.
 */

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
 * Sends an email notification
 * @param {string} subject - The email subject
 * @param {string} body - The email body
 * @param {string|Array} recipients - The email recipient(s)
 */
function sendEmailNotification(subject, body, recipients) {
  // Normalize recipients to an array
  if (!Array.isArray(recipients)) {
    recipients = [recipients];
  }
  
  // Filter out empty or invalid recipients
  recipients = recipients.filter(email => email && email.includes('@'));
  
  if (recipients.length === 0) {
    // No valid recipients
    return;
  }
  
  // Send email to each recipient
  for (const email of recipients) {
    try {
      GmailApp.sendEmail(email, subject, body);
    } catch (error) {
      console.error(`Error sending email to ${email}: ${error.message}`);
    }
  }
}

/**
 * Gets the sheet ID from the URL or direct ID
 * @param {string} urlOrId - The URL or ID of the spreadsheet
 * @return {string} The spreadsheet ID
 */
function getSheetId(urlOrId) {
  if (!urlOrId) {
    throw new Error('No URL or ID provided');
  }
  
  // Check if it's just an ID
  if (/^[a-zA-Z0-9_-]+$/.test(urlOrId)) {
    return urlOrId;
  }
  
  // Try to extract from URL
  const match = urlOrId.match(/\/d\/([^\/]+)/);
  if (match && match[1]) {
    return match[1];
  }
  
  throw new Error('Invalid spreadsheet URL or ID: ' + urlOrId);
}

/**
 * Creates a timestamp string in a readable format
 * @return {string} The formatted timestamp
 */
function createTimestamp() {
  const now = new Date();
  return now.toLocaleString();
}

/**
 * Generates a unique ID
 * @return {string} A unique identifier
 */
function generateUniqueId() {
  return 'id-' + new Date().getTime() + '-' + Math.floor(Math.random() * 1000000);
}

/**
 * Validates email format
 * @param {string} email - The email to validate
 * @return {boolean} True if valid, false otherwise
 */
function isValidEmail(email) {
  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  return emailRegex.test(email);
}

/**
 * Truncates a string to the specified length and adds ellipsis if needed
 * @param {string} str - The input string
 * @param {number} maxLength - The maximum length
 * @return {string} The truncated string
 */
function truncateString(str, maxLength) {
  if (!str || str.length <= maxLength) {
    return str;
  }
  return str.substring(0, maxLength) + '...';
}

/**
 * Deep clones an object
 * @param {Object} obj - The object to clone
 * @return {Object} The cloned object
 */
function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Formats a date according to the specified format
 * @param {Date|string} date - The date to format
 * @param {string} format - The format string (optional)
 * @return {string} The formatted date
 */
function formatDate(date, format) {
  if (!date) {
    return '';
  }
  
  if (typeof date === 'string') {
    date = new Date(date);
  }
  
  if (!format) {
    // Default format: MM/DD/YYYY HH:MM:SS
    return date.toLocaleString();
  }
  
  // Simple format implementation - can be expanded as needed
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');
  
  return format
    .replace('YYYY', year)
    .replace('MM', month)
    .replace('DD', day)
    .replace('HH', hours)
    .replace('mm', minutes)
    .replace('ss', seconds);
}