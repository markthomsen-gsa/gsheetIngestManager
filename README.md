# Data Ingestion System - Greenfield Implementation

A modular, high-performance Google Apps Script data ingestion system designed to automate CSV data transfer between various sources (emails, Google Sheets) and destination sheets.

## 🎯 Overview

This is a complete rewrite of a legacy 5,000+ line monolithic system, reduced to ~860 lines across 7 focused modules. The system prioritizes simplicity, reliability, and performance within Google Apps Script constraints.

### Key Features

- **📧 Email CSV Processing**: Automatically process CSV attachments from Gmail
- **📊 Sheet-to-Sheet Import**: Transfer data between Google Sheets
- **🚀 Data Push Operations**: Push data from current sheet to external destinations
- **🔄 Three Processing Modes**: Clear & reuse, append, or recreate sheets
- **📝 Session-Based Logging**: Comprehensive tracking with unique session IDs
- **📬 Email Notifications**: Automated status reporting with template system
- **⚡ High Performance**: 15-30 seconds for 75K row files
- **🛡️ Robust Error Handling**: Retry logic with exponential backoff

## 🏗️ Architecture

### Modular Design (7 Files)

```
code/
├── main.gs              // Entry point, menu system, orchestration
├── config.gs            // System constants and configuration
├── logging.gs           // Session-based logging and tracking
├── emailIngest.gs       // CSV email attachment processing
├── sheetIngest.gs       // Google Sheets data import operations
├── pushIngest.gs        // Data push to external sheets
├── notifications.gs     // Email notifications with templates
└── utils.gs             // Shared utilities and validation
```

### Data Processing Limits

- **File Format**: CSV only (.csv extension)
- **Maximum File Size**: 25MB (Google Apps Script limit)
- **Maximum Rows**: 75,000 per file
- **Target Processing Time**: 15-30 seconds for max file size
- **Encoding**: UTF-8 (default)

## 🚀 Quick Start

### 1. Setup

1. **Create a new Google Apps Script project**
2. **Copy all files** from the `code/` directory to your GAS project
3. **Run the setup function**:
   ```javascript
   setupSheets()
   ```

### 2. Configuration

The system creates two sheets for configuration and logging:

#### Rules Sheet (`data-rules`)
Configure your data ingestion rules:

| Column | Field | Description | Example |
|--------|-------|-------------|---------|
| A | Rule ID | Unique identifier | `daily-sales` |
| B | Active | TRUE/FALSE | `TRUE` |
| C | Method | email/gSheet/push | `email` |
| D | Source Query | Gmail query or Sheet ID | `from:reports@company.com` |
| E | Attachment Pattern | Regex for CSV files | `sales-.*\.csv$` |
| F | Destination Sheet ID | Target sheet ID (44 chars) | `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms` |
| G | Mode | Processing mode | `clearAndReuse` |
| H | Email Recipients | Notification emails | `admin@company.com` |

#### Logs Sheet (`ingest-logs`)
Automatically tracks all operations with session correlation.

### 3. Usage

Access the system through the **📊 Data Ingest** menu:

- **🚀 Ingest Data**: Execute all active rules
- **⚙️ Initialize System**: Create required sheets
- **📋 View Logs**: Navigate to logs sheet
- **📝 View Rules**: Navigate to rules configuration

## 📋 Processing Methods

### Email Method
- Searches Gmail using query syntax
- Processes first matching CSV attachment per rule
- Supports regex patterns for file filtering

**Example Rule**:
```
Rule ID: daily-reports
Method: email
Source Query: from:accounting@company.com subject:"Daily Report"
Attachment Pattern: report-\d{4}-\d{2}-\d{2}\.csv$
```

### Google Sheets Method
- Imports data from source Google Sheets
- Uses Sheet ID for cross-spreadsheet access
- Transfers data from first sheet of source

**Example Rule**:
```
Rule ID: master-import
Method: gSheet
Source Query: 1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
```

### Push Method
- Pushes data from current active sheet
- Sends to external destination sheets
- Useful for data distribution workflows

## 🔄 Processing Modes

### clearAndReuse
- Clears destination sheet completely
- Writes new data starting from A1
- **Use case**: Replace existing data entirely

### append
- Adds data to end of existing data
- Skips header row from source
- **Use case**: Accumulate data over time

### recreate
- Deletes and recreates destination sheet
- Ensures clean slate with new structure
- **Use case**: Schema changes or fresh start

## 🛡️ Error Handling

### Retry Strategy
- **3 automatic retry attempts** with exponential backoff (1s, 2s, 4s)
- **Retryable errors**: Timeouts, rate limits, temporary issues
- **Non-retryable errors**: Invalid configurations, malformed files

### Rule Isolation
- Individual rule failures don't affect other rules
- Session continues processing remaining rules
- Detailed error logging for each rule

### Validation
- **Pre-execution validation** of all rule configurations
- **Sheet ID format validation** (44-character Google format)
- **Email address validation**
- **Regex pattern validation**
- **File size and row count validation**

## 📧 Email Notifications

### Template System
Automated notifications for:
- **Session Start**: When processing begins
- **Session Success**: All rules completed successfully
- **Session Error**: System-level failures
- **Partial Success**: Some rules failed

### Configuration
Set email recipients in the rules configuration. The system automatically:
- Collects unique email addresses from all active rules
- Sends notifications using plain text templates
- Includes session ID for log correlation

## 📊 Session Management

### Unique Session IDs
Each execution gets a unique session ID (format: `S12345678123`) for:
- **Operation correlation** across logs
- **Debugging assistance**
- **Performance tracking**
- **Email notification correlation**

### Comprehensive Logging
All operations logged to `ingest-logs` sheet:
- Session start/end times
- Rule processing status
- Error details with specific messages
- Row counts processed
- Performance metrics

## 🔧 Configuration Reference

### System Constants (`config.gs`)
```javascript
MAX_ROWS_PER_FILE = 75000       // Maximum rows per CSV
MAX_FILE_SIZE_MB = 25          // File size limit
RETRY_ATTEMPTS = 3             // Retry attempts
LOG_RETENTION_DAYS = 30        // Log cleanup period
```

### Valid Configuration Values
- **Methods**: `email`, `gSheet`, `push`
- **Modes**: `clearAndReuse`, `append`, `recreate`
- **File Extensions**: `.csv` (case-insensitive)

## 🚨 Troubleshooting

### Common Issues

**"Configuration validation failed"**
- Check required fields in `data-rules` sheet
- Verify Sheet ID format (44 characters)
- Validate email addresses and regex patterns

**"No matching CSV attachments found"**
- Verify Gmail search query syntax
- Check attachment pattern regex
- Ensure CSV files have `.csv` extension

**"Cannot access sheet with ID"**
- Verify Sheet ID is correct (44 characters)
- Check sharing permissions on destination sheet
- Ensure Sheet ID exists and is accessible

**"CSV file exceeds maximum rows"**
- File has >75K rows - split file or increase limit
- Check file format is valid CSV

### Performance Issues

**Slow Processing**
- Large files may take 15-30 seconds (normal)
- Check for very wide CSV files (many columns)
- Consider breaking large files into smaller chunks

**Timeout Errors**
- Gmail API rate limits - retry automatically handled
- Large dataset processing - files may be too large

### Debugging

1. **Check Session Logs**: Use 📋 View Logs menu item
2. **Find Session ID**: Note session ID from toast notifications
3. **Review Error Details**: Look for specific error messages in logs
4. **Test Individual Rules**: Temporarily disable other rules to isolate issues

## 🔮 Future Enhancements

### Phase 2 (Planned)
- **Excel file support** (.xlsx files)
- **Enhanced scheduling** options
- **Advanced error recovery** mechanisms

### Phase 3 (Consideration)
- **Data transformation** capabilities
- **Integration with external systems**
- **Advanced reporting and analytics**

## 📈 Performance Metrics

### Target Performance
- **Processing Speed**: 2,500-5,000 rows/second
- **File Size**: Up to 25MB CSV files
- **Execution Time**: 15-30 seconds for maximum file size
- **Success Rate**: >99% for valid configurations
- **Memory Usage**: Optimized for Google Apps Script limits

### Code Metrics
- **Total Lines**: ~860 lines (75% reduction from legacy)
- **Files**: 7 focused modules
- **Average Function Size**: <50 lines
- **Maintainability**: High - new features <100 lines

## 🤝 Contributing

This is a greenfield implementation focused on simplicity and reliability. Key principles:

- **Modular design** - single responsibility per file
- **Comprehensive error handling** - expect and handle failures gracefully
- **Clear validation** - fail fast with specific error messages
- **Performance first** - optimize for Google Apps Script constraints
- **User-friendly** - clear feedback and simple configuration

## 📄 License

This project is part of an internal data pipeline tool suite. See organization licensing for usage terms.

## 🆘 Support

For issues and questions:
1. **Check logs** using the 📋 View Logs menu
2. **Review configuration** in the 📝 View Rules sheet
3. **Validate setup** using the ⚙️ Initialize System function
4. **Test email templates** using `testEmailNotifications()` function

---

**Built with Google Apps Script** | **Optimized for Performance** | **Production Ready**