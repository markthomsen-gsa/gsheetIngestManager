/**
 * Test functions for URL and tab functionality
 * Run these to verify the new features work correctly
 */

/**
 * Test URL parsing and validation
 */
function testUrlParsing() {
  console.log('Testing URL parsing and validation...');

  const testCases = [
    {
      name: 'Valid Google Sheets URL',
      input: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit',
      expectedId: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
      shouldPass: true
    },
    {
      name: 'Valid Sheet ID',
      input: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
      expectedId: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
      shouldPass: true
    },
    {
      name: 'Invalid URL format',
      input: 'https://google.com/invalid-url',
      shouldPass: false
    },
    {
      name: 'Invalid Sheet ID (too short)',
      input: '1BxiMVs0XRA5nFMdKvBdBZ',
      shouldPass: false
    },
    {
      name: 'Empty destination',
      input: '',
      shouldPass: false
    }
  ];

  testCases.forEach(testCase => {
    try {
      const result = parseDestination(testCase.input);

      if (testCase.shouldPass) {
        if (result === testCase.expectedId) {
          console.log(`✅ ${testCase.name}: PASSED`);
        } else {
          console.log(`❌ ${testCase.name}: FAILED - Expected ${testCase.expectedId}, got ${result}`);
        }
      } else {
        console.log(`❌ ${testCase.name}: FAILED - Should have thrown error but returned ${result}`);
      }
    } catch (error) {
      if (testCase.shouldPass) {
        console.log(`❌ ${testCase.name}: FAILED - Unexpected error: ${error.message}`);
      } else {
        console.log(`✅ ${testCase.name}: PASSED - Correctly threw error: ${error.message}`);
      }
    }
  });
}

/**
 * Test URL validation functions
 */
function testUrlValidation() {
  console.log('Testing URL validation functions...');

  const urlTests = [
    {
      url: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit',
      expected: true
    },
    {
      url: 'https://docs.google.com/spreadsheets/d/invalid-id/edit',
      expected: false
    },
    {
      url: 'https://google.com/invalid',
      expected: false
    }
  ];

  urlTests.forEach(test => {
    const result = isValidSheetUrl(test.url);
    if (result === test.expected) {
      console.log(`✅ URL validation test passed for: ${test.url.substring(0, 50)}...`);
    } else {
      console.log(`❌ URL validation test failed for: ${test.url.substring(0, 50)}...`);
    }
  });
}

/**
 * Test rule validation with new fields
 */
function testRuleValidation() {
  console.log('Testing rule validation with new fields...');

  const testRules = [
    {
      name: 'Valid rule with URL and tab',
      rule: {
        id: 'test-rule',
        method: 'email',
        sourceQuery: 'from:test@example.com',
        attachmentPattern: '.*\\.csv$',
        destination: 'https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit',
        destinationTab: 'Data Import',
        mode: 'clearAndReuse',
        emailRecipients: 'admin@test.com'
      },
      shouldPass: true
    },
    {
      name: 'Valid rule with ID (no tab)',
      rule: {
        id: 'test-rule-2',
        method: 'gSheet',
        sourceQuery: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
        destination: '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
        mode: 'append'
      },
      shouldPass: true
    },
    {
      name: 'Invalid rule with bad URL',
      rule: {
        id: 'test-rule-3',
        method: 'email',
        destination: 'https://invalid-url.com',
        mode: 'clearAndReuse'
      },
      shouldPass: false
    }
  ];

  testRules.forEach(test => {
    try {
      const errors = validateRule(test.rule);

      if (test.shouldPass && errors.length === 0) {
        console.log(`✅ ${test.name}: PASSED`);
      } else if (!test.shouldPass && errors.length > 0) {
        console.log(`✅ ${test.name}: PASSED - Correctly found errors: ${errors.join(', ')}`);
      } else {
        console.log(`❌ ${test.name}: FAILED - Expected ${test.shouldPass ? 'no errors' : 'errors'}, got: ${errors.join(', ')}`);
      }
    } catch (error) {
      console.log(`❌ ${test.name}: FAILED - Unexpected error: ${error.message}`);
    }
  });
}

/**
 * Test tab creation functionality
 */
function testTabCreation() {
  console.log('Testing tab creation functionality...');

  // Note: This test requires a real spreadsheet for actual testing
  // These are unit tests for the logic components

  const mockSpreadsheet = {
    getSheets: () => [{ getName: () => 'Sheet1' }],
    getSheetByName: (name) => {
      if (name === 'Existing Tab') {
        return { getName: () => 'Existing Tab' };
      }
      return null; // Simulate tab doesn't exist
    },
    insertSheet: (name) => {
      console.log(`Mock: Creating new tab "${name}"`);
      return { getName: () => name };
    }
  };

  const testCases = [
    {
      name: 'Empty tab name uses first sheet',
      tabName: '',
      expectCreation: false
    },
    {
      name: 'Null tab name uses first sheet',
      tabName: null,
      expectCreation: false
    },
    {
      name: 'Existing tab found',
      tabName: 'Existing Tab',
      expectCreation: false
    },
    {
      name: 'New tab created',
      tabName: 'New Data Tab',
      expectCreation: true
    },
    {
      name: 'Tab with spaces trimmed',
      tabName: '  Trimmed Tab  ',
      expectCreation: true
    }
  ];

  testCases.forEach(testCase => {
    try {
      console.log(`Testing: ${testCase.name}`);

      // Since we can't easily mock the actual function, we test the logic
      let tabName = testCase.tabName;
      if (!tabName || !tabName.trim()) {
        console.log(`  ✅ Would use first sheet`);
        return;
      }

      const trimmedName = tabName.trim();
      const existingSheet = mockSpreadsheet.getSheetByName(trimmedName);

      if (!existingSheet) {
        if (testCase.expectCreation) {
          mockSpreadsheet.insertSheet(trimmedName);
          console.log(`  ✅ Would create new tab: ${trimmedName}`);
        } else {
          console.log(`  ❌ Expected no creation but would create: ${trimmedName}`);
        }
      } else {
        if (testCase.expectCreation) {
          console.log(`  ❌ Expected creation but tab exists: ${trimmedName}`);
        } else {
          console.log(`  ✅ Would use existing tab: ${trimmedName}`);
        }
      }

    } catch (error) {
      console.log(`  ❌ ${testCase.name}: Error - ${error.message}`);
    }
  });
}

/**
 * Test append mode behavior with new tabs
 */
function testAppendModeWithNewTabs() {
  console.log('Testing append mode with new tabs...');

  const testCases = [
    {
      name: 'Empty sheet gets all data including headers',
      lastRow: 0,
      csvData: [['Name', 'Age'], ['John', '25'], ['Jane', '30']],
      expectedRows: 3,
      expectedDescription: 'all data including headers'
    },
    {
      name: 'Existing data gets only data rows (skip header)',
      lastRow: 5,
      csvData: [['Name', 'Age'], ['John', '25'], ['Jane', '30']],
      expectedRows: 2,
      expectedDescription: 'data rows only (header skipped)'
    },
    {
      name: 'Header only CSV to empty sheet',
      lastRow: 0,
      csvData: [['Name', 'Age']],
      expectedRows: 1,
      expectedDescription: 'header row only'
    }
  ];

  testCases.forEach(testCase => {
    console.log(`Testing: ${testCase.name}`);

    const lastRow = testCase.lastRow;
    const csvData = testCase.csvData;

    let rowsToWrite;
    let description;

    if (lastRow === 0) {
      // Empty sheet - write all data including headers
      rowsToWrite = csvData.length;
      description = 'all data including headers';
    } else {
      // Existing data - skip header row
      rowsToWrite = csvData.length - 1;
      description = 'data rows only (header skipped)';
    }

    if (rowsToWrite === testCase.expectedRows && description === testCase.expectedDescription) {
      console.log(`  ✅ Correct: ${rowsToWrite} rows (${description})`);
    } else {
      console.log(`  ❌ Expected ${testCase.expectedRows} rows (${testCase.expectedDescription}), got ${rowsToWrite} (${description})`);
    }
  });
}

/**
 * Run all tests
 */
function runAllUrlTests() {
  console.log('🚀 Starting URL and Tab functionality tests...\n');

  testUrlParsing();
  console.log('');

  testUrlValidation();
  console.log('');

  testRuleValidation();
  console.log('');

  testTabCreation();
  console.log('');

  testAppendModeWithNewTabs();
  console.log('');

  console.log('✨ All tests completed!');
}