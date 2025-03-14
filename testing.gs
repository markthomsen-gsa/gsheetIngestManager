function extractSidebarDataPackage() { 


const sidebarData = getSidebarData();
console.log('Sidebar data:', JSON.stringify(sidebarData, null, 2));
}

function testEmailIngestion() { 
    // Get the rule
const config = getConfig();
const ruleId = "rule-1741965039072";
const rule = config.rules.find(r => r.id === ruleId);

// Create a log session for tracking
const sessionId = createLogSession('Testing email processing for rule: ' + rule.description);
console.log('Log session created:', sessionId);

// Process the email rule (this actually processes the attachment)
try {
  console.log('Starting to process email rule...');
  processEmailRule(rule, sessionId);
  console.log('Email rule processing completed successfully!');
} catch (error) {
  console.error('Error processing email rule:', error);
  console.error('Error stack:', error.stack);
}
}