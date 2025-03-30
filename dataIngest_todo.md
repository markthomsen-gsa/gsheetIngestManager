- Verification function is failing because of date comparisions. Original date is one format, final sheet is a different format because if import transforms around dates. Potentaill solutions: ignore date-like fields, prescribe a certain date format, create toggle for date verification.
- Also, the ingest verification doesn't seem to work. Says complete, but stats aren't populating. 
- Perhaps we can toggle these sheets on and offf. 
- There's signficant code bloat. Almost 5K lines of code. 
+
- Still issue with identifying if rules are enabled or not, I think.
- Headless mode might not work; old script is firing errors; need to test tihs on this version.


- Improve look-and-feel of menu.
- Format of log. No clear way to tell rows from last run. 

# Blockers
- Success reported by no data! 


===============

Explanation of Automatic Running:

As requested, the clarification is added to the script's header comment (v2.5.0 changes). Essentially:

Time-Driven Triggers: Automatic, scheduled execution relies on Google Apps Script's time-driven triggers.

Management UI: You enable/disable these triggers using the "Manage Scheduled Triggers" menu item.

How it Works: When you enable a trigger (e.g., "Run All Active Rules Daily"), you are telling Google's servers to execute the runAll() function at the specified time (e.g., daily around 1 AM) in the script's configured timezone.

No User Needed: The spreadsheet does not need to be open, and no user needs to be actively using it for the trigger to fire and the script to run.

Authorization: The user who creates the trigger must have authorized the script to perform its actions (access Sheets, Gmail, etc.). The script runs with the authority of the user who created the trigger.

This setup fulfills all your requests, providing manual control over sheet setup and trigger creation while retaining the core functionality. Remember to test the trigger management UI after implementing.

===

It's working, but takes a long time. Need to make it production ready and remove code bloat.
Sufficent features
Ideally, we'd move to adialogue box which I believe was already coded in, but so many version. 