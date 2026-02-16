# Requirements Specification

Sections 1–3 state **testable requirements** (behavior and quality attributes). Section 4 captures **design and implementation choices** that may evolve without changing the requirements.

## 1. Functional Requirements

### FR-1: Data Ingestion

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1.1 | System SHALL ingest CSV data from Gmail attachments matching a configurable search query and filename pattern (regex) | Must |
| FR-1.2 | System SHALL ingest data from external Google Sheets by URL, spreadsheet ID, or GID, optionally targeting a specific tab | Must |
| FR-1.4 | System SHALL support three write modes: **clearAndReuse** (replace content, keep sheet), **append** (add rows), **recreate** (delete and rebuild sheet) | Must |
| FR-1.3 | System SHALL support dynamic sheet naming using configurable patterns with placeholders (e.g. `Data_{{timestamp}}`, `Import_{{YYYY-MM-DD}}`, `{{source}}_{{HH-mm}}`) where placeholders are resolved at runtime to create timestamped or contextual sheet names | Must |
| FR-1.5 | System SHALL validate each active ingest rule before execution (required fields, valid method, accessible source) | Must |
| FR-1.6 | System SHALL record the result, timestamp, and row/column dimensions of each ingest run in the rule's config row | Must |
| FR-1.7 | System SHALL continue processing remaining rules when an individual rule fails | Must |
| FR-1.8 | System SHALL support a configurable validation formula per rule, evaluated after ingest, with PASS/FAIL/WARN color coding | Should |
| FR-1.9 | System SHALL enforce configurable limits to stay within Google Apps Script quotas (default: max 75,000 rows per file, max 25MB attachment size) | Must |
| FR-1.10 | System SHALL ingest data via API (configurable URL, method, and optional headers); response SHALL be parseable as CSV or structured data for sheet write | Must |
| FR-1.11 | System SHALL support ingesting multiple attachments from a single email when the Gmail source matches one message (e.g. all attachments matching a filename pattern, or all attachments up to a limit) | Must |
| FR-1.12 | System SHALL ingest document(s) from Google Drive using a configurable search query; the search MAY return a single file or multiple files, and the rule SHALL support ingesting the result set (one or many) into the target sheet(s) | Must |
| FR-1.13 | When a Drive search returns multiple files, the system SHALL support selecting a single file to ingest by a configurable selection rule (e.g. latest by modified date, latest by created date, or filename/regex pattern matching one of the results) | Must |
| FR-1.14 | System SHALL ingest Excel files (.xlsx, .xls) from Gmail attachments, Drive, or API; support targeting a specific sheet/tab by name or index and apply the same write modes and row limits as CSV ingest | Must |
| FR-1.15 | System SHALL support processing password-protected files (e.g. Excel or CSV) when the rule supplies a password (stored in ScriptProperties or a secure config cell); decryption SHALL occur in memory and the password SHALL NOT be logged | Should |
| FR-1.16 | System SHALL support API ingest where the request (URL, query parameters, or body) is constructed from a designated source sheet or from placeholders resolved against pipeline/sheet data, so that API calls can consume the output of prior pipeline steps; response SHALL be parseable as CSV or structured data for sheet write | Must |
| FR-1.17 | When ingesting from a sheet source (Google Sheet URL/ID/GID or sheet within the workbook), the system SHALL allow the rule to restrict what is ingested: (1) a configurable maximum row count (e.g. first 20 rows), and (2) a subset of columns specified by header name (comma-separated or equivalent); only the specified columns SHALL be read from the source and written to the target | Must |
| FR-1.18 | System SHALL support ingest via built-in import formulas (e.g. `IMPORTHTML`, `IMPORTDATA`) as an ingest method: rule specifies URL and formula type/parameters; the system writes the formula (or its evaluated result) into the target sheet per the rule's write mode | Must |
| FR-1.19 | System SHALL allow each ingest rule to specify whether target tab(s) are hidden or visible after ingest; the rule SHALL set the sheet's visibility (hidden/visible) accordingly when creating or writing to the target | Must |

### FR-2: Data Transformation (Formula Enhancement)

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-2.1 | System SHALL apply formula templates to target sheets, resolving placeholders: `{{col:HeaderName}}` → column letter, `{{row}}` → row number, `{{sheet}}` → sheet name | Must |
| FR-2.2 | System SHALL support two application modes: **overwrite** (replace column values) and **append** (add new column) | Must |
| FR-2.3 | System SHALL support targeting specific sheets by name (comma-separated) or all sheets (`*`) | Must |
| FR-2.4 | System SHALL support formula output formats: text, number, date | Should |
| FR-2.5 | System SHALL skip disabled transform rules without error | Must |
| FR-2.6 | System SHALL support placeholders that resolve to another sheet name or a named range (e.g. `{{sheet:Lookup}}`, `{{range:PriceTable}}`) so formula templates can reference lookup tables or other sheets in the same workbook | Should |
| FR-2.7 | System SHALL allow each transform rule to choose whether to write **formulas** (cells remain live and recalc) or **values only** (paste computed results) | Should |
| FR-2.8 | System SHALL support applying a transform rule to a specific range (e.g. rows 2–500, or columns C–F) instead of the entire sheet | Should |
| FR-2.9 | System SHALL support applying the formula template only within the current data extent (e.g. last row with content in a key column) so empty rows are left untouched | Should |
| FR-2.10 | System SHALL support column-level overwrite: target a single column (by header name or letter), apply a formula template to that column, and replace its values in place (for correction or derivation); output SHALL be formula or values per FR-2.7 | Must |
| FR-2.11 | System SHALL support sequencing of field (column) creation: transform rules SHALL be executable in a defined order so that dependent columns are created only after their prerequisite columns exist | Must |
| FR-2.12 | System SHALL allow each transform rule to declare prerequisite column(s) (e.g. by header name); a rule SHALL NOT be applied until all declared prerequisites are present and populated (or the rule SHALL be skipped with a logged reason) | Must |
| FR-2.13 | System SHALL allow formulas to be applied to multiple sheets by either: (1) comma-separated sheet names in the rule target, or (2) pattern tags (e.g. wildcards such as `Data_*`, `*_Import`, or configurable regex/pattern matching) that resolve to a set of sheet names at runtime | Must |

### FR-3: Data Distribution

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-3.1 | System SHALL execute QUERY formulas with placeholder substitution and write results to destination tabs | Must |
| FR-3.2 | System SHALL support distribution to tabs within the current spreadsheet or external spreadsheets (by URL/ID) | Must |
| FR-3.3 | System SHALL support overwrite mode (clear destination before writing) per rule | Must |
| FR-3.4 | System SHALL capture execution metrics per rule: runtime, row count, column count | Must |
| FR-3.5 | System SHALL support an optional validation formula per distribution rule, with color-coded results | Should |
| FR-3.6 | System SHALL optionally generate metrics entries in a dedicated metrics sheet | Should |

### FR-4: Workflow Orchestration

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-4.0 | Stage names and count SHALL be defined by workflow/orchestrator configuration (not fixed in code); ingest, transform, and distribute are examples only. | Must |
| FR-4.1 | System SHALL execute multi-step workflows defined in a config sheet, where each step maps to a pipeline stage entry point | Must |
| FR-4.2 | System SHALL support configurable delay (in seconds) between workflow steps | Must |
| FR-4.3 | System SHALL queue multiple workflows and execute them sequentially | Must |
| FR-4.4 | System SHALL track workflow state: IDLE → QUEUED → RUNNING → COMPLETED / FAILED | Must |
| FR-4.5 | System SHALL support continuing workflow execution across steps that exceed the 6-minute execution limit | Must |
| FR-4.6 | System SHALL clean up triggers on workflow completion or failure | Must |
| FR-4.7 | System SHALL support a "force reset" that clears all workflow state and deletes all triggers | Must |
| FR-4.8 | System SHALL automatically start the next queued workflow when the current one completes | Must |
| FR-4.9 | System SHALL support configurable halt conditions (e.g. any rule failed, any validation FAIL) evaluated after a stage; when met, the workflow SHALL stop and clean up triggers without running subsequent steps | Must |
| FR-4.10 | When workflow status is RUNNING, system SHALL record a **phase** (substage) indicating current activity: within a stage (e.g. initializing, running rules, validating, persisting) or between stages (evaluating halt, scheduling next, waiting for trigger), so that dashboards and APIs can show context-relevant progress | Must |
| FR-4.11 | When status is RUNNING, system SHALL expose a phase (substage) so dashboards/APIs can show current activity; phase SHALL be absent when not RUNNING | Must |
| FR-4.12 | Pipeline execution SHALL run headless: runs SHALL complete without requiring user interaction (no dialogs, no spreadsheet open). Execution MAY be started by time-based trigger or API and SHALL proceed to completion without user presence | Must |
| FR-4.13 | System SHALL support scheduling pipeline runs (e.g. ScriptApp time-based trigger invoking the run-entry function, or external scheduler calling the Web App API to start a run) | Must |
| FR-4.14 | System SHALL support executing a single functional stage (e.g. ingest, transform, distribute — as defined in orchestrator config) once, standalone, without starting or being part of a full pipeline workflow; the run SHALL execute that stage's rules one time and then complete (no subsequent stages, no workflow state). Menu, API, and extension SHALL offer this single-stage run in addition to full workflow execution | Must |

### FR-5: Logging & Observability

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-5.1 | System SHALL write structured log entries to a unified pipeline-log sheet with: session ID, timestamp, stage, level, rule name, status, message, duration, dimensions, error details, metadata | Must |
| FR-5.2 | System SHALL support log levels: DEBUG, INFO, WARN, ERROR, SUCCESS | Must |
| FR-5.3 | System SHALL support logging presets: minimal (errors + success only), normal, verbose (includes debug + console output) | Must |
| FR-5.4 | System SHALL emit log entries at least for: workflow and stage lifecycle, rule execution (start and outcome), validation results, trigger creation/removal, API requests, and errors (with category/code per FR-8); which events to log and required fields per event are defined in design | Must |
| FR-5.5 | System SHALL correlate all log entries within a pipeline run via a shared session ID | Must |
| FR-5.6 | System SHALL build and maintain a health dashboard showing: overall status KPI, last run per stage, workflow status, recent errors (last 20), recent activity (last 30) | Must |
| FR-5.7 | System SHALL color-code the dashboard status: green (OK), orange (Stale >24h or errors present) | Should |
| FR-5.8 | System SHALL support dumping scheduled triggers and PropertiesService contents to the log for diagnostics | Should |
| FR-5.9 | Every error log entry SHALL contain useful diagnostic information; there SHALL be no "unknown" or empty error entries. At minimum, each error entry SHALL include a non-empty message and either a category/code (per FR-8) or a stack trace (or equivalent dump), so that every error is debuggable | Must |

### FR-6: Configuration Management

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-6.1 | System SHALL store all user-editable configuration in dedicated spreadsheet sheets with defined schemas | Must |
| FR-6.2 | System SHALL minimize input errors for boolean and enumerated config fields | Must |
| FR-6.3 | System SHALL auto-create config sheets with headers, formatting, and validation on system initialization | Must |
| FR-6.4 | System SHALL support navigation to any config sheet from the menu | Should |

### FR-7: User Interface

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-7.1 | System SHALL present a hierarchical menu in the spreadsheet menu bar on open | Must |
| FR-7.2 | System SHALL provide toast notifications for operation start/completion/failure | Must |
| FR-7.3 | System SHALL prompt for confirmation before destructive operations (force reset, clear logs) | Must |
| FR-7.4 | System SHALL provide a help menu item explaining all available operations | Should |
| FR-7.5 | System SHOULD provide a sidebar UI for richer monitoring and control (new in refactor) | Should |

### FR-8: Error Handling

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-8.1 | System SHALL retry transient errors (quota exceeded, network timeout) with exponential backoff (1s → 2s → 4s), up to 3 attempts | Must |
| FR-8.2 | System SHALL classify errors by category: validation, source, processing, destination, system | Must |
| FR-8.3 | System SHALL assign standardized error codes (e.g., VAL-1001, SRC-2001) | Must |
| FR-8.4 | Callers SHALL receive a consistent structure indicating success/failure and metadata for every operation | Must |
| FR-8.5 | System SHALL update rule status cells with color-coded results (green=pass, red=fail) | Should |
| FR-8.6 | System SHALL optionally send email notifications on pipeline completion with summary | Could |

### FR-9: Web App API

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-9.1 | System SHALL expose pipeline operations via Web App endpoints (doGet/doPost) | Must |
| FR-9.2 | System SHALL authenticate API requests using API keys | Must |
| FR-9.3 | System SHALL return all API responses in a standard JSON envelope with success, data, meta, and error | Must |
| FR-9.4 | System SHALL expose read endpoints for all config sheets (e.g. rules per stage, workflows); stage-specific config SHALL be driven by orchestrator config, not a fixed list | Must |
| FR-9.5 | System SHALL expose write endpoints for config mutations (create, update, toggle rules) with validation | Must |
| FR-9.6 | System SHALL expose pipeline execution endpoints (run per stage, start/stop workflow); stages SHALL be those defined in config (e.g. ingest, transform, distribute), not hardcoded | Must |
| FR-9.7 | System SHALL expose monitoring endpoints (health status, last runs per stage, recent errors) | Must |
| FR-9.8 | System SHALL implement rate limiting (max 60 requests per minute per API key) | Should |
| FR-9.9 | System SHALL log all API requests to pipeline-log with action, timestamp, and result | Should |
| FR-9.10 | System SHALL provide menu items for API key management: generate, list client names, revoke | Must |
| FR-9.11 | System SHALL validate all API request parameters and return descriptive error codes for invalid input | Must |

### FR-10: Chrome Extension

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-10.1 | Extension SHALL provide a popup UI for pipeline status display and quick actions (run per configured stage, run all workflows); stage actions SHALL reflect orchestrator config, not a fixed set | Must |
| FR-10.2 | Extension SHALL display current pipeline health: status (OK/Stale/Issues), last run time, error counts (24h/7d) | Must |
| FR-10.3 | Extension SHALL persist only API endpoint URL and API key locally (no server config caching) | Must |
| FR-10.4 | Extension SHALL poll the `status.health` endpoint every 30 seconds while the popup is open | Should |
| FR-10.5 | Extension SHALL provide a settings page for API URL/key configuration with a "Test Connection" button | Must |
| FR-10.6 | Extension SHALL update the browser badge icon to reflect pipeline status (green=OK, red=errors, blue=running) | Should |
| FR-10.7 | Extension SHALL display the active workflow name when a workflow is running | Should |
| FR-10.8 | Extension SHALL handle API errors gracefully with user-friendly messages (not raw error objects) | Must |

### FR-11: Spreadsheet Stewardship

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-11.1 | The system SHALL remove or clear unused cells beyond the written data extent after every pipeline write to any sheet (all stages). Unused rows and columns SHALL NOT be left in place, so the spreadsheet does not accumulate toward the per-spreadsheet cell limit | Must |

---

## 2. Non-Functional Requirements

### NFR-1: Performance

| ID | Requirement |
|----|-------------|
| NFR-1.1 | Each pipeline stage SHALL complete within the GAS 6-minute execution limit |
| NFR-1.2 | Sheet I/O SHALL be batched (single `getValues()`/`setValues()` per operation where possible) |
| NFR-1.3 | Logger SHALL buffer writes and flush in batches to minimize sheet API calls |
| NFR-1.4 | System SHALL process up to 10,000 rows per batch without timeout |

### NFR-2: Reliability

| ID | Requirement |
|----|-------------|
| NFR-2.1 | Individual rule failures SHALL NOT prevent other rules from executing |
| NFR-2.2 | Workflow state SHALL persist across process restarts (e.g. trigger failures) |
| NFR-2.3 | Orphaned triggers SHALL be detectable and cleanable |
| NFR-2.4 | System SHALL be recoverable from any stuck state via force reset |

### NFR-3: Maintainability

| ID | Requirement |
|----|-------------|
| NFR-3.1 | Domain logic SHALL have zero dependencies on GAS APIs (testable in isolation) |
| NFR-3.2 | No source file SHALL exceed 50KB |
| NFR-3.3 | Code SHALL be organized by architectural layer (presentation / application / domain / infrastructure) |
| NFR-3.4 | All GAS service interactions SHALL go through adapter interfaces |

### NFR-4: Security

| ID | Requirement |
|----|-------------|
| NFR-4.1 | OAuth scopes SHALL follow principle of least privilege (read-only Gmail, file-level Drive) |
| NFR-4.2 | System SHALL NOT store user credentials or secrets in sheet cells (API keys in ScriptProperties are acceptable — they are not visible in sheets) |
| NFR-4.3 | Error messages logged to sheets SHALL NOT contain sensitive data (email bodies, full file contents) |
| NFR-4.4 | API keys SHALL be randomly generated (minimum 32 characters) and stored securely (not in sheet cells) |
| NFR-4.5 | API SHALL enforce HTTPS-only communication (GAS Web Apps use HTTPS by default) |
| NFR-4.6 | API error responses SHALL NOT expose internal details (stack traces, file paths) to external clients |
| NFR-4.7 | API key rotation SHALL be supported without service interruption (generate new key, then revoke old) |
| NFR-4.8 | The API SHALL be the only programmatic writer to config sheets (besides direct human edits) — clients never write to sheets directly |

### NFR-5: Usability

| ID | Requirement |
|----|-------------|
| NFR-5.1 | All menu operations SHALL provide immediate visual feedback (toast or alert) |
| NFR-5.2 | Config entry SHALL minimize user input errors (see Design section for approach) |
| NFR-5.3 | Dashboard SHALL refresh automatically (formula-based) without manual action |
| NFR-5.4 | System initialization SHALL be a single menu click |

---

## 3. Constraints

| Constraint | Details |
|------------|---------|
| **Runtime** | Google Apps Script V8 runtime |
| **Execution limit** | 6 minutes per execution (30 minutes for Workspace accounts) |
| **Deployment** | Clasp-based push to bound script project |
| **No npm/modules** | GAS has no module system; all files share global namespace |
| **Cell limits** | 10 million cells per spreadsheet |
| **PropertiesService** | 9KB per property, 500KB total per script |
| **Triggers** | Max 20 triggers per user per script |
| **UrlFetch** | Not currently used but available for future webhook integrations |
| **Web App responses** | GAS Web Apps always return HTTP 200; real status conveyed in JSON envelope |
| **Web App CORS** | GAS Web Apps redirect responses; Chrome extension must use `host_permissions` for `script.google.com` and `script.googleusercontent.com` |
| **Chrome Extension** | Manifest v3; no persistent background pages; service workers are short-lived |

---

## 4. Design Decisions / Technical Approach

The following implementation choices support the requirements above. They may change without altering sections 1–3.

- **Workflow state**: Stored in PropertiesService (not volatile memory). Phase is stored in the same workflow state blob as status (e.g. PropertiesService active workflow key). Allowed phase values: initializing, running_rules, validating, persisting, evaluating_halt, scheduling_next, waiting_trigger. *Supports FR-4.4, FR-4.10, FR-4.11, NFR-2.2.*

- **Async execution**: ScriptApp time-based triggers for steps beyond the 6-minute execution limit. *Supports FR-4.5.*

- **Logging**: Log writes are buffered and flushed in batches to limit sheet API calls. The exact set of logged events and required fields per event type are defined in the logging design (see FR-5.4). *Supports NFR-1.3, FR-5.4.*

- **Config UI**: Config sheets use checkboxes for boolean fields (Active, Enabled, Overwrite) and data-validation dropdowns for enumerated fields (Method, Mode). Navigation to config sheets from menu. *Supports FR-6, NFR-5.2.*

- **API**: Routing via an `action` parameter with dot-separated namespaces (e.g. `pipeline.<stageName>.run`). Response envelope: `{ success, data, meta, error }`. *Supports FR-9.1, FR-9.3.*

- **API keys**: Stored in ScriptProperties with prefix `API_KEY_`; randomly generated, minimum 32 characters; rotation without service interruption (generate new, then revoke old). *Supports FR-9.2, NFR-4.4, NFR-4.7.*

- **Chrome extension**: Only API endpoint URL and API key stored in chrome.storage.local; no config caching. *Supports FR-10.3.*

- **Result type**: Internal representation uses a standardized Result object (success/failure with metadata) for all operations. *Supports FR-8.4.*
