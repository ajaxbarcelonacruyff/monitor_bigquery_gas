> [!NOTE]
> This article contains information as of March 2021. Please refer to the official website for the latest updates.

# Monitoring Analytical Data

Have you ever encountered a situation where analytical data tables imported into BigQuery from sources like Google Analytics or Firebase were not updated, leading to incorrect analysis results? This is particularly common with Google Analytics 360 data, which often imports data with delays, or even inserts backfill data silently at a later time.

Such situations can lead to discrepancies between your data count or content and the aggregated results if you run daily batch jobs for aggregation. This is especially true when new data is imported after the aggregation has already taken place.

While I’ve always believed that analytical data requires monitoring, it’s challenging to justify the expense of monitoring tools for non-mission-critical systems like these. It would be preferable to allocate such costs to tools like BigQuery ML instead.

# Creating a Monitoring Tool with Google Sheets and Google Apps Script

One might think, "Why not just use a Python script to access BigQuery locally?" While feasible, it would require manual execution or keeping your PC continuously running. Seeking alternatives, I discovered that Google Apps Script could fulfill all my needs: fetching BigQuery results, scheduling daily executions via triggers, and even sending email notifications to myself. Google to the rescue!

# Steps to Create a BigQuery Monitoring Tool

The basic workflow of the monitoring tool is as follows:
1. Use a Google Apps Script trigger to execute a function.
2. Run a query in BigQuery.
3. If the query result contains at least one record, send the result via email.
4. Log the details in Google Sheets.

## Creating the Query

First, create a query. For testing, you can write and execute your query in the BigQuery web console. Make sure the query is designed to flag an error if it returns any records.

For instance, to check whether data from the previous day is stored in GA4, you can use a query like this, which returns results if the most recent date is not the previous day:

```sql
-- Returns the latest table date and the record count from BigQuery
SELECT MAX(_TABLE_SUFFIX) AS tid, COUNT(1) AS cnt_row FROM `project_id.analytics_ga4property_id.event_20*`
-- Condition to flag if the latest date is not the previous day
HAVING MAX(_TABLE_SUFFIX) != FORMAT_DATE('%y%m%d', DATE_SUB(DATE(CURRENT_TIMESTAMP, 'Asia/Tokyo'), INTERVAL 1 DAY))
;
```


## Creating the Google Sheet

### Query Storage Sheet

Next, create a Google Sheet to store the queries and related configuration. Name this sheet `Queries`. For simplicity, the input will include only the email subject and the BigQuery query.

- **Column A:** Title (used as the email subject)
- **Column B:** Query to execute in BigQuery

![image.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/3939399/fa0f75e5-6999-e03e-c3bb-925fe122debb.png)

If you want to execute multiple queries, add them in rows starting from row 3. The script is designed to process queries sequentially without skipping empty rows.

If you need to send emails to different recipients based on the query, you can add a column (e.g., **Column C**) with email addresses. Modify the email sending function (`sendEmail`) later to accept this additional input.

### Log Sheet

Create another sheet named `log`. This sheet can remain empty initially; it will be used to log execution results.

## Writing the Google Apps Script to Query BigQuery

### Retrieving Information from the Sheet

Now, implement the core functionality using Google Apps Script. Open the script editor by navigating to **Tools** → **Script editor** in Google Sheets. This method makes integration with the spreadsheet simpler.

![image.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/3939399/25db5835-ce49-a0fd-bae5-95ffa836078e.png)

Once the Google Apps Script window opens, write the following code into the editor. Save the file with a name like `BigQueryMonitoring`.

![image.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/3939399/7c447b75-2d7a-b294-095e-600ff310f68a.png)

```javascript
function main() {
  // Retrieve the spreadsheet
  var spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  // Get the `Queries` sheet
  var queries_sheet = spreadsheet.getSheetByName("Queries");
  // Retrieve data from each row (skip the header row)
  for (var i = 2; i <= queries_sheet.getLastRow(); i++) {
    // Get the value in column A (subject)
    var query_title = queries_sheet.getRange(i, 1).getValue();
    // Get the value in column B (query)
    var query = queries_sheet.getRange(i, 2).getValue();
    // Skip empty rows
    if (query_title && query) {
      // Execute the query (defined below)
      runQuery(query_title, query);
    } else {
      break;
    }
  }
}
```


# Running SQL Queries
Next, define the runQuery function to execute the SQL. This function includes several steps:

1. Execute the SQL query.
1. Wait for results if the query is still processing.
1. Convert the results to a string format.
1. If results exist, send an email and log the output.
1. Log execution results regardless of errors or success.

```
function runQuery(title, query) {
  var projectId = 'xxxxx'; // Replace with your project ID
  var request = { query: query };
  // Required for standard SQL
  request.useLegacySql = false;
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;
  // Wait for the job to complete
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }
  // Retrieve SQL results
  var rows = queryResults.rows;
  var totalRows = queryResults.totalRows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }
  var message = '';
  // If there are results, convert them to a string and send via email
  if (rows) {
    var headers = queryResults.schema.fields.map(field => field.name);
    message += headers;
    rows.forEach(row => {
      var dataRow = row.f.map(col => col.v).join(',');
      message += dataRow + '\n';
    });
    // Send results via email
    sendEmail(title, message);
    // Log the results
    writeLog(title + ' ' + rows.length + ' rows returned.');
  } else {
    // Log when there are no results
    writeLog(title + ' No rows returned.');
  }
}
```

Replace xxxxx with your BigQuery project ID. You can find your project ID in the BigQuery console under the "Project Name" section.

![image.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/3939399/284ec498-b6e3-2fbc-fa48-79816c5a148e.png)

## Sending Emails
The email sending function uses MailApp.sendEmail. Key points include:

1. The sender is the Google account associated with the spreadsheet and script.
1. The email subject includes a prefix and the query title for filtering.
1. The recipient is fixed for now but can be customized.
1. The email body includes the URL to the Google Sheet.

```
function sendEmail(subject, message) {
  var to = 'abcd@gmail.com';
  if (MailApp.getRemainingDailyQuota() == 0) { return; }
  var subj = '[BigQuery] Alert: ' + subject;
  var url = '\n Script: https://docs.google.com/spreadsheets/d/xxxxxxxxxxx';
  MailApp.sendEmail({
    'to': to,
    'subject': subj,
    'htmlBody': message + url
  });
}

```

Including the URL to the spreadsheet in the email makes it easy to trace the source of alerts.

## Logging Results
Finally, create a function to log execution results in the log sheet. Each log entry includes the timestamp and the execution result.

```
function writeLog(str) {
  var spreadsheetFile = SpreadsheetApp.getActiveSpreadsheet();
  var logSheet = spreadsheetFile.getSheetByName("log");
  var lastRow = logSheet.getLastRow();
  var input = [[formatDateTimeAsString(new Date()), str]];
  logSheet.getRange(lastRow + 1, 1, 1, 2).setValues(input);
}

function formatDateTimeAsString(d) {
  return Utilities.formatDate(d, 'GMT+9:00', 'yyyy/MM/dd HH:mm:ss');
}

```

## Setting Up Triggers
Set up a trigger to run the main function daily. Go to Edit → Current project's triggers in the script editor, and add a new trigger with the following settings:

- Function to run: main
- Deployment: Head
- Event source: Time-driven
- Type of time-based trigger: Day timer
- Time: Choose as needed

Triggers can also be set for spreadsheet events like opening or modifying the sheet.

![image.png](https://qiita-image-store.s3.ap-northeast-1.amazonaws.com/0/3939399/d0f0a580-d0c9-2347-5857-a3f8257b1199.png)

# Practical Uses
Beyond monitoring for missing data, I’ve used this tool for:

- Detecting when daily sales or transaction counts drop below 80% compared to the previous day.
- Flagging unexpected values (e.g., sales prices significantly higher than average).
- Identifying duplicate data due to batch errors or incorrect data imports.

# Advanced Applications
Here are some additional use cases:

- Sharing data with third parties who cannot access BigQuery.
- Writing data to Google Sheets for reporting purposes, though this is less necessary if using Google Data Studio with direct BigQuery connections.
