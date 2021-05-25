## Tableau DataDev Hackathon - May 2021

### Objective
Pause and unpause Tableau Server (or Tableau Online) extract refresh tasks.

### Problem Statement
What do you do if you need to pause an extract refresh task? You might manually 
click through your user interface to find a workbook and remove it from its
schedule. But what about the published datasource the workbook uses? Looks like
you'll need to manually disable that as well.

This project strives to avoid the manual hassle. Just point to a workbook 
and say that you want to pause its extract refresh tasks. And we are done!

Time to re-enable those extract refresh tasks? Point at the workbook and re-enable them.

### Solution
This Python code makes it possible for any team using the Tableau REST API to 
take what has been built here and integrate with their own environment to 
pause and unpause Tableau extract refresh tasks.

Capabilities are as follows:
1) Suspending an entire Tableau Server schedule (Tableau Server only).
2) Activating an entire Tableau Server schedule (Tableau Server only).
3) Pausing all extract refresh tasks associated with a workbook or datasource.
4) Unpausing all extract refresh tasks associated with a workbook or datasource.

Because there is no REST API to literally pause and unpause an extract refresh task, 
this process makes it easy to delete all tasks associated with a workbook (or datasource)
and to recreate (unpause) those tasks when the time is right.

### Tableau Tools
A lot is happening behind the scenes, which is why this solution leans on both 
the Metadata API and the REST API.

#### Metadata API
The Metadata API is used to query all of the data dependencies for a workbook or
for a datasource. When pausing all extract refresh tasks for a workbook, you need 
to stop extracts related to both the embedded extracts and the published datasources 
the workbook uses. When pausing tasks for a specific datasource, this is more straightforward 
as you only need to identify tasks directly related to that datasource.
The Metadata API helps us to identify exactly which refresh tasks we need to target.

### REST API
The REST API is our go-to tool for updating schedules and creating or deleting extract 
refresh tasks. Additionally, we can use the REST API to query details for our workbooks, 
datasources, schedules, and extract refresh tasks.

### Usage Examples

