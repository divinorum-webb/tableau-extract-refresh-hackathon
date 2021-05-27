## Tableau DataDev Hackathon - May 2021

### Objective
Pause and unpause Tableau Server (or Tableau Online) extract refresh tasks. Plug in the name or luid for the workbook 
you want to pause, and all related extract refresh tasks will be paused until you unpause them.

Workbooks can have upstream datasources, so if you pause a workbook then its upstream datasources should be paused as well.

### Problem Statement
If you need to stop the extract refresh tasks for a workbook, datasource, or a workbook and
its upstream datasources then you're in a bit of a pickle.

You could:
1) Suspend an entire schedule and all of its tasks, which stops your workbook's extracts but also
goes nuclear on every other task depending on that schedule.
2) Manually comb through your extract refresh tasks and delete the ones which 
you want to temporarily disable, with plans to manually recreate them later.
3) Use Tableau APIs to automate what is otherwise a manual time vampire.

### Solution
This project makes it possible to pause the extract refresh tasks for the specified 
workbook or datasource. If you point to a workbook, its upstream datasources are automatically 
detected and they are paused as well.

You can then unpause these extract refresh tasks.

Capabilities are as follows:
1) Suspending an entire Tableau Server schedule (Tableau Server only).
2) Activating an entire Tableau Server schedule (Tableau Server only).
3) Pausing all extract refresh tasks associated with a workbook or datasource.
4) Unpausing all extract refresh tasks associated with a workbook or datasource.

There is no REST API to pause and unpause an extract refresh task.
This process makes it easy to delete all tasks associated with a workbook (or datasource)
and to recreate (unpause) those tasks when the time is right.

### Tableau Tools
A lot is happening behind the scenes, which is why this solution leans on both 
the Metadata API and the REST API.

#### Metadata API
The Metadata API is used to identify dependencies between workbooks and datasources. When pausing all 
extract refresh tasks for a workbook, you need 
to stop extracts related to both the embedded extracts and the published datasources 
the workbook uses. When pausing tasks for a specific datasource, this is more straightforward 
as you only need to identify tasks directly related to that datasource.
The Metadata API helps us to identify exactly which refresh tasks we need to target.

#### REST API
The REST API is our go-to tool for updating schedules and creating or deleting extract 
refresh tasks. Additionally, we can use the REST API to query details for our workbooks, 
datasources, schedules, and extract refresh tasks.

### Usage Examples

