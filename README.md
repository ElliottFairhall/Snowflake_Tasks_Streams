# 🚀 Snowflake Tasks and Streams Tutorial

## 🎯 Learning Objectives

1. 🧠 Grasp the concept of tasks within Snowflake.

2. 🛠️ Master the creation and management of tasks in Snowflake.

3. 🧠 Understand the concept of streams within Snowflake.

4. 🛠️ Learn how to create and manage streams in Snowflake.

## 📚 Table of Contents

- Introduction

- Introduction to Change Data Capture (CDC)

- Introduction to Tasks in Snowflake

- Introduction to Streams in Snowflake

- Tutorial

- Conclusion

- Additional Resources

### 🎉 Introduction

Welcome to this tutorial on tasks and streams within Snowflake! This tutorial is designed to help you understand and implement tasks and streams in your Snowflake environment. By the end of this tutorial, you'll be able to confidently use these features to enhance your data processing capabilities. So, let's dive in! 🏊‍♀️

### Introduction to Changed Data Capture (CDC)

Change Data Capture (CDC) is a process that captures changes in a source database and propagates them to a target database using a change stream. In Snowflake, a stream object records data manipulation language (DML) changes made to tables, including inserts, updates, and deletes, as well as metadata about each change. This allows querying and consuming a sequence of change records in a transactional fashion.

### 📝 Introduction to Tasks in Snowflake

Tasks are powerful objects within Snowflake that allow you to execute SQL statements on a schedule or on demand. They can be used for a variety of purposes, from data loading and transformation, to analysis and reporting    .

Creating and managing tasks is achieved using the `CREATE OR REPLACE TASK`, `ALTER TASK`, and `DROP TASK` commands. You can also enable, disable, resume, and suspend tasks using the `TASK_HISTORY` table function.

Scheduling tasks is flexible, allowing you to set intervals or use a subset of CRON	utility syntax  and if you need to run a task manually just use the `EXECUTE TASK` command .

Tasks require compute resources to execute SQL code. You can choose between the **serverless compute model**, managed by Snowflake, or the **user-managed compute model**, where you specify an existing virtual warehouse.

### 🌊 Introduction to Streams in Snowflake

Streams are powerful objects within Snowflake that allow you to track changes in data objects like tables, views, or external tables. They capture the change data capture (CDC) records for an object and store them as a point in time snapshot. The columns `METADATA$ACTION`,  `METADATA$ISUPDATE` and `METADATA$ROW_ID` are added to the table when creating a stream. 

Below are these fields defined.

| **Column Name**	| **Description**| 
|--------------|-----------|
| METADATA$ACTION   | This column specifies whether the record is `inserted` or `deleted`.|
| METADATA$ISUPDATE	| This column will be set to `TRUE` if the record is updated.|
| METADATA$ROW_ID   | This column is a **unique id** for rows that cannot be changed.	|

With Streams, you can query to access the historical data and metadata for the object. This includes the type of change, the row ID, and the before and after state of each row. Streams can be used for various purposes, such as data pipeline, data replication, or data auditing. Creating a stream in Snowflake involves using the `CREATE OR REPLACE STREAM` command.

Snowflake supports different types of Streams depending on the source object and the change tracking mode. You can create and manage Streams using SQL commands and control the offset and retention period of the Stream.

### 💡Tutorial 

In this tutorial, we’ll be exploring how to construct a database, schema, and table within Snowflake. We’ll also delve into creating a stream and a task, and implementing the concept of Slowly Changing Dimensions (SCD) Type 2 using a stored procedure.

[Link to Tutorial](https://quickstarts.snowflake.com/guide/getting_started_with_streams_and_tasks/#0)

By the end of this tutorial, you’ll have gained a comprehensive understanding of how to leverage some of Snowflake’s robust features to efficiently capture and process data changes in a scalable manner.

### 🎈 Conclusion

In conclusion, streams and tasks are powerful features of Snowflake that enable you to track and process data changes in a scalable and efficient way. Streams provide a change tracking mechanism for tables, views, and external tables, while tasks allow you to execute commands or stored procedures on a schedule or on demand. By combining streams and tasks, you can create data pipelines that perform continuous ELT workflows, data quality checks, data transformations, and more.
## 📖 Additional Resources

- [Quick Start - Streams & Tasks](https://quickstarts.snowflake.com/guide/getting_started_with_streams_and_tasks/#0)
- [Data Engineering Simplified Video - Streams & Tasks](https://www.bing.com/videos/riverview/relatedvideo?&q=streams+and+tasks&&mid=28ED2FD13FC49005DFE328ED2FD13FC49005DFE3&&FORM=VRDGAR)
- [Snowflake Dynamic Tables VS Streams & Tasks VS Materialized Views | by Divyansh Saxena | Snowflake | Medium](https://medium.com/snowflake/snowflake-dynamic-tables-vs-streams-tasks-vs-materialized-views-c8e8a6a93b67)

## ⚠️ Disclaimer

Please note that while we strive to keep this tutorial up-to-date, Snowflake's features and capabilities may change over time. Always refer to the [official Snowflake documentation](https://docs.snowflake.com/) for the most accurate and current information.


