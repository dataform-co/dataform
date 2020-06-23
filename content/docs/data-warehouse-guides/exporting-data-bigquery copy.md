---
title: Exporting Data From BigQuery as a JSON
subtitle: Learn how to export data from Bigquery using the bq command line.
priority: 1
---

Cloud-based services are notorious for being closed-systems. Thankfully, Google Cloud delivers enough flexibility with a web-based management console, programming interfaces, and a handy command-line toolset to interact with all of its services, including BigQuery. For instance, those new to BigQuery will likely appreciate [<u>Cloud Console</u>](https://console.cloud.google.com/bigquery), which provides a friendly, streamlined UI for performing and organizing queries and loading and exporting data in a variety of formats, including JSON.

<img
src="https://assets.dataform.co/warehouse%20guides/exporting%20data%20bigquery/image3.png"
style="width: 100%"
/>

Warehouse engines are becoming the standard for housing most companies' data.  The leading use cases warehouses optimize are the performance of analytical queries and managing high volumes of data.  In many cases, there is no need to export data from the warehouse.  However, some circumstances require real-time access to data and transactional support, such as with an eCommerce site, or an online booking system for an airline.  In these cases, a transactional database is a better solution.

Fortunately, for companies that use Google's BigQuery, there are a variety of ways to export data (in different formats; including JSON), and export the schemas for other systems and services as well.  In this article, we will explore three common methods for working with BigQuery and exporting JSON.

# Data Export Options

## Method 1: Cloud Console

In the Google Cloud Console, within every table detail view, there is an "Export" button that provides a means to export data to a [<u>Google Cloud Storage</u>](https://cloud.google.com/storage) bucket in CSV, JSON, or Apache Avro formats.

**Step 1: Expand a project and dataset to list the schemas.**

<img
src="https://assets.dataform.co/warehouse%20guides/exporting%20data%20bigquery/image2.gif"
style="width: 100%"
/>

**Step 2: Click on a table to view its details.**

<img
src="https://assets.dataform.co/warehouse%20guides/exporting%20data%20bigquery/image4.gif"
style="width: 100%"
/>

**Step 3: Click on "Export."**

<img
src="https://assets.dataform.co/warehouse%20guides/exporting%20data%20bigquery/image1.gif"
style="width: 100%"
/>

The main limitation of exporting data through the Cloud Console is that it's only possible to export the schema and data together rather than separately. Also, exporting to [<u>GCS</u>](https://cloud.google.com/storage) means potentially setting up a storage container space in advance, which doesn't require a lot of effort, but does add an extra step or two.

## Method 2: Use a Client Library (i.e., C#, Go, Java, Python, PHP)

The Cloud Console is great for consuming existing data; however, beyond fundamental transformations, most popular programming languages can also talk to BigQuery. Many [<u>client libraries</u>](https://cloud.google.com/bigquery/docs/reference/libraries) for popular programming languages are available and officially supported by Google, such as C#, Go, Java, and Python, enabling programmatic data access to datasets hosted on BigQuery.

**Example:  Python + Pandas + BigQuery**

One popular method favored by Python-based data professionals is with [<u>Pandas</u>](https://pandas.pydata.org/) and the [<u>Google Cloud Library</u>](https://googleapis.dev/python/bigquery/latest/usage/pandas.html).  To get started with this method, installing the Google Cloud Library with the Pandas option will install both libraries into an environment using the pip package manager:

`pip install --upgrade google-cloud-bigquery[pandas]`

Then, to retrieve the result of an SQL query from BigQuery, as a Pandas DataFrame, the API is relatively straightforward to implement:

`from google.cloud import bigquery`

`client = bigquery.Client()`

`sql = """`

`SELECT name, SUM(number) as count`

`FROM`bigquery-public-data.usa_names.usa_1910_current``

`GROUP BY name`

`ORDER BY count DESC`

`LIMIT 10`

`"""`

`df = client.query(sql).to_dataframe()`

`df` becomes a DataFrame object:

``**`name count`**

`0 James 5015584`

`1 John 4885284`

`2 Robert 4749154`

`3 Michael 4366524`

`4 William 3901134`

`5 Mary 3750838`

`6 David 3605910`

`7 Richard 2544867`

`8 Joseph 2528437`

`9 Charles 2280600`

Using the Pandas library with a DataFrame type object also means being able to export the query to a variety of formats, including JSON:

`df.to_json()`

Result:

`'{"name":{"0":"James","1":"John","2":"Robert","3":"Michael","4":"William","5":"Mary","6":"David","7":"Richard","8":"Joseph","9":"Charles"},"count":{"0":5015584,"1":4885284,"2":4749154,"3":4366524,"4":3901134,"5":3750838,"6":3605910,"7":2544867,"8":2528437,"9":2280600}}'`

Outputting a result as JSON is very handy, but lacks schema information.  A lesser-known Pandas function called [<u>build_table_schema()</u>](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.io.json.build_table_schema.html) exists and is the perfect solution for viewing schema information separate from data.  To get schema information from a Pandas DataFrame, you simply need to import this method and pass a DataFrame to it.

`from pandas.io.json import build_table_schema`

`Schema_info = build_table_schema(df)`

Result:

`{'fields': [{'name': 'index', 'type': 'integer'},`

`{'name': 'name', 'type': 'string'},`

`{'name': 'count', 'type': 'integer'}],`

`'primaryKey': ['index'],`

`'pandas_version': '0.20.0'}`

_Note:  Using BigQuery from an API typically requires the setup of application credentials.  The process involves generating a “service account key” which is a JSON file, then creating an environment variable called GOOGLE_APPLICATION_CREDENTIALS that references it.  For more information, please see _[<u>_https://cloud.google.com/docs/authentication/getting-started_</u>](https://cloud.google.com/docs/authentication/getting-started)

## Method 3: The Command-Line Client `bq`

While the Cloud Console is arguably the easiest method of working directly with BigQuery datasets, it doesn't offer much granular control.  On the other hand, rolling a custom solution with Java or Python is usually a bit overkill to acquire the schema or the data alone.  A great alternative that offers flexibility, without having to code a solution from scratch to interact with BigQuery, is the bq command-line client.

The install for Google Cloud SDK, comes with the command-line client bq. An excellent place to start is the [<u>quickstart guide</u>](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-command-line) for bq supplied by Google. It links to the prerequisite SDK and instructions on how to install it.

### Examples

The [<u>bq</u>](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference#bq_show) command-line client provides a number of features for interacting with BigQuery such as loading, exporting, creating tables, and retrieving information about datasets.  For a full list of documented features and usage, review Google reference for [<u>Command-line tool reference</u>](https://cloud.google.com/bigquery/docs/reference/bq-cli-reference).

**The basic syntax of using bq is:**

`bq --common_flags <bq_command> --command-specific_flags <command_arguments>`

**Displaying basic table schema info:**

`bq show publicdata:samples.shakespeare`

**Display basic table schema info without data in JSON format (unformatted):**

`bq show --format=json publicdata:samples.shakespeare`

**Display basic table schema info without data in JSON format (formatted):**

`bq show --format=prettyjson publicdata:samples.shakespeare`

**Redirecting bq output to a file**

Most terminals and shells support saving files of most generated text by using the > operator.  So for instance, to save the basic schema of a BigQuery table to a JSON file, you can simply add “>” to the command and then the filename.

`bq show --format=json publicdata:samples.shakespeare > shakespeare.json`

**Export SQL query result to a local JSON file**

`bq --format=prettyjson query --n=1000 "SELECT * from publicdata:samples.shakespeare" > export.json`

Of course, the bq utility is flexible beyond exporting schemas or data. It's possible to orchestrate SQL operations from the command line, export or import data in a variety of formats.  To learn more about bq, refer to the official documentation [<u>Using the bq command-line tool</u>](https://cloud.google.com/bigquery/docs/bq-command-line-tool).

# Conclusion

Using BigQuery doesn't mean being stuck in a closed platform.  Using the web-based Cloud Console tools allows table exports to GCS and works well for distributing data manually.  Using a programming language like Python or Java is possible, given the availability of the many libraries supporting BigQuery connectivity.  To get at BigQuery data without having to set up an entire development environment, the bq command-line client offers a great deal of flexibility from within a terminal.
