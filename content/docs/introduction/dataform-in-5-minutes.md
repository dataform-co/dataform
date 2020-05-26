---
title: Dataform and SQLX in 5 minutes
subtitle: Learn how Dataform and SQLX can help your team manage data in your warehouse
priority: 2
---

## Introduction

The modern analytics approach consists of centralising all the raw data from a company onto a single data warehouse. Once the raw data is there, it needs to be transformed, aggregated, normalized, joined and filtered before being usable in BI tools and other analytics projects. Dataform helps data teams transform raw data into well defined, reliable, tested and documented data tables that will power your company’s analytics.

### From ETL to ELT

The traditional ETL, which stands for Extraction, Transformation, and Loading, has now evolved into ELT:

1. Raw data is **Extracted** from source systems and **Loaded** into the data warehouse.
2. Raw data is **Transformed** within the data warehouse.

You use Dataform to manage that last part: data transformation in your data warehouse.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
Learn more about ELT with our page on where Dataform fits in the modern data stack.

<a href="/introduction/modern-data-stack"><button intent="primary">See where Dataform fits in the modern data stack</button></a></div>

### Build a single source of truth of data

Once raw data is loaded into your warehouse, your team transforms it into a single source of truth of data across your organisation. Dataform enables your team to follow industry best practices:

- Manage data with code (with SQL)
- Set standardized development processes like version control
- Test your data quality
- Document your data tables

<div className="bp3-callout bp3-icon-info-sign bp3-intent-none" markdown="1">
<h4 class="bp3-heading">A note of Graphical User Interfaces</h4>
Graphical User Interfaces (GUI) are often easy to get started with and can help less technical users build data pipelines. In practice, we find that past 10 or 20 data tables, pipelines become extremely difficult to manage, search and reason about. SQL is one of the best abstraction to express complex data logic.
</div>

## Introducing SQLX

SQL is the de facto language for processing data in cloud data warehouses and SQL has many advantages.

- Scalable processing in the data warehouse.
- It’s usually much simpler and easier to express your pipeline in SQL.
- It’s a common language across teams and systems.
- It’s easy to introspect when something goes wrong.
- It enables faster development thanks to fast feedback loops.

### The few limitations of SQL

Current SQL workflows don’t necessarily follow engineering best practices. Several key features of writing code are missing in current SQL implementations.

- You **can’t reuse code** easily across different scripts.
- There’s **no way to write tests** to ensure data consistency.
- **Managing dependencies is hard** because it requires separate systems. In practice many teams write 1000 lines long queries to ensure data processing happens in the right order.
- **Data is often not documented** because documentation is needs to be managed outside of the code, in a separate system. It makes it hard for teams to keep it updated.

### What is SQLX

SQLX is an open source extension of SQL. As it is an extension, every SQL file is also a valid SQLX file. **SQLX brings additional features to SQL to make development faster, more reliable, and scalable**. It includes many functions including dependencies management, automated data quality testing, and data documentation.

### What does SQLX look like?

In practice, SQLX is mostly composed of SQL in the dialect of your data warehouse (Standard SQL if you are using BigQuery, SnowSQL if you are using Snowflake…).

<img src="https://assets.dataform.co/docs/introduction/sqlx_simple_example.png" max-width="661"  alt="SQLX example" />
<figcaption>This illustration uses BigQuery Standard SQL. SQLX works the same way with all SQL dialects.</figcaption>

<h4><span className="numberTitle">1</span> Config block</h4>

In SQLX, you only write SELECT statements. You specify what you want the output of the script to be in the config block, like a `view` or a `table` as well as other types available.

Dataform takes care of adding boilerplate statements like `CREATE OR REPLACE` or `INSERT`.

<h4><span className="numberTitle">2</span> The Ref function and dependency management<h4>

The `ref` function is a critical concept in Dataform. Instead of hard coding the schema and table names of your data tables, the `ref` function enables you to reference tables and views defined in your dataform project.

Dataform uses that `ref` function to build a dependency tree of all the tables to be created or updated. When Dataform runs your project in your warehouse, that ensures that tables are processed in the right order.

The following images illustrate a simple Dataform project and its dependency tree. In practice, a Dataform project can have dependency trees with hundreds of tables.

<img src="https://assets.dataform.co/docs/introduction/ref_illustration_code.png" max-width="426" alt="Ref function illustration" />
<figcaption>Scripts in a Dataform project</figcaption>

<img src="https://assets.dataform.co/docs/introduction/simple_dag.png" max-width="885" alt="dependency tree" />
<figcaption>Dependency tree in a Dataform project</figcaption>

Managing dependencies with the `ref` function has numerous advantages.

- The dependency tree complexity is abstracted away. Developers simply need to use the ref function and list dependencies.
- It enables us to write smaller, more reusable and more modular queries instead of thousand lines long queries. That makes pipelines easier to debug.
- You get alerted in real time about issues like missing or circular dependencies

### SQLX = transformation logic + data quality testing + documentation

One of the powerful attributes of SQLX is that you can define the transformation logic, data quality testing rules, and your table documentation all within a single file.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
All SQLX features can be adopted incrementally. We often see teams starting with simple scripts and progressively adopting more and more SQLX features as their pipeline complexity grows.
</div>

<img src="https://assets.dataform.co/docs/introduction/sqlx_second_example.png" max-width="481" alt="SQLX second example" />

<figcaption>This example illustrates a SQLX file using data documentation and data quality testing features.</figcaption>

<h4><span className="numberTitle">1</span> Data documentation</h4>

You can add a description of your table and its fields directly in the config block of your SQLX file. Description of your tables is available in the Dataform data catalog.

Defining description within the same file makes it easy to maintain data documentation which is always up to date.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-none" markdown="1">
The documentation you add to Dataform is machine readable. This allows you to parse this documentation, and push it out to other tools.
</div>

<h4><span className="numberTitle">2</span> Data quality tests</h4>

You can define data quality tests, called assertions, directly from the config block of your SQLX file. Assertions can be used to check for uniqueness, null values or any custom row condition.

Assertions defined in the config block get added onto your project’s dependency tree after the table creation.

<img src="https://assets.dataform.co/docs/introduction/assertion_dag.png" max-width="605" alt="DAG with assertions" />
<figcaption>Assertions defined in the config block are added to the dependency tree of your project. They will run after the table creation / update.</figcaption>

For more advanced use cases, assertions can also be defined in separate SQLX files. See the [assertion page](/guides/assertions) on documentation.

### Other SQLX features

SQLX has numerous additional features to help you manage data in your warehouse and build more reliable data pipelines faster.

- Incremental tables and snapshots
- Reusable functions and variables
- Declaring source data
- And many other features

Check the docs to learn more.

## How do you develop in SQLX

<img src="https://assets.dataform.co/docs/introduction/how%20it%20works.png" max-width="1265"  alt="" />

**Step 1.** You develop your pipelines in SQLX files, locally or using the Dataform web editor.

**Step 2.** Dataform compiles your entire project into native SQL that can be run in your warehouse. That process happens in real time, resolving dependencies, checking for errors and alerting you if any issue occurs

**Step 3.** Dataform then connects to your data warehouse and executes those SQL commands. That happens manually, on a schedule or via an API call.

**Step 4.** When this is done, you get a list of data tables that are tested and documented that you can use for your analytics.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-success" markdown="1">
<h4 class="bp3-heading">SQLX is open source</h4>
The SQLX compiler and runner is open source, you can run it locally or on your own servers.
</div>
<br/>
<div className="bp3-callout bp3-icon-info-sign" markdown="1">
Lean how Dataform works in more details.

<a href="/introduction/modern-data-stack"><button>Understand how Dataform works</button></a></div>

## Enable all your team to adopt best practices and be more productive with Dataform web

Dataform web is a web application made for data teams. It packages a rich Integrated Development Environment (IDE), a pipeline scheduler, a logs viewer and a data catalog.

<img src="https://assets.dataform.co/landing/ide-mockup.png" max-width="1230" alt="" />

Dataform web enables data teams to collaborate in a single environment and brings the following benefits:

- **Managed infrastructure** to run data pipelines in the data warehouse.
- An alerting system and detailed logs to **minimize the time spent on pipeline maintenance**.
- An intuitive UX that **lowers the barrier to entry of engineering best practices** like version control and development environments.
- Instant feedback while developing their project for **more productivity**
- A **data catalog** to explore data tables and existing pipelines quickly.

To learn more about Dataform web, you can check [dataform.co/product](https://dataform.co/product).
