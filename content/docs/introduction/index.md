---
title: Introduction
subtitle: Manage data in your warehouse with Dataform and SQLX
priority: 0
icon: menu-open
---

## What is Dataform?

Dataform is a platform to manage data in BigQuery, Snowflake, Redshift, and other data warehouses.

It helps data teams build data pipelines that turn raw data into new tables and views that can be used for analytics. Dataform does the T in ELT (Extract, Load, Transform) processes. It doesn’t extract or load data in your warehouse but it’s very powerful to transform data already loaded in your warehouse.

<div className="bp3-callout bp3-icon-info-sign bp3-intent" markdown="1">
Learn more about ELT and where Dataform fits in the modern data stack.

<a href="https://docs.dataform.co/introduction/modern-data-stack"><button>See where Dataform fits in the modern data stack</button></a></div>

By using Dataform and its best practices, data teams are more productive and build new data tables that are well defined, tested and documented for use by the entire company.

<img src="https://assets.dataform.co/blog/datastack_horizontal.png" width="2254"  alt="" />

## What can it do?

In its simplest form, Dataform helps you run SQL commands in your data warehouse to create new tables and views. Dataform ships many features made to improve the way you manage data and make your team more productive.

- **Define tables and views** to be created in your data warehouse
- **Add documentation** to tables and views
- Define assertions to **test the quality of your data**
- **Reuse code** across multiple scripts
- Run arbitrary **SQL operations**
- Take **snapshots** of your data
- Use **ready-made SQL packages** to help you model your data

## How does it work?

1. You define new tables and views to be built in your data warehouse.
2. As you develop, Dataform builds a dependency tree of all actions to be run in your warehouse. This dependency tree determines the order of the actions to be run and ensures that tables are created and updated in the right order.
3. Dataform runs this dependency tree in your warehouse to create new tables, views, and run other SQL commands.

<div className="bp3-callout bp3-icon-info-sign" markdown="1">
Learn more about how Dataform works.

<a href="https://docs.dataform.co/introduction/dataform-5-minutes"><button intent="primary">5 min overview of Dataform</button></a>
<a href="https://docs.dataform.co/introduction/how-dataform-works"><button>How Dataform works technically</button></a>

</div>

## How can I use it?

They are two main ways to work with Dataform. You can use <a target="_blank" rel="noopener" href="https://dataform.co">Dataform web</a> application with an Integrated Development Environment (IDE). You can also use Dataform locally using the Command Line Interface (CLI).

The core of Dataform is open source (Dataform compiler and runner) and can be used with the CLI.

## Who should use Dataform?

Dataform is built for data professionals who interact with a cloud data warehouse. That includes anyone who knows how to write SQL queries, including data analysts, data engineers and data scientists.

Using Dataform requires an understanding of SQL. If you're unfamiliar with SQL, check out the Khan Academy [Introduction to SQL course](https://www.khanacademy.org/computing/computer-programming/sql) or [Codeacademy](https://www.codecademy.com/learn/learn-sql).

Knowledge of Javascript can be useful to use Dataform most advanced features. Those are totally optional but can make developing faster and easier. If you are unfamiliar with Javascript, check the re-introduction to Javascript on [MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/A_re-introduction_to_JavaScript).

## Why should I use Dataform?

Dataform helps data teams adopt best practices and software engineering workflows to manage tables in their data warehouse.

By using Dataform and its best practices, data teams are able to manage data significantly faster and deliver data that is trusted and understood by the entire organization.

## Need help?

You can join our Slack group and discuss with our team and hundreds of other data professionals using Dataform.

<a href="https://slack.dataform.co" target="_blank" rel="noopener"><button intent="primary">Join Dataform slack</button></a></div>

If you are encountering any issue on the Dataform web app, please contact our team using the intercom messenger icon on the bottom right of the page.
