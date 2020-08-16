---
title: Building your data model
subtitle: Learn how to connect to a warehouse and create and publish your first dataset.
priority: 1
---

Now you have your BigQuery project and warehouse credentials, you’re going to set up your Dataform project using Dataform Web. In this step you'll create a project, connect your warehouse and build out your first dataset.

<div style="position: relative; padding-bottom: 55.93750000000001%; height: 0;"><iframe src="https://www.loom.com/embed/56df10cba03045ddbf27e3a577907876" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

## Create a Dataform project

To create a new project in Dataform:

1. Create a Dataform account [here](https://app.dataform.co/).
2. Follow the sign up flow:

- We will ask you for a bit of information about yourself and what you’re hoping to achieve with Dataform.
- As part of the sign up flow we will create a new project for you. Give it the name `Dataform Tutorial`.
- If you already have an account, you can create a new project by going to the homepage and clicking `New Project`.

<img src="https://assets.dataform.co/getting%20started%20tutorial/creating%20a%20dataset/Group%209%20(1).png" max-width="753"  alt="Creating a new project" />

## Connecting a warehouse

As part of the project creation flow you'll be asked to connect to a warehouse. You’re going to use the credentials you generated in the earlier part of this tutorial to connect:

1. On the Configure Warehouse modal click `Connect`.
2. Select `Google BigQuery` from the drop down menu.
3. Enter your Project ID.

- This can be found by going to the [BigQuery console](https://console.cloud.google.com/) and looking at the Project info box.

4. Browse for the service account key JSON file you created in the `Setting Up` part of this tutorial and upload it.
5. Check your connection is working:

- Press `Test Connection` to check that the connection is working.
- Once this is successful, you can press `Save Connection`.

<img src="https://assets.dataform.co/getting%20started%20tutorial/creating%20a%20dataset/Screenshot%202020-08-13%20at%2015.46%201%20(1).png" max-width="753"  alt="Connecting a warehouse" />

## Creating a dataset

Now that you've created your project and connected a warehouse, you're ready to start defining your data model.

1. Make sure you are in a development branch:

- By default you should already be in a development branch called `yourname_dev`.
- However, if you would like to create a new branch, click `Develop` and select `New Branch`.
- Give your branch a name.

2. Create a new dataset:

- Click on the `New Dataset` button in the left hand side bar.
- Choose whether you want your dataset to be a table, view or incremental table. In this case we want to create a table.
- Name the table `order_stats` and click `Create Table`.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
<h4 class="bp3-heading">In Dataform transformations are defined using SQLX</h4>
 SQLX is an extension of SQL. A typical SQLX file contains a SELECT statement defining a new table or a view and a config block at the top. The config block is used to specify additional options for the table or view. To see a full list of options for the config block, open the Documentation tab.
</a></div>

3. Define the dataset:

- To create your table use the below query which joins orders information from Shopify, payment details from Stripe and also applies some filters to the data:

```sql
SELECT
  orders.date AS order_date,
  orders.id AS id,
  orders.customer_id AS customer_id,
  orders.status AS order_status,
  charges.status AS payment_status,
  charges.payment_method AS payment_method,
  SUM(orders.item_count) AS item_count,
  SUM(charges.amount) AS amount

FROM
  dataform-demos.dataform_tutorial.shopify_orders AS orders
  LEFT JOIN dataform-demos.dataform_tutorial.stripe_payments AS charges
    ON orders.payment_id = charges.id

WHERE
  orders.id <= 999
  AND orders.item_count > 0
  AND orders.status <> 'internal'
  AND charges.payment_method IN ('debit_card', 'subscription', 'coupon')

GROUP BY 1, 2, 3, 4, 5, 6
```

- Paste the query into `order_stats.sqlx` below the config block.
- Dataform will automatically validate your query and check for any errors
- Once you see that the query is valid you can click `Preview Results` to check that the data looks correct

4.  Create the table in your warehouse:

- Click `Publish Table` to create the table in your warehouse
- This will take the SQLX that we’ve written, compile it into the SQL syntax of your warehouse (in this case, BigQuery), and then execute that SQL in your warehouse with the correct boilerplate code to create a table

5. Check Run Logs:

- You can see the progress of the run in the header menu.
- Once you see that it has been successful, you can view `Run Logs` by clicking on the hamburger menu in the top left hand corner.
- In `Run Logs` you can see all the past runs in the project. You can see their status, as well as how and when they were triggered.
- For each run you can see the exact SQL run against the warehouse to create your datasets by clicking the `Details` button.

<img src="https://assets.dataform.co/getting%20started%20tutorial/creating%20a%20dataset/Screenshot%202020-08-13%20at%2015.51%201%20(1).png" max-width="753"  alt="Run Logs" />

You now have a new table called `order_stats` which has been created in your warehouse and you're ready to add to your data model!

For more detailed info on publishing datasets in Dataform, see our [docs].(https://docs.dataform.co/guides/datasets/publish#__next)
