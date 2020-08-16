---
title: Managing dependencies
subtitle: Learn how to use the ref function in Dataform and how to view your project in the Dependency tree.
priority: 2
---

## Managing Dependencies

Dataform provides methods that enable you to easily reference another dataset in your project using the `ref` function.
This provides two advantages:

- You don’t have to provide the full SQL dataset name.
- Any dataset that is referenced by a query will be automatically added to that query's dependencies. Dependency queries are always executed before dependent queries to ensure correctness.

In this step you'll learn how to manage dependencies in Dataform.

<div style="position: relative; padding-bottom: 55.93750000000001%; height: 0;"><iframe src="https://www.loom.com/embed/201481ab82914d55b7c7787e6c903f26" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

1. You'll now create a second table called `customers`, following the same process as before

- Click `New Dataset` and select the table t.emplate.
- Give your table the name `customers`
- Click `Create`.

2. Define your dataset:

- To create your table, you're going to be using the below query:

```sql
SELECT
  customers.id AS id,
  customers.first_name AS first_name,
  customers.last_name AS last_name,
  customers.email AS email,
  customers.country AS country,
  COUNT(orders.id) AS order_count,
  SUM(orders.amount) AS total_spent

FROM
  dataform-demos.dataform_tutorial.crm_customers AS customers
  LEFT JOIN ${ref('order_stats')} orders
    ON customers.id = orders.customer_id

WHERE
  customers.id IS NOT NULL
  AND customers.first_name <> 'Internal account'
  AND country IN ('UK', 'US', 'FR', 'ES', 'NG', 'JP')

GROUP BY 1, 2, 3, 4, 5
```

- Paste the query into your file, below the config block.
- This query uses the `ref` function. The `ref` function enables you to reference any other table defined in a Dataform project.
- You can see the dependencies of a dataset in the right hand side bar.
- If you open the compiled query, you can see that the `ref` function has been replaced with the fully qualified table name.

3. Once you can see that your query is valid you can publish the table to your warehouse by clicking on `Publish Table`.

4. View the Dependency tree:

- Navigate to the menu in the top left hand corner of your project and click on the `Dependency Tree` tab.

<img src="https://assets.dataform.co/getting%20started%20tutorial/manage%20dependencies/Screenshot%202020-08-13%20at%2015.52%201%20(1).png" max-width="753"  alt="The dependency tree" />

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
<h4 class="bp3-heading">The dependency tree</h4>
Here you can see a visualisation of your entire Dataform project. Every time Dataform creates a run and executes SQL in your warehouse, it will be sure to update the actions in the corect dependency order. 
</a></div>

You now have two tables created in your warehouse, one called `order_stats` and one called `customers`. `customers` depends on `order_stats` and will not be run until `order_stats` is updated.

For more detailed info on managing dependencies in Dataform, see our [docs](https://docs.dataform.co/dataform-web/tutorials/102#__next).
