---
title: Data quality tests and documenting datasets
subtitle: Learn how to set up data quality tests using assertions and how to document your datasets
priority: 4
---

## Data quality tests

Adding tests to a project helps validate that your models are working correctly. These tests are run every time your project updates, sending alerts if the tests fail. Data quality tests in Dataform are called assertions.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
<h4 class="bp3-heading">Assertions</h4> Assertions enable you to check the state of data produced by other actions. An assertion query is written to find rows that violate one or more rules. If the query returns any rows, then the assertion will fail. There are various different types of assertions you can use including uniqueness checks and null checks. The simplest way to define assertions is as part of a dataset's `config` settings.
</a></div>

We want to create an assertion for our `order_stats` table will fail if there is more than one row in the dataset with the same value for `id`.

<div style="position: relative; padding-bottom: 55.78124999999999%; height: 0;"><iframe src="https://www.loom.com/embed/f4735816f4614ea6bb2b99de2ec98d46" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

1. Navigate back to your `order_stats` table:

- Click on the hamburger menu in the top left hand corner of your project and click on `Develop Project`.
- Navigate to the `order_stats` file.

2. In the config block write your assertion:

- Copy and paste this example into your file, replacing the config block that is already there.

```js
config {
  type: "table",
  assertions: {
    uniqueKey: ["id"]
  }
}
```

Dataform automatically creates a view in your warehouse containing the results of the compiled assertion query. This makes it easy to inspect the rows that caused the assertion to fail without increasing storage requirements or pulling any data out of your warehouse.

You can also choose to use assertions as dependencies. Assertions create named actions in your project that can be depended upon by using the dependencies `config` parameter. If you would like another dataset, assertion, or operation to only run if a specific assertion passes, you can add the assertion to that action's dependencies.

## Documenting your dataset

Dataform allows you to add documentation to the datasets defined in your project. Table and field descriptions are added using the same config block where you wrote your assertion.

1. In the config block write your documentation:

- Copy and paste the below example into your file, replacing the config block that is already there:

```js
config {
  type: "table",
    description: "This table joins orders information from Shopify & payment information from Stripe",
  columns: {
    order_date: "The date when a customer placed their order",
    id: "Order ID as defined by Shopify",
    order_status: "The status of an order e.g. sent, delivered",
    customer_id: "Unique customer ID",
    payment_status: "The status of a payment e.g. pending, paid",
    payment_method: "How the customer chose to pay",
    item_count: "The number of items the customer ordered",
    amount: "The amount the customer paid"
  },
    assertions: {
    uniqueKey: ["id"]
  }
}
```

- Table and field descriptions are now added to your table.

2. View the `Dependency tree`:

- Navigate back to the `Dependency tree` tab in the hamburger menu in the top left hand corner:
- Click on the `order_stats` table on the left.
- You can view your table and field descriptions in the data catalog.

<img src="https://assets.dataform.co/getting%20started%20tutorial/tests%20%26%20documentation/Screenshot%202020-08-13%20at%2015.56%201%20(1).png" max-width="753"  alt="The Data Catalog" />

You have now created an assertion which will fail if any value in your `id` field is duplicated and you have given your `order_stats` table and its fields descriptions.

You can find more information on [assertions](https://docs.dataform.co/guides/assertions#__next) and [documentation](https://docs.dataform.co/guides/datasets/documentation#__next) in our docs.
