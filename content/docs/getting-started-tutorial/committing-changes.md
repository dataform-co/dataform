---
title: Committing your changes
subtitle: Learn how to committ changes you've made in your Dataform project
priority: 5
---

## Comitting changes

Dataform uses the Git version control system to maintain a record of each change made to project files and to manage file versions. Each Dataform project has an associated Git repository.

When you first use Dataform we will automatically create a personal development branch for you, name_dev. Only you can see this branch and commit to it. All the changes you've made so far should be on this branch.

<div style="position: relative; padding-bottom: 55.93750000000001%; height: 0;"><iframe src="https://www.loom.com/embed/7e0f28b3ff8d473f9594019a7b0bff70" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

1. Press the `Comitt Changes` button:

   - In the modal you should see a list of all the changes you made to your project.

2. Give your commits a description and press `Commit`.

3. Click the `Push to Master` button to push your changes to the master branch:

   - The changes will now be on `production` and used for future schedule runs.

For more information on how version control works in Dataform, see our [docs](https://docs.dataform.co/dataform-web/version-control#__next).

## Summary

You’ve now created your two tables; `order stats` and `customers`. `customers` depends on `order_stats` and will not be run until `order_stats` is updated. You have added data quality tests so that you know the data is reliable and you have documented your datasets so other team members know what the tables contain. A schedule will run every hour to keep the tables up to date, and you will be automatically alerted if the pipeline fails or there are data quality issues.

## Next Steps

### Start working on your own project

Follow the steps in the tutorial to get started with your own project. Connect to your own warehouse and use a query that you frequently run to create your first dataset.

### Learn more about Dataform

Learn more about [Dataform & SQLX in 5 minutes](https://docs.dataform.co/introduction/dataform-in-5-minutes) and our [best practices](https://docs.dataform.co/best-practices).

### Join the Dataform community

Join our [Slack channel](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A) and read our [blog](https://dataform.co/blog) to learn more about Dataform use cases, best practices and to meet other people solving similar problems!
