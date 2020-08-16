---
title: Setting up a schedule
subtitle: Learn how to set up a schedule and alerts in Dataform
priority: 3
---

In the first few videos we introduced the basic concepts of Dataform. We’ll now introduce some of the more advanced features: [testing](https://docs.dataform.co/guides/assertions) and [documentation](https://docs.dataform.co/guides/datasets/documentation), [scheduling](https://docs.dataform.co/dataform-web/scheduling) and [version control](https://docs.dataform.co/dataform-web/version-control).

## Setting up a schedule

You want to make sure your data is updated regularly so that it is always up to date. To do this you will create a schedule. You want your datasets to be updated every hour and create an alert so that you are notified by email if the pipeline fails.

Schedules are defined in the environments.json file. It’s easiest to set them up in the interactive UI, but it's also possible to edit them directly in the JSON.

<div style="position: relative; padding-bottom: 55.93750000000001%; height: 0;"><iframe src="https://www.loom.com/embed/28219ff65f9c4faca1604289c07cae3c" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe></div>

1. Navigate to the `Scheduling` page by clicking on the hamburger menu in the top left hand corner.

2. Press the `Create New Schedule` button.

- Give your schedule the name `hourly_update`.
- Press the 'Choose Frequency' button and select `hourly`.
- Press Save.

3. Set up a new notification channel

- Navigate back to the environments.json file.
- Click on the `Create New Notification channel` button.
- Give your notification channel the name `email`.
- Enter your email address into the box below and press enter.
- It’s also possible to set up notifications to Slack, or a webhook.

<img src="https://assets.dataform.co/getting%20started%20tutorial/schedululing/Screenshot%202020-08-13%20at%2015.54%201%20(1).png" max-width="753"  alt="Setting up a schedule" />

You have now set up a schedule which updates your project every hour annd an alert which notifies you by email if your pipeline fails.

For more detailed info on setting up schedules, see our [docs](https://docs.dataform.co/dataform-web/scheduling#__next).
