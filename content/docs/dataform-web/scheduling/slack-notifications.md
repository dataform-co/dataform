---
title: Slack Notifications
---

## Introduction

Success and failure alerts for schedule runs can be configured as part of schedule set up. This guide explains how to set
up notifications to a [Slack](https://www.slack.com) channel.

## Create an Incoming WebHook url for your slack workspace

1. Open the [Incoming WebHook](https://dataformco.slack.com/apps/A0F7XDUAZ-incoming-webhooks?next_id=0) app.
2. Click "Add to Slack".
3. Choose the slack channel you would like alerts to be sent to
4. Click "Add Incoming WebHooks Integration".
5. On the next page you will see a "Webhook URL". You'll need this for the next step.

## Create a notification channel and link the webhook url

1. Open a development branch.
2. Navigate to the scheduling page using the left menu.
3. Open the "Notification Channels" tab.
4. Click "Add Channel". Give the channel a name, choose the "Slack" notification type, and copy in the "Webhook URL" from
the previous step.
5. Click "Create Channel".

## Add the notification channel to a schedule

1. Open the "Schedules" tab on the schedules page.
2. Create a new schedule, or edit an existing schedule.
3. In the "Notifications" section of the schedule dialogue, click "Add new notification settings".
4. Select the channel you just created. Choose `success`, `failure` or both as "Events".
