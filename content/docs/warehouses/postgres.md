---
title: Postgres
subtitle: Authentication and configuration options for Postgres.
---

## Authentication

Postgres projects require the following configuration settings:

- Hostname. This would be in the form `[name].[id].[region].redshift.amazonaws.com` if you are using AWS.
- Port (usually `5432`)
- Username and password
- Database name

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Dataform's IP addresses must be whitelisted in order to access your Postgres instance. Dataform's IP addresses are <code>35.233.106.210</code> and <code>104.196.10.242</code>.
</div>

## Getting help

If you are using Dataform web and are having trouble connecting to Postgres, please reach out to us by using the intercom messenger at the bottom right.

If you have other questions related to Postgres, you can join our slack community and ask question on the #Postgres channel.

<a href="https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A"><button>Join dataform-users on slack</button></a>
