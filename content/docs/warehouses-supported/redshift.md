---
title: Amazon Redshift
---

# Authentification

Redshift projects require the following configuration settings:

- Hostname in the form `[name].[id].[region].redshift.amazonaws.com`
- Port (usually `5439`)
- Username and password
- Database name

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  Dataform's IP addresses must be whitelisted in order to access your Redshift cluster. Please
  follow
  <a
    target="_blank"
    rel="noopener"
    href="https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-authorize-cluster-access.html"
  >these instructions</a>. Dataform's IP addresses are <code>35.233.106.210</code> and <code>104.196.10.242</code>.
</div>

### How to find Redshift credentials

1. Go to `Redshift` in your AWS console.
2. Select your cluster under `Clusters`.
3. The hostname is the endpoint listed at the top of the page. Username and database name are listed under cluster database properties.

The Redshift user should have permissions to `CREATE` schemas and `SELECT` from `INFORMATION_SCHEMAS.TABLES` and `INFORMATION_SCHEMAS.COLUMNS` . Please contact our team [via slack](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A) if you need help.

# Configuration options

Redshift specific options can be applied to tables using the `redshift` configuration parameter.

## Distributing data

You can configure how Redshift <a target="_blank" rel="noopener" href="https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html#t_data_distribution_concepts">distributes data</a> in your cluster by configuring the `distStyle` and `distKey` properties.

```js
config {
  type: "table",
  redshift: {
    distKey: "user_id",
    distStyle: "key"
  }
}
SELECT user_id FROM ...
```

This query compiles to the following statement:

```js
CREATE TABLE "dataform"."example"
DISTKEY(user_id)
DISTSTYLE even
AS SELECT user_id FROM ...
```

## Sorting data

You can also configure how Redshift <a target="_blank" rel="noopener" href="https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html">sorts data</a> in your datasets with the `sortKeys` and `sortStyle` properties.

```js
config {
  redshift: {
    sortKeys: [ "ts" ],
    sortStyle: "compound"
  }
}
SELECT 1 AS ts
```

## Binding views

By default, all views in Redshift are created as late binding views. This can be changed by setting the `bind` property in the redshift configuration block.

```js
config {
  type: "view",
  redshift: {
    bind: true
  }
}
SELECT 1 AS ts
```

# Dataform web features

## Real time query validation

Dataform validates the compiled script you are editing against Redshift in real time. It will let you know if the query is valid (or won’t run) before having to run it.

<video autoplay controls loop  muted  width="680" ><source src="https://assets.dataform.co/docs/compilation.mp4" type="video/mp4" ><span>Real time compilation video</span></video>

# Blog posts

## Import data from S3 to Redshift using Dataform

The blog post offers a walkthrough to load data from S3 to Redshift.

<a href="https://dataform.co/blog/import-data-s3-to-redshift"><button>Read the article on the blog</button></a>

# Getting help

If you are using Dataform web and are having trouble connecting to Redshift, please reach out to us by using the intercom messenger at the bottom right.

If you have other questions related to Redshift, you can join our slack community and ask question on the #Redshift channel.

<a href="https://slack.dataform.co"><button>Join dataform-users on slack</button></a>
