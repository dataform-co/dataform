---
title: Stackoverflow data on BigQuery
subtitle: A simple project turning Stackoverflow public raw data into reporting tables
priority: 0
---

# Summary

This project transforms four raw datasets (`posts_answers`, `posts_questions`, `badges` and `users`) into two summary reporting tables. `posts_combined` brings stackoverflow posts and answers into a single table, with a “type” field for differentiating between the two. `user_stats` provides an overview of each users engagement: when they signed up, how many badges they have, and how many posts and answers they’ve made.

# Dependency tree of the project

<img src="https://assets.dataform.co/landing/bigquery_sample_project_dag.png" width="1100"  alt="Sample bigquery Dataform project DAG" />
<em>Dependency tree of the BigQuery sample project</em>

# View the project

<a href="https://app.dataform.co/#/6470156092964864/overview"><button>See the example on GitHub</button></a>

<a href="https://app.dataform.co/#/6470156092964864/overview"><button intent="primary">See the example on Dataform web</button></a>
<em>(Viewing the example project on Dataform web requires sign up)</em>
