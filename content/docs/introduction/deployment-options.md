---
title: Deployment options
subtitle: Learn about the options for deploying Dataform in production
priority: 4
---

Dataform provides both fully-managed and self-hosted options to deploy Dataform projects.

## Dataform Web (hosted)

Dataform Web is a fully managed web-based interface for developing, deploying, and managing Dataform projects. It is free to use, and saves you the hassle of managing your own infrastructure.

The Dataform Web platform allows you to schedule runs, configure pipeline monitoring, and view full logs of all Dataform runs related to your project.

<a href="/dataform-web"><button>Learn about Dataform Web</button></a></div>

[Learn about Dataform Web](https://docs.dataform.co/dataform-web).

## Dataform CLI (open source)

The core Dataform framework is open source, and is bundled with a command-line tool that can be used to initialize, test, and run Dataform projects. The CLI can be used locally while developing projects, and also alongside an orchestration tool to run data pipelines on a regular basis.

 - To add Dataform to an Airflow DAG, use Airflow’s [bash operator](https://airflow.apache.org/docs/stable/howto/operator/bash.html). 
 - To use Dataform with Prefect, use the [shell task](https://docs.prefect.io/api/latest/tasks/shell.html#shelltask)

<a href="/dataform-cli"><button>Learn about the Dataform CLI</button></a></div>
