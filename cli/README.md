<p align="center">
  <img src="https://raw.githubusercontent.com/dataform-co/dataform/master/static/images/github_bg.png">
</p>
<p align="center">
  The Dataform SDK is a tool for managing SQL based data operations in your warehouse.
</p>
<div align="center">
  <img src="https://storage.googleapis.com/dataform-cloud-build-badges/build/status.svg" alt="Cloud build status"/>
  <a href="https://www.npmjs.com/package/@dataform/cli"><img src="https://badge.fury.io/js/%40dataform%2Fcli.svg" alt="NPM package version" /></a>
  <a href="https://www.npmjs.com/package/@dataform/cli"><img alt="npm" src="https://img.shields.io/npm/dm/@dataform/cli.svg" alt="Monthly downloads" /></a>
</div>
<div align="center">
  <img src="https://david-dm.org/dataform-co/dataform.svg" alt="NPM dependency status" />
  <!-- <img src="https://slack.dataform.co/badge.svg" alt="Dataform Slack" /> -->
  <img src="https://img.shields.io/github/license/dataform-co/dataform.svg" alt="License information" />
</div>
<div align="center">
  <!-- <a href="https://twitter.com/dataform"><img src="https://img.shields.io/twitter/follow/dataform.svg?style=social" alt="Follow Dataform on Twitter" /></a> -->
</div>

# Intro

The Dataform SDK is an open source framework for managing SQL based data operations in your warehouse. It helps you pilot the T in ELT, with SQL code, and brings engineering best practices to analytics pipeline management.
<br/>
<br/>

<p align="center">
  <img src="https://raw.githubusercontent.com/dataform-co/dataform/master/static/images/datastack.svg">
</p>

# How it works

- You **develop** datasets and assertions in SQL with Dataform’s built-in templating syntax and APIs
- Dataform **compiles** your project into a DAG (Directed Acyclic Graph) of every action in SQL
- Dataform connects to your warehouse and **runs** the DAG or just the actions that you specify
- You receive detailed execution **logs**

# Get started

## With the CLI

You can install the Dataform SDK using the following command line. Follow the [docs](https://docs.dataform.co/guides/command-line-interface/) to get started.

```
npm i -g @dataform/cli
```

<br/>

<img width="700" src="https://raw.githubusercontent.com/dataform-co/dataform/master/static/images/gif.gif">

## Documentation

Documentation is available [here](https://docs.dataform.co/)


For more information please check the github [repository](https://github.com/dataform-co/dataform).
