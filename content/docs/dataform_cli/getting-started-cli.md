---
title: Getting started with the CLI
---

The CLI enables you to initialize, compile test and run Dataform projects directly from your local machine or as part of other systems.

## Installation

The Dataform CLI can be installed using <a target="_blank" rel="noopener" href="https://www.npmjs.com/get-npm">NPM</a>:

```bash
npm i -g @dataform/cli
```

## Create a new project

To create a new `bigquery`, `postgres`, `redshift`, `snowflake`, or `sqldatawarehouse` project in the `new_project` directory, run the respective command:

```bash
dataform init bigquery new_project --gcloud-project-id<your-google-cloud-project-id>
- or -
dataform init postgres new_project
- or -
dataform init redshift new_project
- or -
dataform init snowflake new_project
- or -
dataform init sqldatawarehouse new_project
```

### Project structure

Change directory into the newly-created `new_project` directory and take a look at your newly created project files:

```bash
cd new_project
ls
```

You should see the following structure:

```bash
project-dir
├── definitions
├── includes
├── package.json
└── dataform.json
```

## Define a dataset

The `definitions/` directory should be used for files that define [tables](datasets), [assertions](assertions), and [operations](operations).

To create a new dataset, create a new file `definitions/example.sqlx`:

```bash
echo "config { type: 'view' } SELECT 1 AS test" > definitions/example.sqlx
```

## Compile your code

To check that your Dataform code compiles, run the `compile` command at the root of your project directory to get JSON output of the compiled project:

```bash
dataform compile
```

You should see output similar to the following:

```bash
Compiling...

Compiled 1 action(s).
1 dataset(s):
  dataform.example [view]
```

## Create a credentials file

Dataform requires a credentials file in order to connect to your warehouse. Run the `init-creds` command and Dataform will guide you through credentials file creation:

```bash
dataform init-creds bigquery
- or -
dataform init-creds postgres
- or -
dataform init-creds redshift
- or -
dataform init-creds snowflake
- or -
dataform init-creds sqldatawarehouse
```

A `.df-credentials.json` file will be written to disk containing your provided details.

Check out our [data warehouse setup guide](../how_to_guides/dataform_web/set_up_datawarehouse) if you need help with the `init-creds` wizard.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
  If using a source control system, we strongly recommend that you do not commit the{" "}
  <code>.df-credentials.json</code> file to your repository in order to protect these access
  credentials.
</div>

## Run your code

In order to run your code, Dataform needs to access your data warehouse in order to determine its current state and tailor the resulting
SQL accordingly. If you'd like to see the final SQL that Dataform will run on your warehouse without actually running it, you can perform a dry run:

```bash
dataform run --dry-run
```

You should see something similar to the following:

```bash
Compiling...

Compiled successfully.

Dry run (--dry-run) mode is turned on; not running the following actions against your warehouse:

1 dataset(s):
  dataform.example [table]
```

Removing the `--dry-run` option will result in the SQL being run in your warehouse:

```bash
dataform run
```

The `run` command's output will now include the run's execution status, including any errors encountered during the run:

```bash
Compiling...

Compiled successfully.

Running...

Dataset created:  dataform.example [view]
```

## Get help

In addition to this guide, you can run the `help` command to get a short description of any Dataform command or option. For example, you can type:

```bash
dataform help
```

This will list all of the available commands and options:

```bash
Commands:
  dataform help [command]                                 Show help. If [command] is specified, the help is for the given command.
  dataform init <warehouse> [project-dir]                 Create a new dataform project.
  dataform init-creds <warehouse> [project-dir]           Create a .df-credentials.json file for dataform to use when accessing your warehouse.
  dataform compile [project-dir]                          Compile the dataform project. Produces JSON output describing the non-executable graph.
  dataform test [project-dir]                             Run the dataform project\'s unit tests on the configured data warehouse.
  dataform run [project-dir]                              Run the dataform project\'s scripts on the configured data warehouse.
  dataform listtables <warehouse>                         List tables on the configured data warehouse.
  dataform gettablemetadata <warehouse> <schema> <table>  Fetch metadata for a specified table.

Options:
  --help     Show help  [boolean]
  --version  Show version number  [boolean]
```

If you want to get help for a specific command, you can type:

```bash
dataform help compile
```

You should see something similar to the following:

```bash
dataform compile [project-dir]

Compile the dataform project. Produces JSON output describing the non-executable graph.

Positionals:
  project-dir  The Dataform project directory.  [default: \".\"]

Options:
  --help           Show help  [boolean]
  --version        Show version number  [boolean]
  --watch          Whether to watch the changes in the project directory.  [boolean] [default: false]
  --schema-suffix  A suffix to be appended to output schema names.
  --verbose        If true, the full contents of command output will be output (containing fully compiled SQL, etc).  [boolean] [default: false]
```

## Next steps

You have now seen how easy it is to use Dataform to publish simple datasets. Next, how about [publishing a dataset](../how_to_guides/datasets)?
