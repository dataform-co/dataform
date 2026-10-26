# Contributing

Dataform is a TypeScript project, using [Bazel](https://bazel.build) as a build tool. To scope out work, please [check existing issues (which includes feature requests)](https://github.com/dataform-co/dataform/issues) or [open a discussion thread](https://github.com/dataform-co/dataform/discussions)!

## Getting Started

First :fork_and_knife: [fork this repository](https://github.com/dataform-co/dataform/fork), clone it to your desktop, and navigate within.

### Requirements

#### [Bazel](https://bazel.build)

Bazel is a build system which we used to build the project and run tests.

The easiest way to install the correct Bazel version is through Bazelisk via [NPM](https://nodejs.org/en/download/):

```
npm i -g @bazel/bazelisk
```

### Run the CLI

You can run the project as you would the `npm` installation of `@dataform/cli`, but replace `dataform` with `./scripts/run`.

For example, to print out the default help information:

```bash
./scripts/run help
```

Check the [docs](https://cloud.google.com/dataform/docs/reference/dataform-cli-reference) for more examples.

_Note: If you are running Bazel on a **Mac**, this or any step that requires building may fail with a `Too many open files in system` error. This is [due to a limitation](https://github.com/angular/angular-bazel-example/issues/178) on the default maximum open file descriptors. You can increase the limit by running `sudo sysctl -w kern.maxfiles=<LARGE_NUMBER>` (we use `65536`)._

### Test

The following command runs tests for @dataform/core:

```bash
bazel test //core/...
```

### Unit Test

You can run all unit tests across the project by running this script:

```bash
./scripts/run_tests
```

### Integration Test

Integration tests run against BigQuery using [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials). No service account keys are stored in the repository.

1. **Authenticate with ADC:**

   ```bash
   gcloud auth application-default login
   ```

   Alternatively, you can set `GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json` in your environment.

   Don't add `--impersonate-service-account` here: the default test configuration already impersonates the test service account (see below).

2. **Required IAM roles:**

   By default, tests run in the `dataform-open-source` project as the `dataform-testing@dataform-open-source.iam.gserviceaccount.com` service account, which your ADC identity impersonates. This requires `roles/iam.serviceAccountTokenCreator` (Service Account Token Creator) on that service account, which members of the Dataform team have, and the IAM Service Account Credentials API (`iamcredentials.googleapis.com`) enabled in your ADC quota project.

   When running against your own GCP project (see the next step), your Google account (or service account) needs the following IAM roles on that project instead:
   - `roles/bigquery.jobUser` (BigQuery Job User)
   - `roles/bigquery.dataEditor` (BigQuery Data Editor)

3. **Configure the target GCP project (optional for `dataform-open-source` members):**

   By default, the committed `test_credentials/bigquery.default.json` points to `"projectId": "dataform-open-source"` and `"location": "US"`, and sets `"impersonateServiceAccount"` to the test service account.
   To run integration tests against your own GCP project without modifying tracked files, create a git-ignored `test_credentials/bigquery.json` file, which takes precedence over the default. If you already have a `test_credentials/bigquery.json` from the previous setup, it keeps working:

   ```json
   {
     "projectId": "my-gcp-project",
     "location": "US"
   }
   ```

   Without `"impersonateServiceAccount"`, tests run as your ADC identity. Add it to impersonate a service account of your own instead.

   _(Optional)_ If you still want to test with an explicit service account key inside the Dataform credentials file rather than ADC, you can include the `"credentials"` field containing the JSON key serialized as a string (`jq -Rsa < path/to/key.json`).

4. **Run the integration tests:**

   ```bash
   ./scripts/run_integration_tests
   ```

   To run a single integration test target directly, pass `--config=integration` so that your ADC environment is forwarded into the test sandbox:

   ```bash
   bazel test --config=integration //tests/integration:bigquery.spec
   ```

### Lint

Use the following command to check for any linting and formatting errors

```bash
./scripts/lint
```

All .ts files are expected to be formatted using prettier. You can format all files modified in your current branch by using this script:

```bash
./scripts/format
```

Alternatively, you may want to configure prettier to run on save or as a pre-commit hook.

### Canary test

In CI, a script builds the Core bundle the same way it would be prepared for publishing to npm, and attempts to use it to compile a sample project using Dataform's Google Cloud interface. This ensures changes to Core do not break the Cloud backend.

If you expect your changes to affect the output of the Cloud API's projects.locations.repositories.compilationResults.query endpoint, you will have to regenerate the baseline file that the output is compiled against. Use this script:

```bash
./scripts/regenerate_canary_baseline
```

The script uses dataform-open-source GCP project by default; to run it on your own project, you have to provide a tools/canary/config.local.env with the same variables as in the default tools/canary/config.env. The projects will need to include a Dataform repository and a Cloud Storage bucket to store the bundle in, as well as a service account with access to that bucket.

Once this file is provided, you can also run the canary test itself on your own project by using the script:

```bash
./scripts/run_canary_test
```

### Building

Building the CLI will build most of the required components.

```bash
bazel build cli
```

### Add New NPM Dependencies

Global pnpm installations will throw errors when installing packages, instead you should use:

```bash
$ bazel run -- @pnpm --dir "$PWD" add ...
```

Replace "$PWD" with path to the project directory if running from outside it. The --dir option is necessary, otherwise pnpm will install in Bazel's ephemeral output directory tree.

pnpm will use the registry specified in ~/.npmrc by default. When building in the Cloud, this will be set to a trusted internal registry. In local development, feel free to use a public registry like https://registry.npmjs.org/. Before committing, make sure pnpm-lock.yaml does not contain tarball: fields (pnpm only adds them when a registry serves packages from non-standard URLs, for example some mirrors or proxies, so you shouldn't normally see them with the public npm registry).

Additionally, installed NPM dependencies need to be added to the `deps` of `ts_library` rules by
prefixing them with `//:node_modules/...`.

## Windows Support

The Dataform CLI is supported on Windows 10 and 11 in both **Command Prompt (`cmd.exe`)** and **PowerShell**.

### Prerequisites & Setup

- **Node.js**: Node 22+ LTS is required.
- **Git line endings**: This repository includes a `.gitattributes` file (`* text=auto eol=lf`) that enforces LF line endings automatically without changing your global Git configuration. For user Dataform projects checked out with CRLF (`core.autocrlf=true`), `dataform format` and `dataform format --check` are CRLF-aware and normalize line endings before comparing.
- **Symlinks & Long Paths**: If using Bazel or deep `node_modules` hierarchies on Windows, enable [Windows Developer Mode](https://learn.microsoft.com/en-us/windows/apps/get-started/enable-your-device-for-development) or enable `core.longpaths` for this repository:
  ```bash
  git config core.longpaths true
  ```

### Development & Path Conventions

- **Canonical Forward Slashes**: Dataform's internal graph contracts, table names, and proto file paths require canonical POSIX forward slashes (`/`). Even on Windows, definitions must resolve as `definitions/my_table.sqlx` rather than with backslashes (`\`).
- **Cross-Platform Path Helpers**: Use the `Path` utility in `core/path.ts` (`Path.toPosixPath`, `Path.startsWithPath`, `Path.join`, `Path.normalize`, `Path.relativePath`, `Path.dirName`, `Path.basename`). It handles mixed separators and Windows drive letters (e.g. `C:\` vs `c:/`) automatically.

### Running Windows Smoke Tests

The smoke test suite lives in `cli/tests/windows/smoke_test.ts` and is bundled by Bazel into a
single file that only needs Node.js to run. Build it on Linux, macOS or WSL (or download the
`windows-smoke-test` artifact from a CI run of `.github/workflows/test.yaml`):

```bash
bazel build //cli/tests/windows:smoke_test_bundle //packages/@dataform/cli:package_tar //packages/@dataform/core:package_tar
# -> bazel-bin/cli/tests/windows/smoke_test_bundle.js
```

Then, on Windows, install the CLI globally and run the bundle through the shell you want to test:

- **In PowerShell**:

  ```powershell
  npm install -g path\to\cli\package.tar.gz
  node smoke_test_bundle.js --shell powershell --core-tarball path\to\core\package.tar.gz
  ```

- **In Command Prompt (`cmd.exe`)**:

  ```cmd
  npm install -g path\to\cli\package.tar.gz
  node smoke_test_bundle.js --shell cmd --core-tarball path\to\core\package.tar.gz
  ```

Without `--core-tarball`, the `@dataform/core` version pinned by `dataform init` is installed from
the npm registry instead. Without `--shell`, the CLI bundle is run directly with Node (bypassing
the `dataform.cmd` launcher), which also works on Linux and macOS.

### Windows CI on GitHub Actions

All pull requests run the `CLI smoke tests (Windows, Node 22)` and
`CLI smoke tests (Windows, Node 24)` jobs on `windows-latest` via `.github/workflows/test.yaml`.
Each installs the CLI tarball built by the Linux job and runs the smoke tests under both PowerShell
and `cmd.exe`, with `@dataform/core` installed from the locally built tarball.

## The Contribution Process

1. Decide on what you'd like to contribute. The majority of open-source contributions come from:

   1. Someone deciding they want a feature that is not currently present, and which isn't a priority for the team.

   1. Embracing the community aspect of open source (or getting that commit count up) and solving an issue.

1. Plan out the change, and whether it is feasible.

   1. If you're unsure of the scope of the change, then ask in the issue or [create a discussion](https://github.com/dataform-co/dataform/discussions).

   1. We'd much prefer multiple smaller code changes than a single large one.

   1. Avoid changing core functionality over a long time frame. Our development process is very dynamic, so if your code depends on lots of other parts of the project, then it is likely to be out of date by the time you finish!

   1. If you're solving an issue, be sure to comment to make it known that you are currently solving it. Unless we have worked with you before, it is unlikely that we will lock the issue to you.

1. Begin materialising your masterpiece.

   1. Create a feature branch based on `main` ([link](https://github.com/dataform-co/dataform/tree/main)) for development work.

1. Once done, review your code, run the tests, **[check for common mistakes](#common-pull-request-mistakes)** and then open a pull request.

   1. Tidy the code by removing erronous log statements. Comment difficult to interpret sections. Make sure functions are names appropriately. We will review the pull request mainly by the git difference.

   1. Assign a reviewer. Pick anyone on the team who seems to contribute a lot and they will refer it onto whoever is most responsible for the given subsystem.

1. Discuss and process any changes requested.

   1. It's unlikely your pull request will be perfect immediately; there will likely be some changes requested, whether it's to do with style or a more fundamental issue.

   1. The automated integration tests must pass.

   1. Once a pull request is accepted and all automated integration tests are passing, we will merge it for you.

### Reporting Issues (!)

Another way we'd love for you to contribute is by flagging any issues you find. First check through the list of [existing issues](https://github.com/dataform-co/dataform/issues) for anything similar, in order to avoid duplicates. If not, then full steam ahead!

### Promoting Dataform

If you're using Dataform for interesting projects then please let people know! Reach out to [dataform-preview@google.com](dataform-preview@google.com) for marketing support.

### Common Pull Request Mistakes

1. Is it too long? Small pull requests are easier to review and merge. If you are planning on making a larger change, then talk to the team and write a document on the design.

1. Have you appropriately increased test coverage? If the operation of the change is not already tested, then tests will need to be written.

1. Is it a hack? Does it solve the problem, but it is not reliable, reproducable or extendable? In other words, [does it smell](https://en.wikipedia.org/wiki/Code_smell)?

1. Have you changed whitespace or touched unrelated code? Please avoid this as it makes pull requests far more difficult to review.

1. Are the comments useful, and is the code readable? Are the function and variable names appropriate?
