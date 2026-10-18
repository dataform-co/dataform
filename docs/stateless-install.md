# Stateless installs

[<- Back to Home](https://dataform-co.github.io/dataform).

When `dataformCoreVersion` is set in `workflow_settings.yaml`, the CLI doesn't use a `node_modules` directory in the project. Instead, each time it compiles the project, it:

1. copies the project to a temporary directory,
2. writes a `package.json` there that depends on `@dataform/core` at that version,
3. runs `npm i` in the copy,
4. compiles the copy, and
5. deletes the copy.

The project itself must not contain `package.json`, `package-lock.json` or `node_modules`.

The copy uses ignore rules to skip potentially large directories, such as `.venv` or build output. This page describes what it includes.

## Always copied

- `workflow_settings.yaml` (or `dataform.json`).
- `.npmrc`, which configures the `npm i` in the copy.
- Everything under `definitions/` and `includes/`, including nested `node_modules` directories, but not `.git` directories.
- Anything those reach through symbolic links that resolve inside the project, along with the directories the links need in order to resolve. For example, with `definitions -> generated`, `generated/` is copied even if it's gitignored. A link to the project root (`definitions -> .`) makes the whole project reachable, so everything except `.git` is copied.

Ignore files can't exclude any of these.

## Always excluded

Outside the locations above, `.git` and `node_modules` directories are never copied. Ignore files can't re-include them.

## `.gitignore` and `.dataformignore`

Everything else is filtered by the `.gitignore` in the project root, then by the `.dataformignore` in the project root, if either exists.

- `.dataformignore` uses the same syntax as `.gitignore`. Because it's applied second, it can exclude more paths, or re-include gitignored ones with a negation:

  ```
  # Copy the SQL that definitions/actions.yaml references.
  !/queries/
  ```

  It doesn't change what git ignores. A final `!*` rule in `.dataformignore` turns off exclusions from both files, leaving only `.git` and `node_modules` excluded.

- As in git, a path can't be re-included while its parent directory is excluded. If `build/` is ignored, `!/build/ca.pem` has no effect; re-include the directory with `!/build/` instead, and exclude what you don't need inside it.
- As in git, a pattern ending in `/` matches directories only, not a symbolic link to a directory. With `.venv/` in `.gitignore`, a `.venv` symlink is copied as a link.
- Only the two files in the project root are read. Nested `.gitignore` files, `.git/info/exclude` and global git excludes are not, so the copy can differ from what git ignores.
- Git's index is not consulted: a tracked file that matches a pattern (for example, one added with `git add -f`) is still excluded.
- Patterns match case-insensitively on Windows and macOS, and case-sensitively elsewhere.

With `--verbose`, the CLI prints which ignore files it applied.

## When a skipped file is needed

A file outside `definitions/` and `includes/` that an ignore file excludes isn't available during installation or compilation. For example:

- a SQL file that `actions.yaml` references outside `definitions/` (`filename: ../queries/example.sql`),
- a helper that a definition loads with `require()`, or
- a CA file that `.npmrc` names by a relative path (`cafile=.certs/company-ca.pem`).

Compilation or installation then fails, even though the file is in the project. If project code catches the failed load, it carries on, possibly with different behavior.

When the CLI can tell that a missing module was excluded from the copy, it prints a note alongside the error:

```
Note: 'queries' is in the project but not in the copy compiled for dataformCoreVersion, because .gitignore or .dataformignore excludes it. If module 'queries/example.sql' is there, re-include it in .dataformignore.
```

To make such a file available, re-include it in `.dataformignore`, or move it under `definitions/` or `includes/`.
