---
title: Using version control
---

## Introduction

Dataform uses the Git version control system to maintain a record of each change made to project files and to manage file versions. Each Dataform project has an associated Git repository.

By default, Dataform will manage your project's Git repository for you. However, projects can be configured to use GitHub instead by clicking the `Migrate project to GitHub` button in Settings.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-primary" markdown="1">
  Version control is tightly integrated into Dataform Web and has consequences on how your data
  pipelines run. We strongly recommend understanding the following concepts before developing your
  datasets in Dataform.
</div>

## Branches

One of the main benefits of Git is that a developer can work in an isolated "branch" of the file repository without affecting the base files. Dataform has full support for Git branches.

From any non-`production` branch you can edit files (and run those edited scripts on your warehouse, if desired).

Once you are happy with your changes, you can commit them and push them to the `master` branch. If at any point you make changes that you don't want to commit, you can revert your branch to its last commit.

Note that the `production` branch is not directly editable and **all schedules run from the production branch**. This enables users to develop and test changes without affecting the project's data pipelines or affecting other users.

### Personal and shared branches

When you first use Dataform we will automatically create a personal development branch for you, `name_dev`. Only you can see this branch and commit to it.

It's also possible to create a shared branch, which everyone in the project can see and commit to. These can be useful if you're collaborating with someone on a project.

### Creating and deleting branches

To create a new branch, open the branch selection menu and click "New Branch". You will have the option to make it a shared branch.

To delete a branch, first open the branch. Then, from the version control settings, choose the `Delete branch` option.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1">
If you delete a branch, any changes which have not yet been pushed to `production` will be lost.
</div>

## How to use version control

Select the branch you would like to use to make your changes.

<div className="bp3-callout bp3-icon-info-sign bp3-intent-warning" markdown="1" style={{ marginBottom: "10px" }}>
  We recommend that each team member uses their own separate development branch.
</div>

After making changes to files, you can commit them. You will be shown a list of the modified files, and asked to provide a commit message.

Once you are happy with your changes, you can `Push to master`. The changes will now be on `production` and used for future schedule runs.

## Reverting uncommitted changes

If you'd like to undo any changes you've made since your last commit, you can `Revert uncommitted changes` from the version control menu.

## Resolving merge conflicts

If other changes have been pushed to the `master` branch while you were making edits in your branch, you will need to pull those changes into your branch first by choosing `Pull from master`.

There may be conflicting edits that you will have to manually resolve. Files with conflicts are highlighted in the files list. Once you are comfortable with your edits, you can commit and push your changes to `master`.
