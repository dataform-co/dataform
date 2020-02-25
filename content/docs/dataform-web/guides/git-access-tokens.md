---
title: Git access tokens
---

Git access tokens are important for Dataform projects linked to remote git providers, such as GitHub. They provide authentication for accessing and controlling the git project.

## User authentication 🙋‍♂️🙋‍♀️

User access tokens are used to authenticate git access of Dataform projects during project manipulation.

Without a valid access token, users will be unable to do things such as create branches.

They must be configured to have both **read and write access** to your remote repository.

They can be added or removed on the **user settings page**.

## Scheduler authentication ⏱

Scheduler access tokens are used to authenticate git access of Dataform projects when running automated schedules.

Without a valid access token, the scheduler will not be able to read the contents of the project and perform the run.

They must have **read** access to your remote repository.

They can be updated on the **project settings page**, within the app.

For smaller projects (e.g. 5 or fewer people), it's ok to use a user's personal access token to authenticate the scheduler, but for larger teams a **bot account** is recommended.

### Bot accounts

Bot accounts are generally regular user accounts, but used purely to run automated (or triggered) tasks.

Some of the advantages of using a bot account include:

- Highly configurable access privileges, which can prevent misbehavior.

- It becomes easier to distinguish between user actions and automated ones.

Before making a bot account on GitHub, please read the [TOCs](https://help.github.com/en/github/site-policy/github-terms-of-service#3-account-requirements).

## Creating git access tokens

How to create a git access token differs between remote git providers.

[Creating a **GitHub** access token](https://help.github.com/en/github/authenticating-to-github/creating-a-personal-access-token-for-the-command-line#creating-a-token).

For providers other than GitHub (e.g. GitLab), the access token name must be `dataform`, and it must have the **api** permission.

[Creating a **GitLab** access token](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html#creating-a-personal-access-token).
