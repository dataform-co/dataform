---
title: Packages
priority: 6
---

Dataform packages are collections of code that help other analysts work with data. Some packages help model specific datasets (for example the [Segment package](https://docs.dataform.co/packages/dataform-segment)), and other packages make helpful utility functions available for others to use (soon, watch this space).

To see how packages work, watch this [short video](https://www.loom.com/share/fdfa25dcdc8544e38fe844199b970f87).

## Creating a package

Creating your own package is relatively easy, as long as you're relatively familiar with the Dataform framework. It may also help to understand the fundamentals of JavaScript, but you can probably muddle through without this!

### Clone the base package repo

[This repo](https://github.com/dataform-co/dataform_package_base) contains the building blocks of a package:
- index.js
- example.js
- README.md

To get started, clone this repo into a location of your choice. Make sure the repo is public (if you'd like to share it with others).

### Optional: Import the cloned repo to Dataform Web

To make development of your package easier, you may want to import it to Dataform web. This will give you real-time errors and warnings, and a visual representation of the data catalog and dependency graph.

To import the repo, follow [this link](https://app.dataform.co/#/importgitrepo).

### Update the base repo to add your packages functionality

The base package repo creates a simple dependency graph with one declaration and two chained tables reading from that declaration. Explore the files in the package to get an idea of how it's put together. Then, start making the changes to create the functionality you'd like to create in your package. At a minimum, you should update:
- README.md
- index.js
- example.js
- includes/dataset_one.js
- includes/dataset_two.js

If you're not quite sure how to set your package up, you may want to explore the code respoitories for other Dataform packages. Alternatively, ask the community in our [Slack channel](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A).

### Connect to a data warehouse and test it out

Once you're done, it's a good idea to connect to a data warehouse and make sure it's doing what you expected.

### Release to the community!

Once your package is ready to be released, let the community know in our [Slack channel](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A). And if you'd like to list your package on docs.dataform.co, send an email to the [Dataform team](mailto:team@dataform.co).

## Questions?

If you have any questions at any point, don't hesitate to get in touch by [email](mailto:team@dataform.co) or [Slack](https://join.slack.com/t/dataform-users/shared_invite/zt-dark6b7k-r5~12LjYL1a17Vgma2ru2A).

On behalf of the Dataform community, thanks in advance for your contribution!
