# Packages

[<- Back to Home](https://dataform-co.github.io/dataform).

## Sample Packages

- [https://github.com/dataform-co/dataform-scd](https://github.com/dataform-co/dataform-scd).
- [https://github.com/ArtemKorneevGA/dataform-ga4-sessions](https://github.com/ArtemKorneevGA/dataform-ga4-sessions).
- [https://github.com/mokhahmed/dvform](https://github.com/mokhahmed/dvform).
- [https://github.com/dataform-co/dataform-fivetran-log](https://github.com/dataform-co/dataform-fivetran-log).
- [https://github.com/dataform-co/dataform-segment](https://github.com/dataform-co/dataform-segment).
- [https://github.com/dataform-co/dataform-fivetran-stripe](https://github.com/dataform-co/dataform-fivetran-stripe).

## Creating a package

Creating your own package is relatively easy, as long as you're relatively familiar with the Dataform framework. It may also help to understand the fundamentals of JavaScript, but you can probably muddle through without this!

### Clone the base package repo

[This repo](https://github.com/dataform-co/dataform-package-base) contains the building blocks of a package:

- index.js
- example.js
- README.md

To get started, clone this repo into a location of your choice. Make sure the repo is public (if you'd like to share it with others).

### Update the base repo to add your packages functionality

The base package repo creates a simple dependency graph with one declaration and two chained tables reading from that declaration. Explore the files in the package to get an idea of how it's put together. Then, start making the changes to create the functionality you'd like to create in your package. At a minimum, you should update:

- README.md
- index.js
- example.js
- includes/dataset_one.js
- includes/dataset_two.js

### Connect to a data warehouse and test it out

Once you're done, it's a good idea to connect to a data warehouse and make sure it's doing what you expected.

### Release to the community!

Once your package is ready to be released, let [the community know](https://cloud.google.com/dataform/docs/get-support#get_support_from_the_community). If you'd like to list your package here, make a pull request to add it!
