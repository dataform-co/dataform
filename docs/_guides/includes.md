---
layout: documentation
title: Includes
---

Javascript files can be added in the `includes/` folder to define simple scripts, constants or macros that can promote reuse of code across your project.

Each file in the includes folder will be made available to be used within your other SQL or JavaScript files.

For example, the following includes file defines a constant and a function that can be used within other scripts:

```js
// includes/utils.js

// Define a constant.
const PROJECT_NAME = "my_project_name";

// Define a function that prefixes the project name to a string.
function prefixProjectName(value) {
  return PROJECT_NAME + "_" + value;
}

// Export the constant and the function so they can be used.
module.exports = { PROJECT_NAME, prefixProjectName };
```

You can use these functions in your scripts, and they will be automatically made available under the name of the file they where defined in, in this case `utils`:

```js
// models/example.sql
select 1 as ${utils.prefixProjectName("test")}
```

Which get's compiled to:

```sql
select 1 as my_project_name_test
```
