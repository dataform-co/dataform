const macros = require("includes/subdirectory/macros");

macros.deferredPublish("example_deferred", "select 1 as test").hermetic(false);
