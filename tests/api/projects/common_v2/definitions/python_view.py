import dataform

dataform.publish("python_view", { "hermetic": True }).query("select 1 from test")
