def css_typings(name, srcs, path):
  # Only create outputs for css files.
  outs = [f.replace(".css", ".css.d.ts") for f in srcs if f[-4:] == ".css"]
  if (len(outs) == 0):
    fail("No .css files found, check your srcs")
  # Some little hacks in here to work with nested workspaces and the tcm CLI.
  native.genrule(
    name = name,
    srcs = srcs + [":BUILD"],
    outs = outs,
    cmd = """$(location //:tcm) --silent -o $(GENDIR) -p "$$(dirname $(location :BUILD))/**/*.css" """,
    tools = ["//:tcm"])
