def css_typings(name, srcs, path):
  outs = [f.replace(".css", ".css.d.ts") for f in srcs]
  native.genrule(
    name = name,
    srcs = srcs,
    outs = outs,
    cmd = """./$(location //:tcm) -o $(GENDIR) -p "%s/**/*.css" """ % path,
    tools = ["//:tcm"])
