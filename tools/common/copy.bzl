def copy_file(name, src, out):
    native.genrule(
        name = name,
        srcs = [src],
        outs = [out],
        cmd = "cp -r $(SRCS) $(OUTS)",
    )
