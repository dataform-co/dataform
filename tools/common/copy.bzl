def copy_file(name, src, out):
    native.genrule(
        name = name,
        srcs = [src],
        outs = [out],
        cmd = "cp -r $(SRCS) $(OUTS)",
    )


def copy_files(name, map):
    for key, value in map.items():
        native.genrule(
            name = "copy_" + value,
            srcs = [key],
            outs = [value],
            cmd = "cp $(SRCS) $(OUTS)",
        )
    native.filegroup(
        name = name,
        srcs = map.values()
    )
