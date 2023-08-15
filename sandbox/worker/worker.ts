import { compile } from "df/sandbox/vm/compile";

var compiled = compile({
    projectDir: "/usr/local/google/home/lewishemens/workspace/dataform-data",
})

console.log(compiled);