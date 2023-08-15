import { compile, compileAndSend } from "df/sandbox/vm/compile";

console.log(process.argv);

compileAndSend(process.argv[2], process.argv[3]);