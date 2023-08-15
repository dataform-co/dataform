import { compile } from "df/sandbox/vm/compile";
import { decode64, encode64 } from "df/common/protos";
import { dataform } from "df/protos/ts";
import {createWriteStream} from "fs";

// Disable any and all logging

(console as any).log = (a: any) => {};

const out = createWriteStream(null, {fd: 1});

const args = process.argv;

const base64EncodedConfig = args[3];
const request = decode64(dataform.CompileConfig, base64EncodedConfig);

//var compiled = compile(request);

var compiled: string = compile({
    projectDir: "/usr/local/google/home/lewishemens/workspace/dataform-data"
})

for (var i = 0; i < compiled.length; i+= 65536) {
    out.write(compiled.substring(i, Math.min(compiled.length, i + 65536)));
}
