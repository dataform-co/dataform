import { expect } from "chai";

import { ProjectConfigOptions } from "df/cli/project_config_options";
import { generateReadme } from "df/cli/readme_generator";
import { ICommandBase } from "df/cli/yargswrapper";
import { suite, test } from "df/testing";

const projectDirPositional = {
  name: "project-dir",
  option: { describe: "The Dataform project directory.", default: "." },
};

function command(overrides: Partial<ICommandBase> = {}): ICommandBase {
  return {
    format: "build [project-dir]",
    description: "Build the project.",
    positionalOptions: [projectDirPositional],
    options: [
      {
        name: "fast",
        option: { describe: "Builds faster.", type: "boolean", default: false },
      },
    ],
    processFn: async () => 0,
    ...overrides,
  };
}

function block(name: string, ...content: string[]): string {
  return [`<!-- BEGIN GENERATED: ${name} -->`, ...content, `<!-- END GENERATED: ${name} -->`].join(
    "\n",
  );
}

// A minimal README with every required block for the given command names, all empty.
function readmeFor(names: string[], extra = ""): string {
  return [
    "# CLI",
    block("commands"),
    ...names.map((name) =>
      [
        `### \`${name}\``,
        block(`usage ${name}`),
        block(`positionals ${name}`),
        block(`options ${name}`),
      ].join("\n"),
    ),
    "## Shared project-config flags",
    block("project-config-options"),
    extra,
  ].join("\n");
}

function generatedBlock(content: string, name: string): string {
  const begin = `<!-- BEGIN GENERATED: ${name} -->\n`;
  const start = content.indexOf(begin) + begin.length;
  return content.slice(start, content.indexOf(`<!-- END GENERATED: ${name} -->`, start)).trimEnd();
}

suite("readme generator", () => {
  test("generates the command list", () => {
    const { content } = generateReadme(readmeFor(["build", "init-creds"]), [
      command(),
      command({ format: "init-creds [project-dir]" }),
    ]);
    expect(generatedBlock(content, "commands")).equals(
      "- [`build`](#build)\n- [`init-creds`](#init-creds)",
    );
  });

  test("generates the usage line", () => {
    const { content } = generateReadme(readmeFor(["build"]), [command()]);
    expect(generatedBlock(content, "usage build")).equals("```\ndataform build [project-dir]\n```");
  });

  test("generates the positionals table", () => {
    const required = {
      name: "target",
      option: { describe: "The target." },
      docs: { required: true },
    };
    const optional = { name: "extra", option: { describe: "Extra." } };
    const { content } = generateReadme(readmeFor(["build"]), [
      command({ positionalOptions: [projectDirPositional, required, optional] }),
    ]);
    expect(generatedBlock(content, "positionals build")).equals(
      [
        "| Argument | Required | Description |",
        "|---|---|---|",
        "| `project-dir` | No (default `.`) | The Dataform project directory. |",
        "| `target` | **Yes** | The target. |",
        "| `extra` | No | Extra. |",
      ].join("\n"),
    );
  });

  test("generates the options table", () => {
    const { content } = generateReadme(readmeFor(["build"]), [
      command({
        options: [
          { name: "fast", option: { describe: "Builds faster.", type: "boolean", default: false } },
          { name: "retries", option: { describe: "Retries.", type: "number", default: 3 } },
          { name: "out", option: { describe: "Output file.", type: "string", default: null } },
          { name: "credentials", option: { describe: "Credentials.", default: "creds.json" } },
          {
            name: "timeout",
            option: { describe: "Timeout.", type: "string", default: null },
            docs: { default: "`'60s'`" },
          },
        ],
      }),
    ]);
    expect(generatedBlock(content, "options build")).equals(
      [
        "| Flag | Type | Default | Description |",
        "|---|---|---|---|",
        "| `--fast` | boolean | `false` | Builds faster. |",
        "| `--retries` | number | `3` | Retries. |",
        "| `--out` | string | — | Output file. |",
        "| `--credentials` | — | `creds.json` | Credentials. |",
        "| `--timeout` | string | `'60s'` | Timeout. |",
      ].join("\n"),
    );
  });

  test("collapses project-config options into a single row", () => {
    const { content } = generateReadme(readmeFor(["build"]), [
      command({
        options: [
          { name: "fast", option: { describe: "Builds faster.", type: "boolean" } },
          ...ProjectConfigOptions.allYargsOptions,
        ],
      }),
    ]);
    expect(generatedBlock(content, "options build")).equals(
      [
        "| Flag | Type | Default | Description |",
        "|---|---|---|---|",
        "| `--fast` | boolean | — | Builds faster. |",
        "| Project-config overrides | — | — | See [Shared project-config flags](#shared-project-config-flags). |",
      ].join("\n"),
    );
    const sharedTable = generatedBlock(content, "project-config-options");
    for (const option of ProjectConfigOptions.allYargsOptions) {
      expect(sharedTable).includes(`| \`--${option.name}\` |`);
    }
  });

  test("renders commands without options or positionals", () => {
    const { content } = generateReadme(readmeFor(["build"]), [
      command({ positionalOptions: [], options: [] }),
    ]);
    expect(generatedBlock(content, "positionals build")).equals("None.");
    expect(generatedBlock(content, "options build")).equals("None.");
  });

  test("escapes table cells", () => {
    const { content } = generateReadme(readmeFor(["build"]), [
      command({
        options: [{ name: "mode", option: { describe: "Either a|b,\nor c.", type: "string" } }],
      }),
    ]);
    expect(generatedBlock(content, "options build")).includes(
      "| `--mode` | string | — | Either a\\|b, or c. |",
    );
  });

  test("escapes HTML in descriptions", () => {
    const { content } = generateReadme(readmeFor(["build"]), [
      command({
        options: [
          {
            name: "prefix",
            option: { describe: "Uses the form dataform-<prefix>- & more.", type: "string" },
            docs: { default: "`<none>`" },
          },
        ],
      }),
    ]);
    expect(generatedBlock(content, "options build")).includes(
      "| `--prefix` | string | `<none>` | Uses the form dataform-&lt;prefix&gt;- &amp; more. |",
    );
  });

  test("leaves hand-written content untouched and replaces stale blocks", () => {
    const readme = readmeFor(["build"], "Hand-written\r\nnotes.").replace(
      block("options build"),
      block("options build", "| stale |"),
    );
    const { content, changedBlocks } = generateReadme(readme, [command()]);
    expect(content).includes("# CLI\n");
    expect(content).includes("Hand-written\nnotes.");
    expect(content).not.includes("| stale |");
    expect(changedBlocks).deep.equals([
      "commands",
      "usage build",
      "positionals build",
      "options build",
      "project-config-options",
    ]);
  });

  test("is idempotent", () => {
    const first = generateReadme(readmeFor(["build"]), [command()]);
    const second = generateReadme(first.content, [command()]);
    expect(second.content).equals(first.content);
    expect(second.changedBlocks).deep.equals([]);
  });

  test("fails for a command without a README section", () => {
    expect(() =>
      generateReadme(readmeFor(["build"]), [command(), command({ format: "deploy" })]),
    ).to.throw(/missing the generated block\(s\).*"deploy" command[\s\S]*### `deploy`/);
  });

  test("fails for a block referring to an unknown command", () => {
    expect(() => generateReadme(readmeFor(["build", "removed"]), [command()])).to.throw(
      /refers to the unknown command "removed"/,
    );
  });

  test("fails for an unknown block kind", () => {
    expect(() =>
      generateReadme(readmeFor(["build"], block("examples build")), [command()]),
    ).to.throw(/unknown generated block "examples build"/);
  });

  test("fails for malformed markers", () => {
    expect(() =>
      generateReadme(readmeFor(["build"], "<!-- BEGIN GENERATED: commands -->"), [command()]),
    ).to.throw(/duplicate generated block "commands"/);
    expect(() =>
      generateReadme(readmeFor(["build"], "<!-- BEGIN GENERATED: dangling -->"), [command()]),
    ).to.throw(/"BEGIN GENERATED: dangling" is never closed/);
    expect(() =>
      generateReadme(readmeFor(["build"], "<!-- END GENERATED: commands -->"), [command()]),
    ).to.throw(/doesn't match any BEGIN GENERATED marker/);
    expect(() =>
      generateReadme(
        readmeFor(["build"], "<!-- BEGIN GENERATED: a -->\n<!-- BEGIN GENERATED: b -->"),
        [command()],
      ),
    ).to.throw(/found "BEGIN GENERATED: b" before "END GENERATED: a"/);
  });

  test("fails when a required shared block is missing", () => {
    expect(() =>
      generateReadme(readmeFor(["build"]).replace(block("commands"), ""), [command()]),
    ).to.throw(/no "commands" generated block/);
  });
});
