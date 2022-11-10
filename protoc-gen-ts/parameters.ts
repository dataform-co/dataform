export interface IGeneratorParameters {
  importPrefix?: string;
}

export function parseGeneratorParameters(parameters: string): IGeneratorParameters {
  const parsed: IGeneratorParameters = {};
  parameters.split(",").forEach(parameter => {
    const [key, value] = parameter.split("=");
    if (key === "import_prefix") {
      parsed.importPrefix = value;
    }
  });
  return parsed;
}
