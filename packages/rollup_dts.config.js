import dts from "rollup-plugin-dts";

export default {
  plugins: [
    dts({
      respectExternal: true,
      compilerOptions: {
        baseUrl: process.cwd(),
        paths: { "df/*": ["*"] },
      },
    }),
  ],
};
