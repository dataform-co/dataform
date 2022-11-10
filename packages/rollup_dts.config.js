import dts from "rollup-plugin-dts";

export default {
  plugins: [
    dts({
      respectExternal: true
    })
  ]
};
