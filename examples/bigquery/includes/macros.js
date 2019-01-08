module.exports = {
  foo: value => `foo_${value}`,
  deferredMaterialize: (name, arg) => materialize(name, arg)
};
