module.exports = {
  foo: value => `foo_${value}`,
  deferredPublish: (name, arg) => publish(name, arg)
};
