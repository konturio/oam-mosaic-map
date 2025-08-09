module.exports = {
  testRegex: "(/__tests__/.*|(\\.|/)(test|spec))\\.(mjs?|js?)$",
  transform: {},
  // Skip integration tests requiring network access
  testPathIgnorePatterns: ["/__tests__/mosaic.mjs"],
  moduleFileExtensions: ["js", "jsx", "mjs"],
};
