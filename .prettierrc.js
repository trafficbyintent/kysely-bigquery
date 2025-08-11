let prettierConfig;

try {
  /* Try to use the style guide if available */
  prettierConfig = require('@trafficbyintent/style-guide/typescript/prettier');
} catch (error) {
  /* Fallback to basic Prettier configuration if style-guide is not available */
  prettierConfig = {
    semi: true,
    trailingComma: 'es5',
    singleQuote: true,
    printWidth: 100,
    tabWidth: 2,
    useTabs: false,
    arrowParens: 'avoid',
    endOfLine: 'lf',
  };
}

module.exports = prettierConfig;