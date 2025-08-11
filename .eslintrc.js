let baseConfig = {};

try {
  /* Try to use the style guide if available */
  const { getESLintConfig } = require('@trafficbyintent/style-guide/typescript');
  baseConfig = getESLintConfig({
    allowConsoleError: true,
    allowConsoleWarn: true,
  });
} catch (error) {
  /* Fallback to basic ESLint configuration if style-guide is not available */
  baseConfig = {
    root: true,
    parser: '@typescript-eslint/parser',
    plugins: ['@typescript-eslint'],
    extends: [
      'eslint:recommended',
      'plugin:@typescript-eslint/recommended',
      'plugin:@typescript-eslint/recommended-requiring-type-checking',
    ],
    env: {
      node: true,
      es2021: true,
    },
  };
}

module.exports = {
  ...baseConfig,
  parserOptions: {
    project: './tsconfig.json',
    ...(baseConfig.parserOptions || {}),
  },
  rules: {
    ...(baseConfig.rules || {}),
    /* Temporarily disable import rules due to resolver conflicts */
    'import/no-unresolved': 'off',
    'import/namespace': 'off',
    'import/no-duplicates': 'off',
    'import/order': 'off',
    'import/default': 'off',
    'import/export': 'off',
    'import/no-named-as-default': 'off',
    'import/no-named-as-default-member': 'off',
    
    /* TypeScript rules - changed from warn to error for TXI compliance */
    '@typescript-eslint/no-explicit-any': 'error',
    '@typescript-eslint/no-unsafe-assignment': 'error',
    '@typescript-eslint/no-unsafe-member-access': 'error',
    '@typescript-eslint/no-unsafe-return': 'error',
    '@typescript-eslint/no-unsafe-argument': 'error',
    '@typescript-eslint/no-unsafe-call': 'error',
    '@typescript-eslint/strict-boolean-expressions': 'off',
    '@typescript-eslint/no-unnecessary-condition': 'off',
    '@typescript-eslint/no-unnecessary-type-assertion': 'error',
    '@typescript-eslint/require-await': 'error',
    '@typescript-eslint/await-thenable': 'error',
    '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
    
    /* Comment style - enforce multi-line comments per TXI standards */
    'multiline-comment-style': ['error', 'starred-block'],
  },
};