const fs = require('fs');
const path = require('path');

let extendsPath = './tsconfig.base.json';

/* Check if style-guide is available */
try {
  require.resolve('@trafficbyintent/style-guide/typescript/tsconfig');
  extendsPath = '@trafficbyintent/style-guide/typescript/tsconfig';
  console.log('Using @trafficbyintent/style-guide tsconfig');
} catch (error) {
  console.log('Using fallback tsconfig.base.json');
}

const tsconfig = {
  extends: extendsPath,
  compilerOptions: {
    rootDir: './src',
    outDir: './dist'
  },
  include: ['src'],
  exclude: ['node_modules', 'dist']
};

fs.writeFileSync(
  path.join(__dirname, '..', 'tsconfig.json'),
  JSON.stringify(tsconfig, null, 2) + '\n'
);

console.log('tsconfig.json generated successfully');