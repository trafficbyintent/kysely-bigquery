import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov', 'html'],
      include: ['src/**/*.ts'],
      exclude: [
        'src/**/*.d.ts',
        'src/**/*.test.ts',
        'src/**/*.spec.ts',
        'src/index.ts', /* Re-export file */
      ],
      thresholds: {
        lines: 100,
        functions: 100,
        branches: 98, // Two edge case branches in compiler are defensive
        statements: 100,
      },
    },
  },
});