import { describe, expect, test } from 'vitest';

import { BigQueryAdapter } from '../src/BigQueryAdapter';

describe('BigQueryAdapter', () => {
  test('supportsReturning should return false', () => {
    const adapter = new BigQueryAdapter();
    
    /* BigQuery doesn't support RETURNING clause */
    expect(adapter.supportsReturning).toBe(false);
  });
});