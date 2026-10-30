import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.{test,spec}.ts', 'tests/**/test_*.ts'],
    environment: 'node',
    globals: true,
  },
});
