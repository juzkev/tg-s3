import { describe, it, expect } from 'vitest';
import { mapWithConcurrency } from '../src/utils/concurrency';

const tick = (ms: number) => new Promise(r => setTimeout(r, ms));

describe('mapWithConcurrency', () => {
  it('preserves input order regardless of completion order', async () => {
    const items = [30, 10, 20, 5];
    const out = await mapWithConcurrency(items, 4, async (v) => { await tick(v); return v * 2; });
    expect(out).toEqual([60, 20, 40, 10]);
  });

  it('never exceeds the concurrency limit', async () => {
    let inFlight = 0;
    let peak = 0;
    const items = Array.from({ length: 12 }, (_, i) => i);
    await mapWithConcurrency(items, 3, async (i) => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await tick(5);
      inFlight--;
      return i;
    });
    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBe(3);
  });

  it('runs sequentially when concurrency is 1', async () => {
    let inFlight = 0;
    let peak = 0;
    await mapWithConcurrency([1, 2, 3], 1, async (i) => {
      inFlight++; peak = Math.max(peak, inFlight);
      await tick(3);
      inFlight--; return i;
    });
    expect(peak).toBe(1);
  });

  it('clamps a concurrency larger than the item count', async () => {
    let peak = 0, inFlight = 0;
    await mapWithConcurrency([1, 2], 100, async (i) => {
      inFlight++; peak = Math.max(peak, inFlight); await tick(2); inFlight--; return i;
    });
    expect(peak).toBeLessThanOrEqual(2);
  });

  it('propagates the first error and stops taking new work', async () => {
    const started: number[] = [];
    await expect(mapWithConcurrency([0, 1, 2, 3, 4, 5], 2, async (i) => {
      started.push(i);
      await tick(5);
      if (i === 1) throw new Error('boom');
      return i;
    })).rejects.toThrow('boom');
    // With concurrency 2, items 0 and 1 start; after 1 fails, no further items are pulled.
    expect(started).not.toContain(4);
    expect(started).not.toContain(5);
  });

  it('handles an empty list', async () => {
    expect(await mapWithConcurrency([], 4, async () => 1)).toEqual([]);
  });
});
