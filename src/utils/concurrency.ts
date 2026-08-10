// Bounded-concurrency map, order-preserving. Runs at most `concurrency` calls to
// `fn` at once, workers pulling from a shared cursor. Results are returned in input
// order regardless of completion order. On the first rejection, workers stop
// pulling new items; after all in-flight settle, the first error is re-thrown (so
// callers can clean up whatever succeeded without racing still-running work).
export async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const n = items.length;
  const limit = Math.max(1, Math.min(Math.floor(concurrency) || 1, n || 1));
  const results = new Array<R>(n);
  let cursor = 0;
  let firstError: unknown = null;

  const worker = async (): Promise<void> => {
    while (true) {
      if (firstError !== null) return; // a sibling failed; stop taking new work
      const i = cursor++;
      if (i >= n) return;
      try {
        results[i] = await fn(items[i], i);
      } catch (e) {
        if (firstError === null) firstError = e;
        return;
      }
    }
  };

  await Promise.all(Array.from({ length: limit }, () => worker()));
  if (firstError !== null) throw firstError;
  return results;
}
