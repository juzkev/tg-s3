// Pure interval logic for chunk-backed objects (>2GB split across multiple TG files).
// Deliberately free of any Cloudflare Workers / Telegram / D1 dependency so it can be
// unit-tested in isolation — this is where the tricky chunk-boundary math lives.

/**
 * Greedily group parts into batches, each summing to <= maxChunkSize, preserving
 * order. Every part is already <= maxChunkSize (UploadPart enforces the single-file
 * limit), so a part always fits in a fresh batch and no batch ever overflows.
 */
export function groupPartsIntoChunks<T extends { size: number }>(parts: T[], maxChunkSize: number): T[][] {
  const batches: T[][] = [];
  let current: T[] = [];
  let currentSize = 0;
  for (const p of parts) {
    if (current.length > 0 && currentSize + p.size > maxChunkSize) {
      batches.push(current);
      current = [];
      currentSize = 0;
    }
    current.push(p);
    currentSize += p.size;
  }
  if (current.length > 0) batches.push(current);
  return batches;
}

export interface ChunkSelection<T> { chunk: T; localStart: number; localEnd: number; }

/**
 * Given an ordered chunk map (each chunk knows its object-relative `offset` and its
 * `size`) and an object-relative byte range [start, end] (inclusive), return each
 * overlapping chunk with the sub-range translated into that chunk's own local byte
 * offsets. A range may span multiple chunks near a boundary.
 */
export function selectChunksForRange<T extends { offset: number; size: number }>(
  chunks: T[], start: number, end: number,
): ChunkSelection<T>[] {
  const selected: ChunkSelection<T>[] = [];
  for (const c of chunks) {
    const cStart = c.offset;
    const cEnd = c.offset + c.size - 1;
    if (cEnd < start || cStart > end) continue; // no overlap
    selected.push({
      chunk: c,
      localStart: Math.max(start, cStart) - cStart,
      localEnd: Math.min(end, cEnd) - cStart,
    });
  }
  return selected;
}
