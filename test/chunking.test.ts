import { describe, it, expect } from 'vitest';
import { groupPartsIntoChunks, selectChunksForRange } from '../src/utils/chunking';

const GB = 1024 * 1024 * 1024;
const MAX = 2 * GB; // VPS_SINGLE_FILE_MAX

// Build the chunk map the write path would produce from a list of part sizes:
// group parts into <=MAX batches, then assign each batch a cumulative offset.
function chunksFromPartSizes(partSizes: number[], max = MAX) {
  const batches = groupPartsIntoChunks(partSizes.map(size => ({ size })), max);
  let offset = 0;
  return batches.map((batch, i) => {
    const size = batch.reduce((s, p) => s + p.size, 0);
    const chunk = { chunk_index: i, offset, size };
    offset += size;
    return chunk;
  });
}

describe('groupPartsIntoChunks', () => {
  it('keeps everything in one batch when the total fits', () => {
    const batches = groupPartsIntoChunks([{ size: 500 }, { size: 500 }, { size: 24 }], MAX);
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(3);
  });

  it('starts a new batch exactly when adding the next part would overflow', () => {
    // 3 parts of 1.5GB → 1.5 fits, +1.5=3.0 overflows 2GB → batch break, etc.
    const p = Math.floor(1.5 * GB);
    const batches = groupPartsIntoChunks([{ size: p }, { size: p }, { size: p }], MAX);
    expect(batches.map(b => b.length)).toEqual([1, 1, 1]);
  });

  it('packs greedily: a batch fills until the next part would overflow', () => {
    // 600MB parts: 3 fit in 2GB (1800MB), 4th (2400MB) overflows → new batch
    const p = 600 * 1024 * 1024;
    const batches = groupPartsIntoChunks(Array(5).fill({ size: p }), MAX);
    expect(batches.map(b => b.length)).toEqual([3, 2]);
  });

  it('treats a sum that exactly equals the max as still fitting', () => {
    const half = MAX / 2;
    const batches = groupPartsIntoChunks([{ size: half }, { size: half }, { size: 1 }], MAX);
    // half + half === MAX (fits), then +1 overflows → second batch
    expect(batches.map(b => b.length)).toEqual([2, 1]);
  });

  it('handles a single part and an empty list', () => {
    expect(groupPartsIntoChunks([{ size: 10 }], MAX)).toEqual([[{ size: 10 }]]);
    expect(groupPartsIntoChunks([], MAX)).toEqual([]);
  });

  it('never produces a batch exceeding the max and preserves order + total (each part <= max)', () => {
    const sizes = [1.9 * GB, 0.2 * GB, 0.2 * GB, 1.99 * GB, 0.01 * GB, 2 * GB].map(Math.floor);
    const parts = sizes.map((size, id) => ({ size, id }));
    const batches = groupPartsIntoChunks(parts, MAX);
    // no batch overflows
    for (const b of batches) {
      expect(b.reduce((s, p) => s + p.size, 0)).toBeLessThanOrEqual(MAX);
    }
    // order + completeness preserved
    expect(batches.flat().map(p => p.id)).toEqual(parts.map(p => p.id));
  });
});

describe('selectChunksForRange', () => {
  // 5GB object: [0,2GB) [2GB,4GB) [4GB,5GB)
  const chunks = chunksFromPartSizes([2 * GB, 2 * GB, GB]);

  it('models the intended 3-chunk layout', () => {
    expect(chunks).toEqual([
      { chunk_index: 0, offset: 0, size: 2 * GB },
      { chunk_index: 1, offset: 2 * GB, size: 2 * GB },
      { chunk_index: 2, offset: 4 * GB, size: GB },
    ]);
  });

  it('selects a range fully inside the first chunk', () => {
    expect(selectChunksForRange(chunks, 100, 200)).toEqual([
      { chunk: chunks[0], localStart: 100, localEnd: 200 },
    ]);
  });

  it('translates a range fully inside a middle chunk to local offsets', () => {
    expect(selectChunksForRange(chunks, 2 * GB + 10, 2 * GB + 20)).toEqual([
      { chunk: chunks[1], localStart: 10, localEnd: 20 },
    ]);
  });

  it('splits a range straddling the chunk0/chunk1 boundary (off-by-one trap)', () => {
    const start = 2 * GB - 5;
    const end = 2 * GB + 4;
    const sel = selectChunksForRange(chunks, start, end);
    expect(sel).toEqual([
      { chunk: chunks[0], localStart: 2 * GB - 5, localEnd: 2 * GB - 1 }, // last 5 bytes of chunk0
      { chunk: chunks[1], localStart: 0, localEnd: 4 },                    // first 5 bytes of chunk1
    ]);
    // reconstructed byte count equals the requested range length
    const bytes = sel.reduce((s, x) => s + (x.localEnd - x.localStart + 1), 0);
    expect(bytes).toBe(end - start + 1);
  });

  it('handles the very first byte of the object', () => {
    expect(selectChunksForRange(chunks, 0, 0)).toEqual([
      { chunk: chunks[0], localStart: 0, localEnd: 0 },
    ]);
  });

  it('handles the very last byte of the object', () => {
    const last = 5 * GB - 1;
    expect(selectChunksForRange(chunks, last, last)).toEqual([
      { chunk: chunks[2], localStart: GB - 1, localEnd: GB - 1 },
    ]);
  });

  it('selects every chunk for a full-object range, each mapped to its whole extent', () => {
    const sel = selectChunksForRange(chunks, 0, 5 * GB - 1);
    expect(sel).toEqual([
      { chunk: chunks[0], localStart: 0, localEnd: 2 * GB - 1 },
      { chunk: chunks[1], localStart: 0, localEnd: 2 * GB - 1 },
      { chunk: chunks[2], localStart: 0, localEnd: GB - 1 },
    ]);
  });

  it('spans three chunks: partial-first, whole-middle, partial-last', () => {
    const start = 2 * GB - 1; // last byte of chunk0
    const end = 4 * GB;       // first byte of chunk2
    const sel = selectChunksForRange(chunks, start, end);
    expect(sel).toEqual([
      { chunk: chunks[0], localStart: 2 * GB - 1, localEnd: 2 * GB - 1 },
      { chunk: chunks[1], localStart: 0, localEnd: 2 * GB - 1 },
      { chunk: chunks[2], localStart: 0, localEnd: 0 },
    ]);
    const bytes = sel.reduce((s, x) => s + (x.localEnd - x.localStart + 1), 0);
    expect(bytes).toBe(end - start + 1);
  });

  it('produces contiguous, gapless local pieces summing to the range length (fuzz over boundaries)', () => {
    const total = 5 * GB;
    // probe ranges centered on each chunk boundary and interior points
    const probes = [0, 1, GB, 2 * GB - 1, 2 * GB, 2 * GB + 1, 4 * GB - 1, 4 * GB, total - 2];
    for (const start of probes) {
      for (const len of [1, 2, 7, GB + 3]) {
        const end = Math.min(start + len - 1, total - 1);
        const sel = selectChunksForRange(chunks, start, end);
        // object-relative reconstruction must be exactly [start, end] with no gaps/overlaps
        let cursor = start;
        for (const s of sel) {
          const absStart = s.chunk.offset + s.localStart;
          const absEnd = s.chunk.offset + s.localEnd;
          expect(absStart).toBe(cursor);
          cursor = absEnd + 1;
        }
        expect(cursor).toBe(end + 1);
      }
    }
  });

  it('single-chunk object (<2GB) still selects correctly', () => {
    const one = chunksFromPartSizes([GB]);
    expect(one).toHaveLength(1);
    expect(selectChunksForRange(one, 10, 20)).toEqual([
      { chunk: one[0], localStart: 10, localEnd: 20 },
    ]);
  });
});

describe('VPS_SINGLE_FILE_MAX', () => {
  it('stays within Telegram\'s 4000 x 512KB big-file upload ceiling', async () => {
    const { VPS_SINGLE_FILE_MAX } = await import('../src/constants');
    const TG_MAX_UPLOAD = 4000 * 512 * 1024; // 2,097,152,000 bytes ("2000 MB")
    // A larger value lets chunking build chunks Telegram rejects with
    // "Bad Request: FILE_PARTS_INVALID" (2GiB is ~48MiB over the limit).
    expect(VPS_SINGLE_FILE_MAX).toBeLessThanOrEqual(TG_MAX_UPLOAD);
    expect(VPS_SINGLE_FILE_MAX).toBeGreaterThan(1024 * 1024 * 1024); // still >1GiB
  });

  it('never groups parts into a chunk Telegram would reject', async () => {
    const { VPS_SINGLE_FILE_MAX } = await import('../src/constants');
    const parts = Array.from({ length: 12 }, () => ({ size: 300 * 1024 * 1024 }));
    for (const batch of groupPartsIntoChunks(parts, VPS_SINGLE_FILE_MAX)) {
      expect(batch.reduce((s, p) => s + p.size, 0)).toBeLessThanOrEqual(4000 * 512 * 1024);
    }
  });
});
