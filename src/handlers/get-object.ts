import type { Env, S3Request, ObjectRow, BucketRow, ChunkRow } from '../types';
import { MetadataStore } from '../storage/metadata';
import { downloadFromTelegram } from '../telegram/download';
import { uploadToTelegram } from '../telegram/upload';
import { computeEtag } from '../utils/crypto';
import { parseRange, isImageContentType, etagMatches, strip304Headers, buildResponseHeaders } from '../utils/headers';
import { errorResponse } from '../xml/builder';
import { BOT_API_GETFILE_LIMIT, R2_CACHE_MIN_SIZE, R2_CACHE_MAX_SIZE, CACHE_CONTROL_IMMUTABLE, CHUNKED_SENTINEL, VPS_LONG_TIMEOUT } from '../constants';
import { VpsClient } from '../media/vps-client';
import { parseSseCHeaders, validateKeyMd5, decrypt, isEncrypted, getStoredKeyMd5, addSseResponseHeaders, SseCError, isEncryptedS3, decryptS3, addSseS3ResponseHeaders } from '../utils/sse';
import { selectChunksForRange } from '../utils/chunking';

const MAX_DIRECT_DOWNLOAD = BOT_API_GETFILE_LIMIT;

function isR2Cacheable(size: number): boolean {
  return size >= R2_CACHE_MIN_SIZE && size <= R2_CACHE_MAX_SIZE;
}

// CDN Cache: build a cache key URL for a given object
function cacheKeyUrl(baseUrl: string, bucket: string, key: string): string {
  return `${baseUrl}/__cache__/${bucket}/${encodeURIComponent(key)}`;
}

// CDN Cache: purge cached response for an object
export async function purgeCdnCache(baseUrl: string, bucket: string, key: string): Promise<void> {
  try {
    const cache = caches.default;
    const url = cacheKeyUrl(baseUrl, bucket, key);
    await cache.delete(new Request(url));
  } catch { /* best effort */ }
}

// R2 Cache: key for a cached object
export function r2CacheKey(bucket: string, key: string): string {
  return `${bucket}/${key}`;
}

// R2 Cache: parse bucket and key from R2 cache key
export function parseR2CacheKey(r2Key: string): { bucket: string; key: string } | null {
  const slashIdx = r2Key.indexOf('/');
  if (slashIdx < 0) return null;
  return { bucket: r2Key.slice(0, slashIdx), key: r2Key.slice(slashIdx + 1) };
}

// R2 Cache: purge cached object
export async function purgeR2Cache(env: Env, bucket: string, key: string): Promise<void> {
  if (!env.CACHE) return;
  try {
    await env.CACHE.delete(r2CacheKey(bucket, key));
  } catch { /* best effort */ }
}

// R2 Cache: clean orphaned entries (D1 source deleted or ETag mismatch)
export async function cleanR2Cache(env: Env, store: MetadataStore, limit = 20): Promise<number> {
  if (!env.CACHE) return 0;
  let cleaned = 0;
  try {
    const listed = await env.CACHE.list({ limit });
    for (const obj of listed.objects) {
      const parsed = parseR2CacheKey(obj.key);
      if (!parsed) { await env.CACHE.delete(obj.key); cleaned++; continue; }
      const dbObj = await store.getObject(parsed.bucket, parsed.key);
      if (!dbObj || dbObj.etag !== obj.customMetadata?.etag) {
        await env.CACHE.delete(obj.key);
        cleaned++;
      }
    }
  } catch { /* R2 unavailable */ }
  return cleaned;
}

export async function handleGetObject(s3: S3Request, env: Env, ctx?: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);
  const bucket = await store.getBucket(s3.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', s3.bucket);
  const obj = await store.getObject(s3.bucket, s3.key);
  if (!obj) return errorResponse(404, 'NoSuchKey', 'The specified key does not exist.', `/${s3.bucket}/${s3.key}`);

  // SSE-C: if object is encrypted with customer key, require matching SSE-C headers
  const objEncrypted = isEncrypted(obj.system_metadata);
  let sseParams: ReturnType<typeof parseSseCHeaders> = null;
  if (objEncrypted) {
    try {
      sseParams = parseSseCHeaders(s3.headers);
      if (!sseParams) {
        return errorResponse(400, 'InvalidRequest', 'The object was stored using SSE-C. You must provide the encryption key headers.');
      }
      await validateKeyMd5(sseParams);
      const storedMd5 = getStoredKeyMd5(obj.system_metadata);
      if (storedMd5 && sseParams.keyMd5 !== storedMd5) {
        return errorResponse(403, 'AccessDenied', 'The provided encryption key does not match the key used to encrypt the object.');
      }
    } catch (e) {
      if (e instanceof SseCError) return errorResponse(400, 'InvalidArgument', e.message);
      throw e;
    }
  }

  // SSE-S3: object encrypted with server-managed key (auto-decrypt)
  const objEncryptedS3 = isEncryptedS3(obj.system_metadata);
  if (objEncryptedS3 && !env.SSE_MASTER_KEY) {
    return errorResponse(500, 'InternalError', 'Object is SSE-S3 encrypted but SSE_MASTER_KEY is not configured.');
  }

  // Conditional: If-Match (412 if ETag doesn't match)
  const ifMatch = s3.headers.get('if-match');
  if (ifMatch && !etagMatches(ifMatch, obj.etag, true)) {
    return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
  }

  // Conditional: If-Unmodified-Since (skip if If-Match present)
  if (!ifMatch) {
    const ifUnmodified = s3.headers.get('if-unmodified-since');
    if (ifUnmodified && new Date(obj.last_modified).getTime() > new Date(ifUnmodified).getTime()) {
      return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
    }
  }

  // Build response headers early so 304 responses include user metadata
  const headers = buildResponseHeaders(obj, s3.query);
  // Add SSE response headers
  if (objEncrypted) addSseResponseHeaders(headers, obj.system_metadata);
  if (objEncryptedS3) addSseS3ResponseHeaders(headers, obj.system_metadata);

  // Conditional: If-None-Match (304 if ETag matches)
  const ifNoneMatch = s3.headers.get('if-none-match');
  if (ifNoneMatch && etagMatches(ifNoneMatch, obj.etag)) {
    return new Response(null, { status: 304, headers: strip304Headers(headers) });
  }

  // Conditional: If-Modified-Since (304 if not modified, skip if If-None-Match present)
  if (!ifNoneMatch) {
    const ifModified = s3.headers.get('if-modified-since');
    if (ifModified && new Date(obj.last_modified).getTime() <= new Date(ifModified).getTime()) {
      return new Response(null, { status: 304, headers: strip304Headers(headers) });
    }
  }

  // Handle GetObject with partNumber (return a specific part of a multipart-uploaded object)
  const partNumberParam = s3.query.get('partNumber');
  if (partNumberParam) {
    return handlePartNumberGet(s3, obj, headers, parseInt(partNumberParam, 10), env, sseParams, objEncryptedS3, store);
  }

  // Chunked objects (>2GB, bytes split across multiple TG files): serve via the chunk
  // map. Checked before image-variant/cache/size branches since there is no single
  // tg_file_id to download and these objects are far larger than any cache threshold.
  if (obj.tg_file_id === CHUNKED_SENTINEL) {
    const rangeHeader = s3.headers.get('range');
    return downloadViaChunks(obj, headers, rangeHeader, env, objEncrypted ? sseParams : null, objEncryptedS3, store);
  }

  // Auto-convert HEIC/HEIF to web-compatible format (browsers can't display HEIC natively)
  const isHeic = obj.content_type === 'image/heic' || obj.content_type === 'image/heif';
  if (isHeic && !s3.query.has('original') && !s3.query.get('fmt') && env.VPS_URL) {
    s3.query = new URLSearchParams(s3.query);
    s3.query.set('fmt', 'auto');
  }

  // Handle image variant requests (w=, fmt=, q=)
  const width = s3.query.get('w');
  let format = s3.query.get('fmt');
  const quality = s3.query.get('q');
  const fmtAuto = format === 'auto';
  if ((width || format || quality) && isImageContentType(obj.content_type) && !obj.content_type.includes('svg')) {
    // Encrypted objects cannot be processed for variants (VPS would receive ciphertext)
    if (objEncrypted || objEncryptedS3) {
      return errorResponse(400, 'InvalidRequest', 'Image variants are not supported for encrypted objects.');
    }
    // Resolve fmt=auto from Accept header (AVIF > WebP > JPEG)
    if (fmtAuto) {
      const accept = s3.headers.get('accept') || '';
      if (accept.includes('image/avif')) format = 'avif';
      else if (accept.includes('image/webp')) format = 'webp';
      else format = 'jpeg';
    }
    const ALLOWED_FORMATS = ['webp', 'jpeg', 'jpg', 'png', 'avif'];
    if (width && (!/^\d+$/.test(width) || +width < 1 || +width > 4096)) {
      return errorResponse(400, 'InvalidArgument', 'w must be an integer between 1 and 4096.');
    }
    if (format && !ALLOWED_FORMATS.includes(format)) {
      return errorResponse(400, 'InvalidArgument', `fmt must be one of: auto, ${ALLOWED_FORMATS.join(', ')}.`);
    }
    if (quality && (!/^\d+$/.test(quality) || +quality < 1 || +quality > 100)) {
      return errorResponse(400, 'InvalidArgument', 'q must be an integer between 1 and 100.');
    }
    return handleImageVariant(s3, obj, env, store, bucket, width, format, quality, fmtAuto, ctx);
  }

  const rangeHeader = s3.headers.get('range');

  // 0-byte objects: reject Range requests (416), otherwise return empty body
  if (obj.size === 0) {
    if (rangeHeader) {
      return new Response(null, {
        status: 416,
        headers: { ...headers, 'Content-Range': 'bytes */0' },
      });
    }
    return new Response(new ArrayBuffer(0), {
      status: 200,
      headers: { ...headers, 'Content-Length': '0' },
    });
  }

  // Skip CDN/R2 cache for SSE-C encrypted objects (cache doesn't know the key)
  // CDN Cache: try serving from CF edge cache for non-Range full GETs of <=20MB files
  if (!objEncrypted && !rangeHeader && obj.size > 0 && obj.size <= MAX_DIRECT_DOWNLOAD) {
    const cache = caches.default;
    const cacheUrl = cacheKeyUrl(s3.url.origin, s3.bucket, s3.key);
    const cacheReq = new Request(cacheUrl);
    const cached = await cache.match(cacheReq);
    if (cached) {
      const cachedEtag = cached.headers.get('ETag');
      if (cachedEtag === obj.etag) {
        const mergedHeaders: Record<string, string> = { ...headers, 'Content-Length': obj.size.toString(), 'X-Cache': 'HIT' };
        return new Response(cached.body, { status: 200, headers: mergedHeaders });
      }
      // Stale cache: ETag mismatch, purge
      ctx?.waitUntil(cache.delete(cacheReq));
    }
  }

  // R2 Cache: try serving from R2 persistent cache (survives CDN eviction)
  if (!objEncrypted && env.CACHE && !rangeHeader && isR2Cacheable(obj.size)) {
    try {
      const r2Obj = await env.CACHE.get(r2CacheKey(s3.bucket, s3.key));
      if (r2Obj && r2Obj.customMetadata?.etag === obj.etag) {
        const r2Data = await r2Obj.arrayBuffer();
        const mergedHeaders: Record<string, string> = {
          ...headers, 'Content-Length': obj.size.toString(), 'X-Cache': 'R2-HIT',
        };
        // Also populate CDN cache from R2 hit
        if (ctx) {
          const cacheUrl = cacheKeyUrl(s3.url.origin, s3.bucket, s3.key);
          ctx.waitUntil(caches.default.put(new Request(cacheUrl), new Response(r2Data, { status: 200, headers: mergedHeaders }).clone()).catch(e => console.error('CDN cache put failed:', e)));
        }
        return new Response(r2Data, { status: 200, headers: mergedHeaders });
      }
      // Stale R2 cache: ETag mismatch, purge
      if (r2Obj) ctx?.waitUntil(env.CACHE.delete(r2CacheKey(s3.bucket, s3.key)));
    } catch { /* R2 unavailable, continue to TG */ }
  }

  // >20MB: must go through VPS proxy (Bot API getFile can't handle it)
  if (obj.size > MAX_DIRECT_DOWNLOAD) {
    if (!env.VPS_URL) {
      return errorResponse(503, 'ServiceUnavailable', 'File exceeds 20MB and requires VPS proxy which is not configured.');
    }
    return downloadViaVps(obj, headers, rangeHeader, env, objEncrypted ? sseParams : null, objEncryptedS3);
  }

  // Check range satisfiability before downloading
  const range = rangeHeader ? parseRange(rangeHeader, obj.size) : null;
  if (range === 'unsatisfiable') {
    return new Response(null, {
      status: 416,
      headers: { ...headers, 'Content-Range': `bytes */${obj.size}` },
    });
  }

  // <=20MB: download from TG directly (Range by slicing, acceptable for small files)
  const tgRes = await downloadFromTelegram(obj.tg_file_id, env);
  let data = await tgRes.arrayBuffer();

  // Decrypt after download (SSE-C or SSE-S3)
  if (objEncrypted && sseParams) {
    data = await decrypt(data, sseParams.keyBase64);
  } else if (objEncryptedS3 && env.SSE_MASTER_KEY) {
    data = await decryptS3(data, env.SSE_MASTER_KEY);
  }

  if (range) {
    const sliced = data.slice(range.start, range.end + 1);
    return new Response(sliced, {
      status: 206,
      headers: {
        ...headers,
        'Content-Length': sliced.byteLength.toString(),
        'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
      },
    });
  }

  // Cache store: CDN + R2 for full non-Range responses (skip for encrypted objects)
  const response = new Response(data, {
    status: 200,
    headers: { ...headers, 'Content-Length': obj.size.toString(), 'X-Cache': objEncrypted ? 'SSE-C' : objEncryptedS3 ? 'SSE-S3' : 'MISS' },
  });
  if (ctx && !objEncrypted) {
    const cacheUrl = cacheKeyUrl(s3.url.origin, s3.bucket, s3.key);
    ctx.waitUntil(caches.default.put(new Request(cacheUrl), response.clone()).catch(e => console.error('CDN cache put failed:', e)));
    // Store to R2 persistent cache (only for files within cacheable size range)
    if (env.CACHE && isR2Cacheable(obj.size)) {
      ctx.waitUntil(env.CACHE.put(r2CacheKey(s3.bucket, s3.key), data, {
        customMetadata: { etag: obj.etag },
        httpMetadata: { contentType: obj.content_type },
      }).catch(e => console.error('R2 cache put failed:', e)));
    }
  }
  return response;
}

async function handlePartNumberGet(
  s3: S3Request, obj: ObjectRow, headers: Record<string, string>,
  partNumber: number, env: Env,
  sseParams: ReturnType<typeof parseSseCHeaders>,
  encryptedS3: boolean,
  store: MetadataStore,
): Promise<Response> {
  // Extract part sizes from system metadata (stored during CompleteMultipartUpload)
  let partSizes: number[] | undefined;
  if (obj.system_metadata) {
    try {
      const sysMeta = JSON.parse(obj.system_metadata);
      if (sysMeta._mp_part_sizes) {
        partSizes = JSON.parse(sysMeta._mp_part_sizes);
      }
    } catch { /* ignore */ }
  }

  if (!partSizes || !Number.isInteger(partNumber) || partNumber < 1 || partNumber > partSizes.length) {
    return errorResponse(400, 'InvalidPartNumber', 'The requested partNumber is not valid.');
  }

  // Calculate byte range for this part (plaintext offsets)
  let start = 0;
  for (let i = 0; i < partNumber - 1; i++) start += partSizes[i];
  const partSize = partSizes[partNumber - 1];
  const end = start + partSize - 1;

  const needsDecrypt = !!(sseParams || (encryptedS3 && env.SSE_MASTER_KEY));

  // Chunked object: the part's object-relative range may span multiple TG chunks
  if (obj.tg_file_id === CHUNKED_SENTINEL) {
    if (!env.VPS_URL) return errorResponse(503, 'ServiceUnavailable', 'Chunked object requires VPS proxy which is not configured.');
    const chunks = await store.getChunks(obj.bucket, obj.key);
    if (chunks.length === 0) return errorResponse(500, 'InternalError', 'Chunk map missing for chunked object.');
    const keyBase64 = sseParams ? sseParams.keyBase64 : (encryptedS3 && env.SSE_MASTER_KEY ? env.SSE_MASTER_KEY : null);
    const vps = new VpsClient(env);
    const selected = selectChunksForRange(chunks, start, end);
    const thunks = selected.map(sel => chunkThunk(vps, sel.chunk, sel.localStart, sel.localEnd, needsDecrypt, keyBase64));
    return buildChunkResponse(206, partSize, `bytes ${start}-${end}/${obj.size}`, {
      ...headers, 'x-amz-mp-parts-count': partSizes.length.toString(),
    }, thunks);
  }

  // Download and serve the part range
  let data: ArrayBuffer;
  if (needsDecrypt) {
    // AES-GCM: must decrypt full file, then slice to part range
    const keyBase64 = sseParams ? sseParams.keyBase64 : env.SSE_MASTER_KEY!;
    if (obj.size <= MAX_DIRECT_DOWNLOAD) {
      const tgRes = await downloadFromTelegram(obj.tg_file_id, env);
      let full = await tgRes.arrayBuffer();
      if (sseParams) full = await decrypt(full, sseParams.keyBase64);
      else if (encryptedS3 && env.SSE_MASTER_KEY) full = await decryptS3(full, env.SSE_MASTER_KEY);
      data = full.slice(start, end + 1);
    } else if (env.VPS_URL) {
      try {
        const vps = new VpsClient(env);
        const vpsRes = await vps.proxyGetDecrypt(obj.tg_file_id, keyBase64, start, end);
        data = await vpsRes.arrayBuffer();
      } catch {
        return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
      }
    } else {
      return errorResponse(503, 'ServiceUnavailable', 'Encrypted file exceeds 20MB and requires VPS proxy.');
    }
  } else if (obj.size <= MAX_DIRECT_DOWNLOAD) {
    const tgRes = await downloadFromTelegram(obj.tg_file_id, env);
    const full = await tgRes.arrayBuffer();
    data = full.slice(start, end + 1);
  } else if (env.VPS_URL) {
    try {
      const vps = new VpsClient(env);
      const vpsRes = await vps.proxyRange(obj.tg_file_id, start, end);
      data = await vpsRes.arrayBuffer();
    } catch {
      return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
    }
  } else {
    return errorResponse(400, 'InvalidRequest', 'Object exceeds direct download limit.');
  }

  return new Response(data, {
    status: 206,
    headers: {
      ...headers,
      'Content-Length': partSize.toString(),
      'Content-Range': `bytes ${start}-${end}/${obj.size}`,
      'x-amz-mp-parts-count': partSizes.length.toString(),
    },
  });
}

async function downloadViaVps(
  obj: ObjectRow, headers: Record<string, string>,
  rangeHeader: string | null, env: Env,
  sseParams: ReturnType<typeof parseSseCHeaders>,
  encryptedS3: boolean,
): Promise<Response> {
  const vps = new VpsClient(env);
  const needsDecrypt = !!(sseParams || (encryptedS3 && env.SSE_MASTER_KEY));

  // Encrypted files: VPS decrypts and streams back (no Worker memory buffering)
  if (needsDecrypt) {
    try {
      const keyBase64 = sseParams ? sseParams.keyBase64 : env.SSE_MASTER_KEY!;

      if (rangeHeader) {
        const range = parseRange(rangeHeader, obj.size);
        if (range === 'unsatisfiable') {
          return new Response(null, {
            status: 416,
            headers: { ...headers, 'Content-Range': `bytes */${obj.size}` },
          });
        }
        if (range) {
          const vpsRes = await vps.proxyGetDecrypt(obj.tg_file_id, keyBase64, range.start, range.end);
          return new Response(vpsRes.body, {
            status: 206,
            headers: {
              ...headers,
              'Content-Length': (range.end - range.start + 1).toString(),
              'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
            },
          });
        }
      }

      const vpsRes = await vps.proxyGetDecrypt(obj.tg_file_id, keyBase64);
      return new Response(vpsRes.body, {
        status: 200,
        headers: { ...headers, 'Content-Length': obj.size.toString() },
      });
    } catch {
      return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
    }
  }

  // Non-encrypted: stream Range directly from VPS
  if (rangeHeader) {
    const range = parseRange(rangeHeader, obj.size);
    if (range === 'unsatisfiable') {
      return new Response(null, {
        status: 416,
        headers: { ...headers, 'Content-Range': `bytes */${obj.size}` },
      });
    }
    if (range) {
      try {
        const vpsRes = await vps.proxyRange(obj.tg_file_id, range.start, range.end);
        return new Response(vpsRes.body, {
          status: 206,
          headers: {
            ...headers,
            'Content-Length': (range.end - range.start + 1).toString(),
            'Content-Range': `bytes ${range.start}-${range.end}/${obj.size}`,
          },
        });
      } catch {
        return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
      }
    }
  }

  // Full download via VPS proxy (non-encrypted)
  try {
    const vpsRes = await vps.proxyGet(obj.tg_file_id);
    return new Response(vpsRes.body, {
      status: 200,
      headers: { ...headers, 'Content-Length': obj.size.toString() },
    });
  } catch {
    return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
  }
}

// ── Chunked object serving (>2GB objects split across multiple TG files) ──
// Interval selection (selectChunksForRange) lives in ../utils/chunking so it can be
// unit-tested without pulling in Worker globals.

/**
 * Build a lazy fetcher for one chunk's [localStart, localEnd] byte sub-range.
 * Encrypted chunks are decrypted VPS-side (get-decrypt over the chunk's plaintext
 * offsets); plaintext chunks use the passthrough range endpoint. A sub-range that
 * covers the whole chunk uses the cheaper full-file endpoint.
 */
function chunkThunk(
  vps: VpsClient, chunk: ChunkRow, localStart: number, localEnd: number,
  needsDecrypt: boolean, keyBase64: string | null,
): () => Promise<ReadableStream<Uint8Array>> {
  const wholeChunk = localStart === 0 && localEnd === chunk.size - 1;
  return async () => {
    let res: Response;
    if (needsDecrypt) {
      // get-decrypt already uses VPS_LONG_TIMEOUT internally.
      res = wholeChunk
        ? await vps.proxyGetDecrypt(chunk.tg_file_id, keyBase64!)
        : await vps.proxyGetDecrypt(chunk.tg_file_id, keyBase64!, localStart, localEnd);
    } else {
      // A single chunk can be up to 2GB, so use the long timeout: the short proxy
      // timeout would abort the streamed body mid-transfer on slower connections.
      res = wholeChunk
        ? await vps.proxyGet(chunk.tg_file_id, VPS_LONG_TIMEOUT)
        : await vps.proxyRange(chunk.tg_file_id, localStart, localEnd, VPS_LONG_TIMEOUT);
    }
    if (!res.body) throw new Error(`Chunk ${chunk.chunk_index} returned an empty body`);
    return res.body;
  };
}

/**
 * Concatenate chunk streams into one, fetching each subsequent chunk lazily only
 * once the previous one is fully consumed (no Worker memory buffering). The first
 * stream is already resolved so a mid-stream backend failure surfaces per chunk.
 */
function concatChunkStream(
  first: ReadableStream<Uint8Array>,
  rest: Array<() => Promise<ReadableStream<Uint8Array>>>,
): ReadableStream<Uint8Array> {
  let reader: ReadableStreamDefaultReader<Uint8Array> | null = first.getReader();
  let idx = 0;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      while (true) {
        if (!reader) {
          if (idx >= rest.length) { controller.close(); return; }
          try {
            reader = (await rest[idx++]()).getReader();
          } catch (e) { controller.error(e as Error); return; }
        }
        try {
          const { done, value } = await reader.read();
          if (done) { reader.releaseLock(); reader = null; continue; }
          controller.enqueue(value);
          return;
        } catch (e) { controller.error(e as Error); return; }
      }
    },
    async cancel(reason) {
      if (reader) { try { await reader.cancel(reason); } catch { /* ignore */ } }
    },
  });
}

/**
 * Assemble a streaming response from per-chunk fetch thunks. The first chunk is
 * fetched eagerly so a backend failure yields a clean 503 (headers not yet sent)
 * instead of a truncated body.
 */
async function buildChunkResponse(
  status: number, contentLength: number, contentRange: string | null,
  headers: Record<string, string>,
  thunks: Array<() => Promise<ReadableStream<Uint8Array>>>,
): Promise<Response> {
  if (thunks.length === 0) {
    return new Response(new ArrayBuffer(0), { status, headers: { ...headers, 'Content-Length': '0' } });
  }
  let firstBody: ReadableStream<Uint8Array>;
  try {
    firstBody = await thunks[0]();
  } catch {
    return errorResponse(503, 'ServiceUnavailable', 'Storage backend temporarily unavailable.');
  }
  const body = concatChunkStream(firstBody, thunks.slice(1));
  const h: Record<string, string> = { ...headers, 'Content-Length': contentLength.toString() };
  if (contentRange) h['Content-Range'] = contentRange;
  return new Response(body, { status, headers: h });
}

export async function downloadViaChunks(
  obj: ObjectRow, headers: Record<string, string>, rangeHeader: string | null,
  env: Env, sseParams: ReturnType<typeof parseSseCHeaders>, encryptedS3: boolean,
  store: MetadataStore,
): Promise<Response> {
  if (!env.VPS_URL) {
    return errorResponse(503, 'ServiceUnavailable', 'File is chunked and requires VPS proxy which is not configured.');
  }
  const chunks = await store.getChunks(obj.bucket, obj.key);
  if (chunks.length === 0) {
    return errorResponse(500, 'InternalError', 'Chunk map missing for chunked object.');
  }
  const vps = new VpsClient(env);
  const needsDecrypt = !!(sseParams || (encryptedS3 && env.SSE_MASTER_KEY));
  const keyBase64 = sseParams ? sseParams.keyBase64 : (encryptedS3 && env.SSE_MASTER_KEY ? env.SSE_MASTER_KEY : null);

  if (rangeHeader) {
    const range = parseRange(rangeHeader, obj.size);
    if (range === 'unsatisfiable') {
      return new Response(null, { status: 416, headers: { ...headers, 'Content-Range': `bytes */${obj.size}` } });
    }
    if (range) {
      const selected = selectChunksForRange(chunks, range.start, range.end);
      const thunks = selected.map(sel => chunkThunk(vps, sel.chunk, sel.localStart, sel.localEnd, needsDecrypt, keyBase64));
      return buildChunkResponse(206, range.end - range.start + 1, `bytes ${range.start}-${range.end}/${obj.size}`, headers, thunks);
    }
  }

  // Full object: stream every chunk in order
  const thunks = chunks.map(c => chunkThunk(vps, c, 0, c.size - 1, needsDecrypt, keyBase64));
  return buildChunkResponse(200, obj.size, null, headers, thunks);
}

async function handleImageVariant(
  s3: S3Request, obj: ObjectRow,
  env: Env, store: MetadataStore, bucket: BucketRow,
  width: string | null, format: string | null, quality: string | null, fmtAuto: boolean, ctx?: ExecutionContext,
): Promise<Response> {
  const qualitySuffix = quality ? `_q${quality}` : '';
  const variantKey = `${obj.key}._derivatives/w${width || 'orig'}${qualitySuffix}_${format || 'original'}`;

  // Vary: Accept ensures CDN caches different fmt=auto results per browser capability
  const variantHeaders = (ct: string, size?: number): Record<string, string> => {
    const h: Record<string, string> = {
      'Content-Type': ct,
      'Cache-Control': CACHE_CONTROL_IMMUTABLE,
      'Access-Control-Allow-Origin': '*',
    };
    if (size !== undefined) h['Content-Length'] = size.toString();
    if (fmtAuto) h['Vary'] = 'Accept';
    return h;
  };

  // Check D1 for cached variant
  const cached = await store.getObject(s3.bucket, variantKey);
  if (cached) {
    const tgRes = await downloadFromTelegram(cached.tg_file_id, env);
    return new Response(tgRes.body, { headers: variantHeaders(cached.content_type, cached.size) });
  }

  if (!env.VPS_URL) {
    // No VPS: return original
    const tgRes = await downloadFromTelegram(obj.tg_file_id, env);
    return new Response(tgRes.body, {
      headers: { 'Content-Type': obj.content_type, 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' },
    });
  }

  // Call VPS to process variant
  const vps = new VpsClient(env);
  let vpsRes: Response;
  try {
    vpsRes = await vps.imageResize(obj.tg_file_id, width, format, quality);
  } catch {
    const tgRes = await downloadFromTelegram(obj.tg_file_id, env);
    return new Response(tgRes.body, {
      headers: { 'Content-Type': obj.content_type, 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' },
    });
  }

  // Cache the variant back to TG + D1 (async, don't block response)
  const variantData = await vpsRes.arrayBuffer();
  const variantCt = vpsRes.headers.get('content-type') || 'image/jpeg';

  // Cache variant back to TG + D1 asynchronously
  {
    const cacheVariant = (async () => {
      try {
        const result = await uploadToTelegram(variantData, bucket.tg_chat_id, variantKey.split('/').pop()!, variantCt, env, bucket.tg_topic_id);
        const etag = await computeEtag(variantData);
        await store.putObject({
          bucket: s3.bucket, key: variantKey, size: variantData.byteLength, etag,
          contentType: variantCt, tgChatId: result.tgChatId, tgMessageId: result.tgMessageId,
          tgFileId: result.tgFileId, tgFileUniqueId: result.tgFileUniqueId,
          derivedFrom: obj.key,
        });
      } catch { /* best effort, will be cached on next request */ }
    })();
    if (ctx) ctx.waitUntil(cacheVariant);
  }

  return new Response(variantData, { headers: variantHeaders(variantCt, variantData.byteLength) });
}
