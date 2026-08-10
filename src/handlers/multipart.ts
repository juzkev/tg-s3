import type { Env, S3Request, ObjectRow } from '../types';
import { groupPartsIntoChunks } from '../utils/chunking';
import { mapWithConcurrency } from '../utils/concurrency';
import { CHUNK_CONSOLIDATE_CONCURRENCY_MAX } from '../constants';

type ChunkDesc = { chunkIndex: number; offset: number; size: number; tgChatId: string; tgMessageId: number; tgFileId: string };

// Parallel VPS consolidations on completion. Default 1 (sequential): raising it
// speeds up multi-chunk uploads but each concurrent consolidate holds a <=2GB temp
// file on the VPS and drives another Telegram transfer, so it is bounded and
// deployment-tunable via the CHUNK_CONSOLIDATE_CONCURRENCY var.
function getChunkConcurrency(env: Env): number {
  const raw = parseInt(env.CHUNK_CONSOLIDATE_CONCURRENCY || '', 10);
  if (isNaN(raw) || raw < 1) return 1;
  return Math.min(raw, CHUNK_CONSOLIDATE_CONCURRENCY_MAX);
}
import { MetadataStore } from '../storage/metadata';
import { uploadToTelegram, RateLimitError, FileTooLargeError, type UploadResult } from '../telegram/upload';
import { downloadFromTelegram } from '../telegram/download';
import { TelegramClient } from '../telegram/client';
import { computeEtag, computeMultipartEtag, sha256Hex } from '../utils/crypto';
import { extractUserMetadata, extractSystemMetadata, etagMatches } from '../utils/headers';
import { readBody } from './put-object';
import { deleteDerivatives, deleteChunks } from './delete-object';
import { purgeCdnCache, purgeR2Cache } from './get-object';
import { initiateMultipartXml, completeMultipartXml, listPartsXml, listMultipartUploadsXml, copyPartResultXml, xmlResponse, errorResponse } from '../xml/builder';
import { BOT_API_GETFILE_LIMIT, VPS_SINGLE_FILE_MAX, S3_MIN_PART_SIZE, S3_MAX_KEYS_DEFAULT, S3_MAX_PART_NUMBER, CHUNKED_SENTINEL } from '../constants';
import { VpsClient } from '../media/vps-client';
import { parseCompleteMultipart } from '../xml/parser';
import {
  parseSseCHeaders, validateKeyMd5, encrypt, decrypt, addSseMetadata, SseCError,
  encryptS3, addSseS3Metadata, decryptS3,
  isEncrypted, isEncryptedS3, getStoredKeyMd5, SSE_COPY_HEADERS,
} from '../utils/sse';

function clampInt(val: string | null, defaultVal: number, min: number, max: number): number {
  const n = parseInt(val || String(defaultVal), 10);
  if (isNaN(n)) return defaultVal;
  return Math.max(min, Math.min(n, max));
}

export async function handleCreateMultipartUpload(s3: S3Request, env: Env): Promise<Response> {
  const store = new MetadataStore(env);

  const bucket = await store.getBucket(s3.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', s3.bucket);

  // SSE-C: parse and validate encryption headers, store in upload metadata
  let sseParams: ReturnType<typeof parseSseCHeaders> = null;
  try {
    sseParams = parseSseCHeaders(s3.headers);
    if (sseParams) await validateKeyMd5(sseParams);
  } catch (e) {
    if (e instanceof SseCError) return errorResponse(400, 'InvalidArgument', e.message);
    throw e;
  }

  // SSE-S3: determine if server-side encryption should be applied
  const sseS3Header = s3.headers.get('x-amz-server-side-encryption');
  if (sseS3Header && sseS3Header !== 'AES256') {
    return errorResponse(400, 'InvalidArgument', 'The encryption method specified is not supported. Supported: AES256.');
  }
  if (sseS3Header === 'AES256' && !env.SSE_MASTER_KEY) {
    return errorResponse(400, 'InvalidArgument', 'SSE-S3 is not configured. Set SSE_MASTER_KEY secret.');
  }
  const useSseS3 = !sseParams && !!env.SSE_MASTER_KEY && (
    sseS3Header === 'AES256' || !!bucket.default_encryption
  );

  const uploadId = crypto.randomUUID();
  const contentType = s3.headers.get('content-type') || 'application/octet-stream';
  const userMeta = extractUserMetadata(s3.headers);
  const metaJson = Object.keys(userMeta).length > 0 ? JSON.stringify(userMeta) : undefined;
  let sysMeta = extractSystemMetadata(s3.headers);
  if (sseParams) sysMeta = addSseMetadata(sysMeta || {}, sseParams);
  if (useSseS3) sysMeta = addSseS3Metadata(sysMeta || {});
  const sysMetaJson = sysMeta ? JSON.stringify(sysMeta) : undefined;

  await store.createMultipartUpload(uploadId, s3.bucket, s3.key, contentType, metaJson, sysMetaJson);

  const resp = xmlResponse(initiateMultipartXml(s3.bucket, s3.key, uploadId));
  if (sseParams) {
    resp.headers.set('x-amz-server-side-encryption-customer-algorithm', 'AES256');
    resp.headers.set('x-amz-server-side-encryption-customer-key-MD5', sseParams.keyMd5);
  } else if (useSseS3) {
    resp.headers.set('x-amz-server-side-encryption', 'AES256');
  }
  return resp;
}

export async function handleUploadPart(s3: S3Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);

  const uploadId = s3.query.get('uploadId');
  const partNumber = parseInt(s3.query.get('partNumber') || '0', 10);
  if (!uploadId || !partNumber) return errorResponse(400, 'InvalidArgument', 'Missing uploadId or partNumber.');
  if (partNumber < 1 || partNumber > S3_MAX_PART_NUMBER) return errorResponse(400, 'InvalidArgument', 'partNumber must be between 1 and 10000.');

  const upload = await store.getMultipartUpload(uploadId);
  if (!upload || upload.bucket !== s3.bucket || upload.key !== s3.key) return errorResponse(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');

  // SSE-C: validate headers match the upload's SSE metadata (if any)
  let uploadSseMd5: string | null = null;
  if (upload.system_metadata) {
    try {
      const sm = JSON.parse(upload.system_metadata);
      if (sm._sse === 'AES256') uploadSseMd5 = sm._sse_key_md5;
    } catch { /* ignore */ }
  }
  if (uploadSseMd5) {
    try {
      const sseParams = parseSseCHeaders(s3.headers);
      if (!sseParams) return errorResponse(400, 'InvalidRequest', 'This multipart upload was initiated with SSE-C. You must provide the encryption key.');
      await validateKeyMd5(sseParams);
      if (sseParams.keyMd5 !== uploadSseMd5) return errorResponse(400, 'InvalidArgument', 'The SSE-C key does not match the key used to initiate the multipart upload.');
    } catch (e) {
      if (e instanceof SseCError) return errorResponse(400, 'InvalidArgument', e.message);
      throw e;
    }
  }

  const bucket = await store.getBucket(upload.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'Bucket not found.');

  // Large part: stream the body straight to the VPS (no Worker memory buffering),
  // lifting the per-part ceiling from ~100MB (buffered) to 2GB. Parts are stored
  // unencrypted regardless of SSE (the consolidated object is encrypted at
  // completion), so no key is passed here. Only for UNSIGNED-PAYLOAD parts: a real
  // x-amz-content-sha256 needs the whole body buffered to verify, and aws-chunked
  // bodies need de-framing first — both fall through to the buffered path below.
  const partContentLength = parseInt(s3.headers.get('content-length') || '0', 10);
  const partDecodedLength = parseInt(s3.headers.get('x-amz-decoded-content-length') || '0', 10);
  const partEstimatedSize = partDecodedLength || partContentLength;
  const partSha256 = s3.headers.get('x-amz-content-sha256') || '';
  const partIsAwsChunked = partSha256.startsWith('STREAMING-');
  const partHasRealHash = partSha256 !== '' && partSha256 !== 'UNSIGNED-PAYLOAD' && !partIsAwsChunked;

  if (partEstimatedSize > BOT_API_GETFILE_LIMIT && env.VPS_URL && !partIsAwsChunked && !partHasRealHash && s3.body) {
    if (partEstimatedSize > VPS_SINGLE_FILE_MAX) {
      return errorResponse(400, 'EntityTooLarge', 'Part size exceeds the 2GB limit.');
    }
    const vps = new VpsClient(env);
    const partMd5 = s3.headers.get('content-md5') || undefined;
    let vpsRes: Response;
    try {
      vpsRes = await vps.proxyPutFull(
        s3.body,
        bucket.tg_chat_id,
        `${upload.key}.part${partNumber.toString().padStart(4, '0')}`,
        'application/octet-stream',
        partContentLength,
        { messageThreadId: bucket.tg_topic_id, contentMd5: partMd5 },
      );
    } catch (e) {
      return errorResponse(502, 'InternalError', `Part upload failed: ${(e as Error).message}`);
    }
    const streamed = await vpsRes.json() as {
      etag: string; tgChatId: string; tgMessageId: number; tgFileId: string; size: number;
    };

    const prevPart = await store.getMultipartPart(uploadId, partNumber);
    await store.putMultipartPart({
      uploadId, partNumber, size: streamed.size, etag: streamed.etag,
      tgChatId: streamed.tgChatId, tgMessageId: streamed.tgMessageId, tgFileId: streamed.tgFileId,
    });
    if (prevPart) ctx.waitUntil(cleanupParts([prevPart], env));

    const streamedHeaders: Record<string, string> = { 'ETag': streamed.etag };
    if (uploadSseMd5) {
      streamedHeaders['x-amz-server-side-encryption-customer-algorithm'] = 'AES256';
      streamedHeaders['x-amz-server-side-encryption-customer-key-MD5'] = uploadSseMd5;
    }
    return new Response(null, { status: 200, headers: streamedHeaders });
  }

  // Read part body (handles AWS chunked streaming format transparently)
  const bodyBuf = await readBody(s3);
  if (!bodyBuf || bodyBuf.byteLength === 0) return errorResponse(400, 'MissingContent', 'Request body is empty.');
  const total = bodyBuf.byteLength;

  // Validate Content-MD5 if provided (same as PutObject)
  const contentMd5 = s3.headers.get('content-md5');
  if (contentMd5) {
    const digest = await crypto.subtle.digest('MD5', bodyBuf);
    const actualMd5 = btoa(String.fromCharCode(...new Uint8Array(digest)));
    if (actualMd5 !== contentMd5) {
      return errorResponse(400, 'BadDigest', 'The Content-MD5 you specified did not match what we received.');
    }
  }

  // Validate x-amz-content-sha256 if a real hash is provided
  const contentSha256 = s3.headers.get('x-amz-content-sha256');
  if (contentSha256 && contentSha256 !== 'UNSIGNED-PAYLOAD' && !contentSha256.startsWith('STREAMING-')) {
    const actualSha256 = await sha256Hex(bodyBuf);
    if (actualSha256 !== contentSha256) {
      return errorResponse(400, 'XAmzContentSHA256Mismatch',
        'The provided \'x-amz-content-sha256\' header does not match what was computed.');
    }
  }

  const etag = await computeEtag(bodyBuf);

  let result;
  try {
    result = await uploadToTelegram(
      bodyBuf,
      bucket.tg_chat_id,
      `${upload.key}.part${partNumber.toString().padStart(4, '0')}`,
      'application/octet-stream',
      env,
      bucket.tg_topic_id,
    );
  } catch (e) {
    if (e instanceof FileTooLargeError) {
      return errorResponse(400, 'EntityTooLarge', e.message);
    }
    if (e instanceof RateLimitError) {
      return errorResponse(503, 'SlowDown', 'Please reduce your request rate.', undefined, e.retryAfter);
    }
    throw e;
  }

  // Check for existing part (S3 allows re-uploading a part with same partNumber)
  const oldPart = await store.getMultipartPart(uploadId, partNumber);

  await store.putMultipartPart({
    uploadId,
    partNumber,
    size: total,
    etag,
    tgChatId: result.tgChatId,
    tgMessageId: result.tgMessageId,
    tgFileId: result.tgFileId,
  });

  // Async cleanup: delete old TG message if part was re-uploaded
  if (oldPart) {
    ctx.waitUntil(cleanupParts([oldPart], env));
  }

  const partHeaders: Record<string, string> = { 'ETag': etag };
  if (uploadSseMd5) {
    partHeaders['x-amz-server-side-encryption-customer-algorithm'] = 'AES256';
    partHeaders['x-amz-server-side-encryption-customer-key-MD5'] = uploadSseMd5;
  }
  return new Response(null, {
    status: 200,
    headers: partHeaders,
  });
}

export async function handleCompleteMultipartUpload(s3: S3Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);

  const uploadId = s3.query.get('uploadId');
  if (!uploadId) return errorResponse(400, 'InvalidArgument', 'Missing uploadId.');

  const upload = await store.getMultipartUpload(uploadId);
  if (!upload || upload.bucket !== s3.bucket || upload.key !== s3.key) return errorResponse(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');

  // Parse completion XML
  if (!s3.body) return errorResponse(400, 'MalformedXML', 'Request body is empty.');
  const bodyText = await new Response(s3.body).text();
  const requestParts = parseCompleteMultipart(bodyText);
  if (requestParts.length === 0) return errorResponse(400, 'MalformedXML', 'You must specify at least one part.');

  const dbParts = await store.getMultipartParts(uploadId);
  const bucket = await store.getBucket(upload.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'Bucket not found.');

  // S3 requires parts in ascending order
  for (let i = 1; i < requestParts.length; i++) {
    if (requestParts[i].partNumber <= requestParts[i - 1].partNumber) {
      return errorResponse(400, 'InvalidPartOrder', 'The list of parts was not in ascending order.');
    }
  }

  // Validate and select only the parts listed in the request (S3 allows skipping parts)
  const selectedParts = [];
  for (const rp of requestParts) {
    const dp = dbParts.find(p => p.part_number === rp.partNumber);
    if (!dp) return errorResponse(400, 'InvalidPart', `Part ${rp.partNumber} not found.`);
    // Normalize ETags: clients may send with or without surrounding quotes
    if (stripQuotes(dp.etag) !== stripQuotes(rp.etag)) return errorResponse(400, 'InvalidPart', `Part ${rp.partNumber} ETag mismatch.`);
    selectedParts.push(dp);
  }

  const sortedParts = selectedParts.sort((a, b) => a.part_number - b.part_number);

  // S3 requires all parts except the last to be at least 5MB
  for (let i = 0; i < sortedParts.length - 1; i++) {
    if (sortedParts[i].size < S3_MIN_PART_SIZE) {
      return errorResponse(400, 'EntityTooSmall',
        `Part ${sortedParts[i].part_number} is ${sortedParts[i].size} bytes, which is below the minimum 5MB limit for non-final parts.`);
    }
  }

  const totalSize = sortedParts.reduce((s, p) => s + p.size, 0);

  // Size limit check. With VPS configured, objects larger than a single 2GB
  // Telegram file are split into multiple <=2GB chunks (see chunked path below).
  // Without VPS the hard cap is 20MB (Bot API getFile limit), same as before.
  if (!env.VPS_URL && totalSize > BOT_API_GETFILE_LIMIT) {
    return errorResponse(400, 'EntityTooLarge', 'Combined size exceeds 20MB limit.');
  }
  const useChunked = !!env.VPS_URL && totalSize > VPS_SINGLE_FILE_MAX;

  let uploadResult: UploadResult | null = null;
  let etag: string;

  // Check if upload has encryption metadata (SSE-C or SSE-S3)
  let uploadSseKeyBase64: string | null = null;
  let uploadSseKeyMd5: string | null = null;
  let uploadUseSseS3 = false;
  if (upload.system_metadata) {
    try {
      const sm = JSON.parse(upload.system_metadata);
      if (sm._sse === 'AES256') {
        uploadSseKeyMd5 = sm._sse_key_md5;
        // SSE-C: parts are stored unencrypted, key is required to encrypt the final consolidated file.
        const sseParams = parseSseCHeaders(s3.headers);
        if (!sseParams) {
          return errorResponse(400, 'InvalidRequest', 'This multipart upload was initiated with SSE-C. You must provide the encryption key to complete it.');
        }
        if (sseParams.keyMd5 !== uploadSseKeyMd5) {
          return errorResponse(400, 'InvalidArgument', 'The SSE-C key does not match the key used to initiate the multipart upload.');
        }
        uploadSseKeyBase64 = sseParams.keyBase64;
      }
      if (sm._sse_s3 === 'AES256') {
        uploadUseSseS3 = true;
      }
    } catch { /* ignore */ }
  }

  // Encryption options shared by every VPS consolidate call (single file or per-chunk)
  const sseOptions: { sseKeyBase64?: string; sseS3KeyBase64?: string } = {};
  if (uploadSseKeyBase64) sseOptions.sseKeyBase64 = uploadSseKeyBase64;
  else if (uploadUseSseS3 && env.SSE_MASTER_KEY) sseOptions.sseS3KeyBase64 = env.SSE_MASTER_KEY;

  // Chunk descriptors (only populated on the chunked path)
  let chunkList: ChunkDesc[] | null = null;

  if (useChunked) {
    // >2GB: split the sorted parts into batches each <=2GB and consolidate every
    // batch into its own Telegram file. Each S3 part is already <=2GB (enforced at
    // UploadPart), so grouping whole parts never overflows a chunk. Offsets are
    // deterministic from batch sizes, so batches can be consolidated concurrently
    // (bounded) and their tg-ids filled in by index — order is preserved.
    const batches = groupPartsIntoChunks(sortedParts, VPS_SINGLE_FILE_MAX);
    let acc = 0;
    const plan = batches.map((batch) => {
      const size = batch.reduce((s, p) => s + p.size, 0);
      const offset = acc;
      acc += size;
      return { batch, offset, size };
    });

    const created: (ChunkDesc | undefined)[] = new Array(plan.length);
    try {
      await mapWithConcurrency(plan, getChunkConcurrency(env), async (p, i) => {
        const r = await consolidateViaVps(
          p.batch, bucket.tg_chat_id,
          `${upload.key}.chunk${i.toString().padStart(4, '0')}`,
          upload.content_type || 'application/octet-stream',
          env, bucket.tg_topic_id, sseOptions,
        );
        created[i] = {
          chunkIndex: i, offset: p.offset, size: p.size,
          tgChatId: r.tgChatId, tgMessageId: r.tgMessageId, tgFileId: r.tgFileId,
        };
      });
    } catch (e) {
      // Roll back TG messages for chunks created so far, plus all uploaded parts
      const done = created.filter((c): c is ChunkDesc => !!c);
      ctx.waitUntil(cleanupParts(done.map(c => ({ tg_chat_id: c.tgChatId, tg_message_id: c.tgMessageId })), env));
      ctx.waitUntil(cleanupParts(sortedParts, env));
      await store.deleteMultipartUpload(uploadId);
      throw e;
    }
    chunkList = created as ChunkDesc[];
    etag = await computeMultipartEtag(sortedParts.map(p => p.etag));
  } else if (totalSize <= BOT_API_GETFILE_LIMIT) {
    // <=20MB: consolidate in Worker memory
    const combined = new Uint8Array(totalSize);
    const downloads = await Promise.all(
      sortedParts.map(part => downloadFromTelegram(part.tg_file_id, env).then(r => r.arrayBuffer())),
    );
    let pos = 0;
    for (let i = 0; i < downloads.length; i++) {
      const buf = downloads[i];
      if (buf.byteLength !== sortedParts[i].size) {
        return errorResponse(500, 'InternalError',
          `Part ${sortedParts[i].part_number} size mismatch: expected ${sortedParts[i].size}, got ${buf.byteLength}`);
      }
      combined.set(new Uint8Array(buf), pos);
      pos += buf.byteLength;
    }

    etag = await computeMultipartEtag(sortedParts.map(p => p.etag));

    // Encrypt consolidated file before uploading (SSE-C or SSE-S3)
    let uploadData: ArrayBuffer = combined.buffer as ArrayBuffer;
    if (uploadSseKeyBase64) {
      uploadData = await encrypt(uploadData, uploadSseKeyBase64);
    } else if (uploadUseSseS3 && env.SSE_MASTER_KEY) {
      uploadData = await encryptS3(uploadData, env.SSE_MASTER_KEY);
    }

    try {
      uploadResult = await uploadToTelegram(
        uploadData,
        bucket.tg_chat_id,
        upload.key,
        upload.content_type || 'application/octet-stream',
        env,
        bucket.tg_topic_id,
      );
    } catch (e) {
      ctx.waitUntil(cleanupParts(sortedParts, env));
      await store.deleteMultipartUpload(uploadId);
      throw e;
    }
  } else {
    // 20MB - 2GB: delegate consolidation to VPS (with optional encryption)
    try {
      const result = await consolidateViaVps(sortedParts, bucket.tg_chat_id, upload.key, upload.content_type || 'application/octet-stream', env, bucket.tg_topic_id, sseOptions);
      uploadResult = result;
      // S3 multipart ETag: computed from part ETags, not content hash
      etag = await computeMultipartEtag(sortedParts.map(p => p.etag));
    } catch (e) {
      ctx.waitUntil(cleanupParts(sortedParts, env));
      await store.deleteMultipartUpload(uploadId);
      throw e;
    }
  }

  // Shared metadata for the destination object row
  const userMetadata = (() => { try { return upload.user_metadata ? JSON.parse(upload.user_metadata) : undefined; } catch { return undefined; } })();
  const systemMetadata = (() => {
    // Merge user-defined system metadata with internal part sizes for GetObject partNumber support
    let base: Record<string, string> = {};
    if (upload.system_metadata) { try { base = JSON.parse(upload.system_metadata); } catch { /* ignore corrupt */ } }
    base['_mp_part_sizes'] = JSON.stringify(sortedParts.map(p => p.size));
    return base;
  })();

  let oldObj: ObjectRow | null;
  if (chunkList) {
    // Chunked object: atomically replace the chunk map + object row, then clean up
    // the previous object's TG messages (its chunks, or its single file).
    const { oldObj: prev, oldChunks } = await store.putChunkedObject({
      bucket: upload.bucket, key: upload.key, size: totalSize, etag,
      contentType: upload.content_type || 'application/octet-stream',
      tgChatId: bucket.tg_chat_id, userMetadata, systemMetadata,
    }, chunkList);
    oldObj = prev;

    ctx.waitUntil(cleanupParts(dbParts, env));
    if (oldChunks.length > 0) ctx.waitUntil(cleanupParts(oldChunks, env));
    // Previous object was a single Telegram file: delete its message
    if (oldObj && oldObj.tg_file_id !== '__zero__' && oldObj.tg_file_id !== CHUNKED_SENTINEL && oldObj.tg_message_id !== 0) {
      const tg = new TelegramClient(env);
      ctx.waitUntil(tg.deleteMessage(oldObj.tg_chat_id, oldObj.tg_message_id).then(() => {}).catch(() => {}));
    }
    if (oldObj) ctx.waitUntil(deleteDerivatives(upload.bucket, upload.key, env, store));
  } else {
    oldObj = await store.putObject({
      bucket: upload.bucket,
      key: upload.key,
      size: totalSize,
      etag,
      contentType: upload.content_type || 'application/octet-stream',
      tgChatId: uploadResult!.tgChatId,
      tgMessageId: uploadResult!.tgMessageId,
      tgFileId: uploadResult!.tgFileId,
      tgFileUniqueId: uploadResult!.tgFileUniqueId,
      userMetadata,
      systemMetadata,
    });

    // Async cleanup: delete ALL part messages (including skipped ones)
    ctx.waitUntil(cleanupParts(dbParts, env));

    // Async cleanup: delete old TG message + stale derivatives if destination was overwritten
    if (oldObj && oldObj.tg_file_id !== '__zero__' && oldObj.tg_file_id !== CHUNKED_SENTINEL && oldObj.tg_file_id !== uploadResult!.tgFileId && oldObj.tg_message_id !== 0) {
      const tg = new TelegramClient(env);
      ctx.waitUntil(tg.deleteMessage(oldObj.tg_chat_id, oldObj.tg_message_id).then(() => {}).catch(() => {}));
    }
    if (oldObj) {
      ctx.waitUntil(deleteDerivatives(upload.bucket, upload.key, env, store));
      // Previous object may have been chunked: remove its orphaned chunk messages/rows
      ctx.waitUntil(deleteChunks(upload.bucket, upload.key, env, store));
    }
  }

  // Purge CDN + R2 cache for destination key (consistent with PutObject/CopyObject)
  ctx.waitUntil(purgeCdnCache(s3.url.origin, upload.bucket, upload.key));
  ctx.waitUntil(purgeR2Cache(env, upload.bucket, upload.key));

  await store.deleteMultipartUpload(uploadId);

  const encodedKey = upload.key.split('/').map(encodeURIComponent).join('/');
  const location = `${s3.url.origin}/${encodeURIComponent(upload.bucket)}/${encodedKey}`;
  const xml = completeMultipartXml(upload.bucket, upload.key, etag, location);
  const completeHeaders: Record<string, string> = {
    'Content-Type': 'application/xml', 'Location': location, 'ETag': etag,
  };
  if (uploadSseKeyBase64 && uploadSseKeyMd5) {
    completeHeaders['x-amz-server-side-encryption-customer-algorithm'] = 'AES256';
    completeHeaders['x-amz-server-side-encryption-customer-key-MD5'] = uploadSseKeyMd5;
  } else if (uploadUseSseS3) {
    completeHeaders['x-amz-server-side-encryption'] = 'AES256';
  }
  return new Response(xml, { status: 200, headers: completeHeaders });
}

export async function handleAbortMultipartUpload(s3: S3Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);
  const uploadId = s3.query.get('uploadId');
  if (!uploadId) return errorResponse(400, 'InvalidArgument', 'Missing uploadId.');

  const upload = await store.getMultipartUpload(uploadId);
  if (!upload || upload.bucket !== s3.bucket || upload.key !== s3.key) return errorResponse(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');

  const parts = await store.getMultipartParts(uploadId);
  ctx.waitUntil(cleanupParts(parts, env));
  await store.deleteMultipartUpload(uploadId);

  return new Response(null, { status: 204 });
}

export async function handleListParts(s3: S3Request, env: Env): Promise<Response> {
  const store = new MetadataStore(env);
  const uploadId = s3.query.get('uploadId');
  if (!uploadId) return errorResponse(400, 'InvalidArgument', 'Missing uploadId.');

  const upload = await store.getMultipartUpload(uploadId);
  if (!upload || upload.bucket !== s3.bucket || upload.key !== s3.key) return errorResponse(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');

  const maxParts = clampInt(s3.query.get('max-parts'), S3_MAX_KEYS_DEFAULT, 0, S3_MAX_KEYS_DEFAULT);
  const partNumberMarker = clampInt(s3.query.get('part-number-marker'), 0, 0, S3_MAX_PART_NUMBER);

  // S3: max-parts=0 returns empty result with IsTruncated=false
  if (maxParts === 0) {
    return xmlResponse(listPartsXml(s3.bucket, s3.key, uploadId, [], false, undefined, 0, partNumberMarker));
  }

  const allParts = await store.getMultipartParts(uploadId);
  const filtered = allParts
    .filter(p => p.part_number > partNumberMarker)
    .sort((a, b) => a.part_number - b.part_number);
  const truncated = filtered.length > maxParts;
  const parts = filtered.slice(0, maxParts);
  const nextMarker = truncated ? parts[parts.length - 1].part_number : undefined;

  return xmlResponse(listPartsXml(s3.bucket, s3.key, uploadId, parts, truncated, nextMarker, maxParts, partNumberMarker));
}

export async function handleListMultipartUploads(s3: S3Request, env: Env): Promise<Response> {
  const store = new MetadataStore(env);

  const bucket = await store.getBucket(s3.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', s3.bucket);

  const prefix = s3.query.get('prefix') || '';
  const delimiter = s3.query.get('delimiter') || '';
  const keyMarker = s3.query.get('key-marker') || '';
  const uploadIdMarker = s3.query.get('upload-id-marker') || '';
  const maxUploads = clampInt(s3.query.get('max-uploads'), S3_MAX_KEYS_DEFAULT, 0, S3_MAX_KEYS_DEFAULT);
  const rawEncodingType = s3.query.get('encoding-type') || undefined;
  if (rawEncodingType && rawEncodingType !== 'url') {
    return errorResponse(400, 'InvalidArgument', 'Invalid encoding type. Only "url" is supported.');
  }
  const encodingType = rawEncodingType;

  const result = await store.listMultipartUploads(s3.bucket, {
    prefix: prefix || undefined,
    delimiter: delimiter || undefined,
    keyMarker: keyMarker || undefined,
    uploadIdMarker: uploadIdMarker || undefined,
    maxUploads,
  });

  return xmlResponse(listMultipartUploadsXml({
    bucket: s3.bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploads,
    isTruncated: result.isTruncated, uploads: result.uploads,
    commonPrefixes: result.commonPrefixes,
    nextKeyMarker: result.nextKeyMarker, nextUploadIdMarker: result.nextUploadIdMarker,
    encodingType,
  }));
}

async function consolidateViaVps(
  parts: Array<{ tg_file_id: string }>,
  chatId: string, filename: string, contentType: string,
  env: Env, messageThreadId?: number | null,
  sseOptions?: { sseKeyBase64?: string; sseS3KeyBase64?: string },
): Promise<UploadResult> {
  const vps = new VpsClient(env);
  const res = await vps.consolidate(parts.map(p => p.tg_file_id), chatId, filename, contentType, messageThreadId, sseOptions);

  const data = await res.json() as Record<string, unknown>;
  if (!data.tgChatId || !data.tgMessageId || !data.tgFileId || !data.tgFileUniqueId) {
    throw new Error(`VPS consolidate returned unexpected structure: ${JSON.stringify(data).slice(0, 200)}`);
  }
  return data as unknown as UploadResult;
}

// Delete part/chunk TG messages, grouped by chat and batched <=100 per call so
// completing a large multipart upload spends ~1 subrequest per 100 parts instead
// of one per part (which would blow the Worker subrequest budget on big objects).
async function cleanupParts(parts: Array<{ tg_chat_id: string; tg_message_id: number }>, env: Env): Promise<void> {
  if (parts.length === 0) return;
  const tg = new TelegramClient(env);

  const byChat = new Map<string, number[]>();
  for (const p of parts) {
    if (!p.tg_message_id) continue; // skip sentinel/zero rows
    const ids = byChat.get(p.tg_chat_id);
    if (ids) ids.push(p.tg_message_id);
    else byChat.set(p.tg_chat_id, [p.tg_message_id]);
  }

  const ops: Promise<unknown>[] = [];
  for (const [chatId, ids] of byChat) {
    for (let i = 0; i < ids.length; i += 100) {
      const batch = ids.slice(i, i + 100);
      ops.push(tg.deleteMessages(chatId, batch).catch(e => {
        console.warn(`Cleanup: failed to delete ${batch.length} part message(s) in ${chatId}:`, e);
      }));
    }
  }
  await Promise.allSettled(ops);
}

function stripQuotes(s: string): string {
  const t = s.trim();
  if (t.startsWith('"') && t.endsWith('"')) return t.slice(1, -1);
  return t;
}

export async function handleUploadPartCopy(s3: S3Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const store = new MetadataStore(env);

  const uploadId = s3.query.get('uploadId');
  const partNumber = parseInt(s3.query.get('partNumber') || '0', 10);
  if (!uploadId || !partNumber) return errorResponse(400, 'InvalidArgument', 'Missing uploadId or partNumber.');
  if (partNumber < 1 || partNumber > S3_MAX_PART_NUMBER) return errorResponse(400, 'InvalidArgument', 'partNumber must be between 1 and 10000.');

  const upload = await store.getMultipartUpload(uploadId);
  if (!upload || upload.bucket !== s3.bucket || upload.key !== s3.key)
    return errorResponse(404, 'NoSuchUpload', 'The specified multipart upload does not exist.');

  const bucket = await store.getBucket(upload.bucket);
  if (!bucket) return errorResponse(404, 'NoSuchBucket', 'Bucket not found.');

  // Parse x-amz-copy-source
  const copySource = s3.headers.get('x-amz-copy-source') || '';
  let decoded: string;
  try {
    decoded = decodeURIComponent(copySource.split('?')[0]);
  } catch {
    return errorResponse(400, 'InvalidArgument', 'Invalid copy source encoding.');
  }
  const trimmed = decoded.startsWith('/') ? decoded.slice(1) : decoded;
  const slashIdx = trimmed.indexOf('/');
  if (slashIdx < 0) return errorResponse(400, 'InvalidArgument', 'Invalid copy source.');

  const srcBucket = trimmed.slice(0, slashIdx);
  const srcKey = trimmed.slice(slashIdx + 1);

  // Check source bucket & object
  const srcBucketRow = await store.getBucket(srcBucket);
  if (!srcBucketRow) return errorResponse(404, 'NoSuchBucket', 'The specified bucket does not exist.', srcBucket);
  const srcObj = await store.getObject(srcBucket, srcKey);
  if (!srcObj) return errorResponse(404, 'NoSuchKey', 'The specified key does not exist.', `/${srcBucket}/${srcKey}`);

  // Conditional copy headers (same precedence as CopyObject)
  const copyIfMatch = s3.headers.get('x-amz-copy-source-if-match');
  if (copyIfMatch && !etagMatches(copyIfMatch, srcObj.etag, true)) {
    return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
  }
  if (!copyIfMatch) {
    const copyIfUnmodifiedSince = s3.headers.get('x-amz-copy-source-if-unmodified-since');
    if (copyIfUnmodifiedSince && new Date(srcObj.last_modified).getTime() > new Date(copyIfUnmodifiedSince).getTime()) {
      return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
    }
  }
  const copyIfNoneMatch = s3.headers.get('x-amz-copy-source-if-none-match');
  if (copyIfNoneMatch && etagMatches(copyIfNoneMatch, srcObj.etag)) {
    return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
  }
  if (!copyIfNoneMatch) {
    const copyIfModifiedSince = s3.headers.get('x-amz-copy-source-if-modified-since');
    if (copyIfModifiedSince && new Date(srcObj.last_modified).getTime() <= new Date(copyIfModifiedSince).getTime()) {
      return errorResponse(412, 'PreconditionFailed', 'At least one of the pre-conditions you specified did not hold.');
    }
  }

  // SSE-C: parse copy-source encryption headers (for encrypted source objects)
  const srcEncrypted = isEncrypted(srcObj.system_metadata);
  const srcEncryptedS3 = isEncryptedS3(srcObj.system_metadata);
  if (srcEncryptedS3 && !env.SSE_MASTER_KEY) {
    return errorResponse(500, 'InternalError', 'Source object is SSE-S3 encrypted but SSE_MASTER_KEY is not configured.');
  }
  let srcSse: ReturnType<typeof parseSseCHeaders> = null;

  if (srcEncrypted) {
    try {
      srcSse = parseSseCHeaders(s3.headers, SSE_COPY_HEADERS);
    } catch (e) {
      if (e instanceof SseCError) return errorResponse(400, 'InvalidArgument', e.message);
      throw e;
    }
    if (!srcSse) {
      return errorResponse(400, 'InvalidRequest', 'The source object was stored using SSE-C. You must provide the source encryption key headers.');
    }
    await validateKeyMd5(srcSse);
    const storedMd5 = getStoredKeyMd5(srcObj.system_metadata);
    if (storedMd5 && srcSse.keyMd5 !== storedMd5) {
      return errorResponse(403, 'AccessDenied', 'The provided source encryption key does not match.');
    }
  }

  const needsDecrypt = srcEncrypted || (srcEncryptedS3 && !!env.SSE_MASTER_KEY);

  // Parse x-amz-copy-source-range (optional, format: bytes=start-end)
  const rangeHeader = s3.headers.get('x-amz-copy-source-range');
  let start = 0;
  let end = srcObj.size - 1;
  if (rangeHeader) {
    const m = rangeHeader.match(/^bytes=(\d+)-(\d+)$/);
    if (!m) return errorResponse(400, 'InvalidArgument', 'Invalid x-amz-copy-source-range.');
    start = parseInt(m[1], 10);
    end = parseInt(m[2], 10);
    if (start > end || end >= srcObj.size) {
      return errorResponse(400, 'InvalidArgument', 'Copy source range out of bounds.');
    }
  }

  // Download source data (or range)
  // Encrypted sources: AES-GCM requires full download + decrypt, then slice range
  let data: ArrayBuffer;
  if (srcObj.size === 0) {
    data = new ArrayBuffer(0);
  } else if (needsDecrypt) {
    // Must download full file, decrypt, then apply range
    let fullData: ArrayBuffer;
    if (srcObj.size <= BOT_API_GETFILE_LIMIT) {
      const tgRes = await downloadFromTelegram(srcObj.tg_file_id, env);
      fullData = await tgRes.arrayBuffer();
    } else if (env.VPS_URL) {
      try {
        const vps = new VpsClient(env);
        const vpsRes = await vps.proxyGet(srcObj.tg_file_id);
        fullData = await vpsRes.arrayBuffer();
      } catch {
        return errorResponse(503, 'ServiceUnavailable', 'Failed to download source object from storage backend.');
      }
    } else {
      return errorResponse(503, 'ServiceUnavailable', 'Source file exceeds 20MB and requires VPS proxy which is not configured.');
    }
    if (srcEncrypted && srcSse) {
      fullData = await decrypt(fullData, srcSse.keyBase64);
    } else if (srcEncryptedS3 && env.SSE_MASTER_KEY) {
      fullData = await decryptS3(fullData, env.SSE_MASTER_KEY);
    }
    data = fullData.slice(start, end + 1);
  } else if (srcObj.size <= BOT_API_GETFILE_LIMIT) {
    const tgRes = await downloadFromTelegram(srcObj.tg_file_id, env);
    const full = await tgRes.arrayBuffer();
    data = full.slice(start, end + 1);
  } else if (env.VPS_URL) {
    try {
      const vps = new VpsClient(env);
      const vpsRes = await vps.proxyRange(srcObj.tg_file_id, start, end);
      data = await vpsRes.arrayBuffer();
    } catch {
      return errorResponse(503, 'ServiceUnavailable', 'Failed to download source object from storage backend.');
    }
  } else {
    return errorResponse(400, 'EntityTooLarge', 'Source object exceeds direct download limit.');
  }

  const total = data.byteLength;
  const etag = await computeEtag(data);

  // Upload part data to TG
  let result;
  try {
    result = await uploadToTelegram(
      data,
      bucket.tg_chat_id,
      `${upload.key}.part${partNumber.toString().padStart(4, '0')}`,
      'application/octet-stream',
      env,
      bucket.tg_topic_id,
    );
  } catch (e) {
    if (e instanceof FileTooLargeError) {
      return errorResponse(400, 'EntityTooLarge', e.message);
    }
    if (e instanceof RateLimitError) {
      return errorResponse(503, 'SlowDown', 'Please reduce your request rate.', undefined, e.retryAfter);
    }
    throw e;
  }

  // Check for existing part (re-upload cleanup)
  const oldPart = await store.getMultipartPart(uploadId, partNumber);

  await store.putMultipartPart({
    uploadId,
    partNumber,
    size: total,
    etag,
    tgChatId: result.tgChatId,
    tgMessageId: result.tgMessageId,
    tgFileId: result.tgFileId,
  });

  if (oldPart) {
    ctx.waitUntil(cleanupParts([oldPart], env));
  }

  const now = new Date(Math.floor(Date.now() / 1000) * 1000).toISOString();
  return xmlResponse(copyPartResultXml(etag, now));
}
