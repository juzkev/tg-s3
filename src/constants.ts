// Telegram Bot API limits
export const BOT_API_GETFILE_LIMIT = 20 * 1024 * 1024; // 20MB: Bot API getFile download max (upload aligned to this)

// VPS Local Bot API limits
// 2000MB (NOT 2GiB): Local Bot API single file max. Telegram uploads big files as
// at most 4000 MTProto parts of 512KB = 2,097,152,000 bytes. Anything larger is
// rejected with "Bad Request: FILE_PARTS_INVALID" — a 2GiB (2,147,483,648) file is
// ~48MiB over the ceiling and fails. Chunk sizing derives from this constant, so
// setting it to 2GiB would let chunked uploads build chunks Telegram refuses.
export const VPS_SINGLE_FILE_MAX = 2000 * 1024 * 1024;

// Sentinel stored in objects.tg_file_id marking an object whose bytes are split
// across multiple <=2GB Telegram files ("chunks"), tracked in the chunks table.
// Mirrors the existing '__zero__' sentinel convention for empty objects.
export const CHUNKED_SENTINEL = '__chunked__';

// S3 limits
export const S3_MAX_KEYS_DEFAULT = 1000;
export const S3_MAX_PART_NUMBER = 10000;
export const S3_MIN_PART_SIZE = 5 * 1024 * 1024; // 5MB: minimum part size (except last)
export const S3_MAX_PRESIGN_EXPIRES = 604800; // 7 days: S3 presigned URL maximum expiry

// R2 Cache thresholds: balance cache value vs R2 operation/storage cost
export const R2_CACHE_MIN_SIZE = 64 * 1024;        // 64KB: below this, TG direct download is fast enough
export const R2_CACHE_MAX_SIZE = 20 * 1024 * 1024;  // 20MB: aligned with Bot API getFile limit (files >20MB go via VPS, not cached to R2)

// Worker memory safety
export const WORKER_BODY_LIMIT = 100 * 1024 * 1024; // 100MB: safe limit for in-memory body buffering (Workers have 128MB total)

// Telegram API
export const TG_API_TIMEOUT = 25_000; // 25s: leave 5s margin for CF Worker 30s limit

// VPS proxy
export const VPS_PROXY_TIMEOUT = 25_000; // 25s: same margin as TG API (for streaming/API calls)
export const VPS_LONG_TIMEOUT = 10 * 60 * 1000; // 10 min: for large file upload and consolidate (up to 2GB)

// Upper bound for CHUNK_CONSOLIDATE_CONCURRENCY (parallel per-chunk consolidations
// at multipart completion). Caps how many <=2GB temp files / Telegram transfers the
// VPS runs at once, so a misconfigured value can't exhaust VPS disk or trip flood limits.
export const CHUNK_CONSOLIDATE_CONCURRENCY_MAX = 8;

// Cache-Control defaults (applied when no stored system metadata or response-* override)
export const CACHE_CONTROL_IMMUTABLE = 'public, max-age=31536000, immutable'; // images: 1 year
export const CACHE_CONTROL_DEFAULT = 'public, max-age=86400'; // other files: 1 day
