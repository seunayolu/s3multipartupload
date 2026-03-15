import express from 'express';
import multer from 'multer';
import cors from 'cors';
import { createHash } from 'crypto';
import { SSMClient, GetParametersCommand } from '@aws-sdk/client-ssm';
import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  ListPartsCommand,
} from '@aws-sdk/client-s3';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import pLimit from 'p-limit';

// ─────────────────────────────────────────────
// App bootstrap
// ─────────────────────────────────────────────

const app    = express();
const region = process.env.AWS_REGION || 'us-east-1';

app.use(cors());
app.use(express.json());

const upload = multer({ storage: multer.memoryStorage() });
const ssm    = new SSMClient({ region });

let s3, dynamo, config = {};

app.get('/health', (_req, res) => res.status(200).send('OK'));

async function bootstrap() {
  try {
    const { Parameters } = await ssm.send(new GetParametersCommand({
      Names: ['/app/s3-bucket', '/app/dynamo-table'],
      WithDecryption: true,
    }));

    Parameters.forEach(p => {
      const key = p.Name.split('/').pop().replace('-', '');
      config[key] = p.Value;
    });

    // Disable SDK's built-in retries — our uploadPartWithRetry wrapper owns all retry logic
    s3     = new S3Client({ region, maxAttempts: 1 });
    dynamo = new DynamoDBClient({ region });

    app.listen(3000, () => console.log('🚀 Manual Multipart Backend active on port 3000'));
  } catch (err) {
    console.error('Bootstrap Error:', err);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────

const CHUNK_SIZE   = 1024 * 1024 * 5; // 5 MB — S3 minimum for all parts except the last
const CONCURRENCY  = 6;               // simultaneous UploadPart requests

// Transient S3/network errors that are safe to retry
const RETRYABLE_CODES = new Set([
  'RequestTimeout',
  'RequestThrottled',
  'SlowDown',
  'ServiceUnavailable',
  'InternalError',
]);

// ─────────────────────────────────────────────
// Helper: MD5 checksum
//
// S3 requires Content-MD5 as base64 in the request header.
// S3 returns the ETag as a quoted hex string in the response.
// Same hash, two encodings — we compute both upfront to compare later.
// ─────────────────────────────────────────────

function computeMD5(buffer) {
  const hash = createHash('md5').update(buffer).digest();
  return {
    hex:    hash.toString('hex'),     // "2f4a9399fba4164dab287b1326f10e02"
    base64: hash.toString('base64'),  // "L0qTmfukFk2rKHsTJvEOAg==" — for Content-MD5 header
  };
}

// ─────────────────────────────────────────────
// Helper: upload one part with retry + integrity check
//
// Responsibilities:
//   1. Attach Content-MD5 header so S3 rejects corrupted bytes at the edge
//   2. Verify the returned ETag matches our client-computed MD5
//   3. Retry transient 5xx / throttle errors with exponential backoff + jitter
//   4. Immediately surface non-retryable errors (4xx, BadDigest, integrity mismatch)
// ─────────────────────────────────────────────

async function uploadPartWithRetry(s3Client, params, { maxAttempts = 4, baseDelayMs = 1000, clientMD5 } = {}) {
  let lastErr;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await s3Client.send(new UploadPartCommand({
        ...params,
        ...(clientMD5 ? { ContentMD5: clientMD5.base64 } : {}),
        // Content-MD5 tells S3: "if what you received doesn't hash to this, return 400 BadDigest
        // immediately rather than silently storing corrupted bytes"
      }));

      // ETag arrives as a quoted hex string e.g. '"2f4a9399..."' — strip the quotes
      const etagHex = response.ETag.replace(/"/g, '');

      // Client-side integrity verification: our MD5 of the chunk must equal S3's MD5
      // If Content-MD5 is set, S3 already rejected mismatches — this is a defence-in-depth
      // check that catches bugs where the header was stripped by a proxy or the SDK
      if (clientMD5) {
        if (etagHex === clientMD5.hex) {
          console.log(
            `[S3] Part ${params.PartNumber} integrity OK ✓` +
            ` | client MD5: ${clientMD5.hex}` +
            ` | S3 ETag:    ${etagHex}` +
            (attempt > 1 ? ` | succeeded on attempt ${attempt}` : ''),
          );
        } else {
          // Should not happen when Content-MD5 is set — S3 would have rejected first.
          // Reaching here means the header was silently dropped somewhere in transit.
          throw new Error(
            `[INTEGRITY] Part ${params.PartNumber} MD5 MISMATCH —` +
            ` client: ${clientMD5.hex}, S3 ETag: ${etagHex}`,
          );
        }
      } else if (attempt > 1) {
        console.log(`[S3] Part ${params.PartNumber} succeeded on attempt ${attempt}. ETag: ${etagHex}`);
      }

      return response;

    } catch (err) {
      // BadDigest = S3 received bytes that don't match the Content-MD5 header.
      // This means in-transit corruption — retrying the same bytes won't help.
      if (err.name === 'BadDigest' || err.Code === 'BadDigest') {
        console.error(
          `[S3] Part ${params.PartNumber} BAD DIGEST —` +
          ` bytes were corrupted in transit. Not retrying.`,
        );
        throw err;
      }

      // Integrity mismatch thrown above — also non-retryable
      if (err.message?.startsWith('[INTEGRITY]')) {
        console.error(err.message);
        throw err;
      }

      lastErr = err;

      const isRetryable =
        RETRYABLE_CODES.has(err.name) ||
        err.$retryable?.throttling    ||
        (err.$metadata?.httpStatusCode ?? 0) >= 500;

      const isLastAttempt = attempt === maxAttempts;

      if (!isRetryable || isLastAttempt) {
        console.error(
          `[S3] Part ${params.PartNumber} FAILED` +
          ` (attempt ${attempt}/${maxAttempts}` +
          `, code: ${err.name}` +
          `, status: ${err.$metadata?.httpStatusCode ?? 'n/a'})`,
        );
        throw err;
      }

      // Exponential backoff: base × 2^(attempt-1), capped at 30 s, ±20 % jitter
      // Jitter is critical when uploading parts in parallel — without it all in-flight
      // parts that hit a throttle error would retry at the exact same moment (thundering herd)
      const expDelay = baseDelayMs * Math.pow(2, attempt - 1);
      const jitter   = expDelay * 0.2 * (Math.random() * 2 - 1);
      const delay    = Math.min(30_000, Math.round(expDelay + jitter));

      console.warn(
        `[S3] Part ${params.PartNumber} failed` +
        ` (attempt ${attempt}/${maxAttempts}, code: ${err.name}).` +
        ` Retrying in ${delay} ms…`,
      );
      await new Promise(r => setTimeout(r, delay));
    }
  }

  throw lastErr;
}

// ─────────────────────────────────────────────
// Helper: abort multipart upload + verify cleanup
//
// AbortMultipartUpload returns 204 No Content — no body, no confirmation.
// We follow up with ListParts: a NoSuchUpload error is the proof of deletion.
//
// If the abort itself fails (network loss, IAM gap), we log loudly and return
// without throwing — the caller's response is already determined at this point,
// and the S3 lifecycle rule is the fallback for orphaned parts.
// ─────────────────────────────────────────────

async function abortWithVerification(s3Client, { Bucket, Key, UploadId }) {
  // Step 1 — send abort
  try {
    await s3Client.send(new AbortMultipartUploadCommand({ Bucket, Key, UploadId }));
    console.log(`[S3] Abort sent for UploadId: ${UploadId}`);
  } catch (abortErr) {
    if (abortErr.name === 'NoSuchUpload') {
      // UploadId already gone — expired TTL, or abort already ran (e.g. double catch)
      console.log(`[S3] Abort skipped — UploadId already cleaned up: ${UploadId}`);
      return;
    }
    // Any other failure (AccessDenied, network) — log and let lifecycle rule handle it
    console.error(
      `[S3] Abort FAILED for UploadId: ${UploadId}` +
      ` — staged parts may leak.` +
      ` Error: ${abortErr.name}: ${abortErr.message}`,
    );
    return;
  }

  // Step 2 — verify via ListParts
  // After a successful abort, ListParts must throw NoSuchUpload.
  // If it returns 200 with data, the abort didn't fully propagate (rare under eventual consistency).
  try {
    await s3Client.send(new ListPartsCommand({ Bucket, Key, UploadId }));

    // Reached here — ListParts still returned 200, parts may still exist
    console.error(
      `[S3] ABORT VERIFICATION FAILED —` +
      ` ListParts still returned data for UploadId: ${UploadId}.` +
      ` S3 lifecycle rule will clean these up within the configured retention window.`,
    );
  } catch (listErr) {
    if (listErr.name === 'NoSuchUpload') {
      // Expected success path — UploadId is gone, all parts deleted
      console.log(
        `[S3] Abort verified ✓ — UploadId ${UploadId} no longer exists.` +
        ` All staged parts have been deleted.`,
      );
    } else {
      console.error(
        `[S3] ListParts check failed unexpectedly:` +
        ` ${listErr.name}: ${listErr.message}`,
      );
    }
  }
}

// ─────────────────────────────────────────────
// Route: POST /upload
// ─────────────────────────────────────────────

app.post('/upload', upload.single('file'), async (req, res) => {
  const metadata = JSON.parse(req.body.metadata);
  const file     = req.file;
  const key      = `${metadata.category}/${Date.now()}-${file.originalname}`;
  const totalParts = Math.ceil(file.size / CHUNK_SIZE);

  let uploadId;

  try {
    // ── PHASE 1: INITIATE ────────────────────
    // CreateMultipartUpload reserves staging space in S3 and returns an UploadId.
    // This UploadId is the session token — every UploadPart and the final
    // CompleteMultipartUpload call must reference it. Losing it means the staged
    // parts cannot be assembled or cleaned up programmatically.

    const init = await s3.send(new CreateMultipartUploadCommand({
      Bucket:      config.s3bucket,
      Key:         key,
      ContentType: file.mimetype,
    }));

    uploadId = init.UploadId;
    console.log(
      `[S3] Handshake started` +
      ` | UploadId: ${uploadId}` +
      ` | Parts: ${totalParts}` +
      ` | File size: ${(file.size / (1024 * 1024)).toFixed(2)} MB`,
    );

    // ── PHASE 2: CHUNKING (parallel with sliding concurrency window) ─────────
    // pLimit holds at most CONCURRENCY UploadPart requests in-flight at any time.
    // Unlike fixed batches (Promise.all over slices), a completed slot immediately
    // picks up the next queued part — no idle waiting for the slowest part in a batch.
    //
    // Parts may complete out of order — that is expected. The .sort() before
    // CompleteMultipartUpload ensures S3 receives them in the correct sequence.

    const limit          = pLimit(CONCURRENCY);
    const completedParts = [];

    const uploadTasks = Array.from({ length: totalParts }, (_, i) => {
      const partNumber = i + 1;
      const start      = i * CHUNK_SIZE;
      const end        = Math.min(start + CHUNK_SIZE, file.size);
      const chunk      = file.buffer.subarray(start, end);

      // Compute MD5 before sending:
      //   hex    → compare against the ETag S3 returns (integrity verification)
      //   base64 → send as Content-MD5 header (S3 rejects corrupted bytes at ingress)
      const clientMD5 = computeMD5(chunk);

      return limit(async () => {
        console.log(
          `[S3] Part ${partNumber}/${totalParts} starting` +
          ` | bytes ${start}–${end - 1}` +
          ` | size ${((end - start) / (1024 * 1024)).toFixed(2)} MB` +
          ` | MD5: ${clientMD5.hex}`,
        );

        const partResponse = await uploadPartWithRetry(
          s3,
          {
            Bucket:     config.s3bucket,
            Key:        key,
            UploadId:   uploadId,
            PartNumber: partNumber,
            Body:       chunk,
          },
          { clientMD5 },
        );

        return { ETag: partResponse.ETag, PartNumber: partNumber };
      });
    });

    const results = await Promise.all(uploadTasks);

    // Sort is required — Promise.all preserves task order, but out-of-order completions
    // mean the ETag list must be explicitly sorted before sending to CompleteMultipartUpload
    completedParts.push(...results.sort((a, b) => a.PartNumber - b.PartNumber));

    // ── PHASE 3: FINALIZE ────────────────────
    // Send the full parts manifest — all (ETag, PartNumber) pairs — in one request.
    // S3 stitches the staged parts in PartNumber order into the final object.
    // The response ETag will be "hex-N" where N = number of parts (e.g. "17f3f437…-318").
    // This composite ETag is the permanent multipart fingerprint — visible in S3 console forever.

    const finalResult = await s3.send(new CompleteMultipartUploadCommand({
      Bucket:           config.s3bucket,
      Key:              key,
      UploadId:         uploadId,
      MultipartUpload:  { Parts: completedParts },
    }));

    const fileUrl = `https://${config.s3bucket}.s3.${region}.amazonaws.com/${key}`;

    console.log(
      `[S3] Upload complete` +
      ` | Final ETag: ${finalResult.ETag}` +
      ` | URL: ${fileUrl}`,
    );

    // ── PHASE 4: PERSIST ─────────────────────
    // Write the record to DynamoDB after S3 confirms assembly.
    // S3 holds the bytes; DynamoDB holds the queryable index.
    // Storing the ETag allows future integrity audits against the live S3 object.

    await dynamo.send(new PutItemCommand({
      TableName: config.dynamotable,
      Item: {
        FileID:       { S: Date.now().toString() },
        Title:        { S: metadata.title },
        Category:     { S: metadata.category },
        S3_Url:       { S: fileUrl },
        S3_ETag:      { S: finalResult.ETag },
        FileSize_MB:  { N: (file.size / (1024 * 1024)).toFixed(2) },
      },
    }));

    console.log(`[DynamoDB] Record persisted | Title: ${metadata.title} | Category: ${metadata.category}`);

    res.status(200).json({ status: 'Success', url: fileUrl });

  } catch (err) {
    console.error('[S3 ERROR]', err.name, err.message);

    // Abort and verify cleanup for any failure after the UploadId was obtained.
    // abortWithVerification never throws — it logs and defers to the lifecycle rule on failure.
    if (uploadId) {
      await abortWithVerification(s3, {
        Bucket:   config.s3bucket,
        Key:      key,
        UploadId: uploadId,
      });
    }

    res.status(500).json({ error: err.message });
  }
});

bootstrap();