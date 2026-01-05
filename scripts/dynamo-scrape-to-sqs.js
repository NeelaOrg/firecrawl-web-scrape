#!/usr/bin/env node
"use strict";

const path = require("path");
const dotenv = require("dotenv");
dotenv.config({ path: path.join(__dirname, ".env"), override: true });

const {
  DynamoDBClient,
} = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  ExecuteStatementCommand: DocumentExecuteStatementCommand,
} = require("@aws-sdk/lib-dynamodb");
const {
  SQSClient,
  SendMessageCommand,
} = require("@aws-sdk/client-sqs");
const {
  S3Client,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const crypto = require("crypto");

const DEFAULT_API_URL = "http://localhost:3002/v2/scrape";
const DEFAULT_CHUNK_SIZE = 2000;
const DEFAULT_OVERLAP_SIZE = 300;
const DEFAULT_MAX_SQS_MESSAGE_BYTES = 250000;
const DEFAULT_CONCURRENCY = 3;
const DEFAULT_TIMEOUT_MS = 30000;
const URL_ATTRIBUTE = "url";

function log(message, data) {
  const prefix = `[${new Date().toISOString()}]`;
  if (data !== undefined) {
    console.log(prefix, message, data);
  } else {
    console.log(prefix, message);
  }
}

function md5Hex(value) {
  return crypto.createHash("md5").update(String(value)).digest("hex");
}

function parseJsonMaybe(value) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  try {
    return JSON.parse(value);
  } catch (_) {
    return value;
  }
}

function parseIntValue(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getNestedValue(obj, path) {
  if (!obj || !path) {
    return undefined;
  }
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (current && Object.prototype.hasOwnProperty.call(current, part)) {
      current = current[part];
    } else {
      return undefined;
    }
  }
  return current;
}

function chunkText(value, chunkSize, overlapSize) {
  const text = value ?? "";
  if (text.length === 0) {
    return [];
  }
  const safeChunkSize = Math.max(1, Math.floor(chunkSize));
  const safeOverlap = Math.max(
    0,
    Math.min(Math.floor(overlapSize), safeChunkSize - 1),
  );

  if (text.length <= safeChunkSize) {
    return [text];
  }

  const chunks = [];
  let start = 0;
  while (start < text.length) {
    const end = Math.min(start + safeChunkSize, text.length);
    const slice = text.slice(start, end);
    if (!slice) {
      break;
    }
    chunks.push(slice);
    if (end >= text.length) {
      break;
    }
    start = end - safeOverlap;
  }
  return chunks;
}

function buildChunkedItems(items, chunkSize, overlapSize) {
  const chunked = [];
  for (const item of items) {
    const content = item.content ?? item.markdown ?? "";
    const chunks = chunkText(content, chunkSize, overlapSize);
    if (chunks.length === 0) {
      continue;
    }
    for (let idx = 0; idx < chunks.length; idx += 1) {
      chunked.push({
        ...item,
        content: chunks[idx],
        section_chunk_index: idx,
        section_chunk_count: chunks.length,
      });
    }
  }
  return chunked;
}

function buildAuthHeader(apiKeyHeader, apiKey) {
  if (!apiKeyHeader || !apiKey) {
    return {};
  }
  if (apiKeyHeader.toLowerCase() === "authorization") {
    const value = apiKey.startsWith("Bearer ") ? apiKey : `Bearer ${apiKey}`;
    return { [apiKeyHeader]: value };
  }
  return { [apiKeyHeader]: apiKey };
}

async function sendChunkedResultsToSink(
  sqs,
  sinkUrl,
  result,
  maxMessageBytes,
) {
  const items = result.items || [];
  const totalCount = items.length;
  const base = Object.fromEntries(
    Object.entries(result).filter(([key]) => key !== "items"),
  );

  const batches = [];
  let current = [];

  const sizeWithItems = candidateItems => {
    const payload = {
      ...base,
      items: candidateItems,
      count: candidateItems.length,
      total_count: totalCount,
      batch_index: 0,
      batch_count: 0,
    };
    return Buffer.byteLength(JSON.stringify(payload), "utf8");
  };

  for (const item of items) {
    const tentative = current.concat([item]);
    if (sizeWithItems(tentative) > maxMessageBytes) {
      if (!current.length) {
        throw new Error("Single item exceeds SQS message size limit");
      }
      batches.push(current);
      current = [item];
    } else {
      current = tentative;
    }
  }
  if (current.length) {
    batches.push(current);
  }

  const batchCount = batches.length || 1;
  for (let idx = 0; idx < batches.length; idx += 1) {
    const batchItems = batches[idx];
    const payload = {
      ...base,
      items: batchItems,
      count: batchItems.length,
      total_count: totalCount,
      batch_index: idx,
      batch_count: batchCount,
    };
    const body = JSON.stringify(payload);
    if (Buffer.byteLength(body, "utf8") > maxMessageBytes) {
      throw new Error("Chunked payload still exceeds SQS size limit");
    }
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: sinkUrl,
        MessageBody: body,
        MessageAttributes: {
          batch_index: { StringValue: String(idx), DataType: "Number" },
          batch_count: { StringValue: String(batchCount), DataType: "Number" },
          total_count: { StringValue: String(totalCount), DataType: "Number" },
        },
      }),
    );
  }
}

async function sendFailureToDlq(sqs, dlqUrl, payload) {
  if (!dlqUrl) {
    return;
  }
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: dlqUrl,
      MessageBody: JSON.stringify(payload),
    }),
  );
}

async function loadUrls(config, ddb) {
  const urls = [];
  let nextToken = undefined;

  log("Fetching URLs with PartiQL statement", {
    statement: config.partiqlStatement,
    limit: config.partiqlLimit ?? "none",
  });

  do {
    const response = await ddb.send(
      new DocumentExecuteStatementCommand({
        Statement: config.partiqlStatement,
        NextToken: nextToken,
        ...(config.partiqlLimit ? { Limit: config.partiqlLimit } : {}),
      }),
    );
    nextToken = response.NextToken;

    const items = response.Items || [];
    for (const item of items) {
      const value = getNestedValue(item, URL_ATTRIBUTE);
      if (typeof value === "string" && value.trim().length > 0) {
        urls.push(value.trim());
        if (config.partiqlLimit && urls.length >= config.partiqlLimit) {
          log("Reached PartiQL limit, stopping fetch", {
            limit: config.partiqlLimit,
            totalUrls: urls.length,
          });
          return urls;
        }
      }
    }
    log("Fetched DynamoDB page", {
      pageItems: items.length,
      totalUrls: urls.length,
      hasNextToken: !!nextToken,
    });
  } while (nextToken);

  return urls;
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return response;
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeScrapeResult(raw, fallbackUrl) {
  if (raw && Array.isArray(raw.items)) {
    return { url: raw.url || fallbackUrl, items: raw.items };
  }
  if (raw && raw.data && Array.isArray(raw.data.items)) {
    return { url: raw.data.url || fallbackUrl, items: raw.data.items };
  }
  return null;
}

async function scrapeAndQueue(url, config, sqs, headers) {
  log("Scrape start", { url });
  const payload = {
    ...(config.scrapeOptions || {}),
    url,
  };

  const response = await fetchWithTimeout(
    config.apiUrl,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...headers,
      },
      body: JSON.stringify(payload),
    },
    config.timeoutMs,
  );

  const bodyText = await response.text();
  let bodyJson;
  try {
    bodyJson = JSON.parse(bodyText);
  } catch (_) {
    bodyJson = null;
  }

  if (!response.ok) {
    throw new Error(
      `API error ${response.status}: ${bodyText.slice(0, 500)}`,
    );
  }

  const normalized = normalizeScrapeResult(bodyJson, url);
  if (!normalized || !Array.isArray(normalized.items)) {
    throw new Error("Unexpected scrape response shape (missing items)");
  }

  const chunkedItems = buildChunkedItems(
    normalized.items,
    config.chunkSize,
    config.overlapSize,
  );

  if (!config.sinkSqsUrl) {
    throw new Error("SINK_SQS_URL is not configured");
  }

  if (config.scrapeOutputBucket && !config.s3) {
    throw new Error("S3 client not configured for scrape output bucket");
  }

  const result = {
    url: normalized.url,
    count: chunkedItems.length,
    items: chunkedItems,
  };

  if (config.scrapeOutputBucket) {
    const key = `scrapes/${md5Hex(url)}-${Date.now()}.json`;
    await config.s3.send(
      new PutObjectCommand({
        Bucket: config.scrapeOutputBucket,
        Key: key,
        Body: Buffer.from(JSON.stringify(result)),
        ContentType: "application/json",
      }),
    );
  }

  await sendChunkedResultsToSink(
    sqs,
    config.sinkSqsUrl,
    result,
    config.maxMessageBytes,
  );
  log("Scrape done", { url, chunks: chunkedItems.length });
}

async function runWithConcurrency(values, concurrency, handler) {
  const queue = values.slice();
  const workers = Array.from({ length: concurrency }, async () => {
    while (queue.length > 0) {
      const value = queue.shift();
      if (value === undefined) {
        return;
      }
      await handler(value);
    }
  });
  await Promise.all(workers);
}

async function main() {
  if (typeof fetch !== "function") {
    throw new Error("Global fetch is not available. Use Node.js 18+.");
  }

  const partiqlStatement = process.env.DDB_PARTIQL_STATEMENT;
  if (!partiqlStatement) {
    throw new Error("DDB_PARTIQL_STATEMENT is required.");
  }

  const config = {
    partiqlStatement,
    partiqlLimit: parseIntValue(process.env.DDB_PARTIQL_LIMIT, undefined),
    apiUrl: process.env.SCRAPE_API_URL || DEFAULT_API_URL,
    apiKeyHeader: process.env.API_KEY_HEADER || "Authorization",
    apiKey: process.env.API_KEY,
    scrapeOptions: parseJsonMaybe(process.env.SCRAPE_OPTIONS_JSON) || {
      formats: ["markdown"],
      optimizedScrapeOutput: true,
      onlyMainContent: true,
    },
    chunkSize: parseIntValue(process.env.CHUNK_SIZE, DEFAULT_CHUNK_SIZE),
    overlapSize: parseIntValue(process.env.OVERLAP_SIZE, DEFAULT_OVERLAP_SIZE),
    maxMessageBytes: parseIntValue(
      process.env.MAX_SQS_MESSAGE_BYTES,
      DEFAULT_MAX_SQS_MESSAGE_BYTES,
    ),
    sinkSqsUrl: process.env.SINK_SQS_URL,
    dlqSqsUrl: process.env.DEAD_LETTER_SQS_URL,
    scrapeOutputBucket:
      process.env.SCRAPE_OUTPUT_BUCKET ||
      (process.env.ROOT_BUCKET
        ? `${process.env.ROOT_BUCKET}-scrape-results`
        : ""),
    timeoutMs: parseIntValue(process.env.API_TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
    concurrency: parseIntValue(process.env.CONCURRENCY, DEFAULT_CONCURRENCY),
  };

  const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
  const sqs = new SQSClient({});
  const s3 = new S3Client({});
  config.s3 = s3;

  log("Starting run", {
    apiUrl: config.apiUrl,
    concurrency: config.concurrency,
  });

  const urls = await loadUrls(config, ddb);
  const uniqueUrls = Array.from(new Set(urls));

  log("Loaded URLs from DynamoDB", { count: uniqueUrls.length });

  const headers = buildAuthHeader(config.apiKeyHeader, config.apiKey);
  let successCount = 0;
  let failureCount = 0;

  await runWithConcurrency(uniqueUrls, config.concurrency, async url => {
    try {
      await scrapeAndQueue(url, config, sqs, headers);
      successCount += 1;
      process.stdout.write(".");
    } catch (error) {
      failureCount += 1;
      const errMessage = error instanceof Error ? error.message : String(error);
      console.error(`\nFailed for ${url}: ${errMessage}`);
      await sendFailureToDlq(sqs, config.dlqSqsUrl, {
        url,
        error: errMessage,
        source: "dynamo-scrape-to-sqs",
      });
    }
  });

  console.log(
    `\nDone. Success: ${successCount}, Failed: ${failureCount}.`,
  );
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
