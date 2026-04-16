/**
 * src/routes/admin.js
 * Admin UI + APIs for runtime status, cron management and S3 tests.
 */

import { randomBytes } from 'crypto'
import { Readable } from 'stream'
import { readFileSync } from 'fs'
import { dirname, join } from 'path'
import { fileURLToPath } from 'url'
import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
  ListObjectsV2Command,
  PutObjectCommand,
} from '@aws-sdk/client-s3'

import config from '../config.js'
import {
  deleteAccount,
  getAccountById,
  getAllAccounts,
  getAllBuckets,
  getAllRoutes,
  getTrackedRoutesByAccount,
  listPublicRoutes,
  ROUTE_SCOPE,
  ROUTE_STATE,
  commitUploadedObjectMetadata,
  finalizeRouteDelete,
  upsertBucketRecord,
  upsertRoute,
  upsertAccount,
} from '../db.js'
import { getAccountsStats, reloadAccountsFromRTDB, reloadAccountsFromSQLite, syncAccountsFromRows } from '../accountPool.js'
import { rtdbBatchPatch } from '../firebase.js'
import { buildRtdbAccountPath, suggestAccountId, validateAccountIdForRealtime } from '../accountId.js'
import { getRtdbState } from './health.js'
import {
  getCronJobKinds,
  listCronJobs,
  removeCronJob,
  runCronJobNow,
  saveCronJob,
} from '../cronScheduler.js'
import { createS3Client } from '../inventoryScanner.js'
import { resolveS3SigningRegion } from '../s3Signing.js'
import {
  isEmailOwner,
  isSupabaseAccessToken,
  normalizeSupabaseAccessTokenExp,
  previewSupabaseS3,
} from '../supabaseS3.js'
import { buildRtdbRouteDocument, encodeKey, PUBLIC_PROXY_BUCKET } from '../metadata.js'
import { buildDirectPublicObjectUrl } from '../publicObjectUrl.js'
import { syncAccountsUsageBatch, syncRouteToRtdb } from '../controlPlane.js'
import { refreshMetadataMetrics } from './metrics.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const adminHtml = readFileSync(join(__dirname, '..', 'admin-ui.html'), 'utf-8')
const adminIcon = readFileSync(join(__dirname, '..', 'admin-icon.svg'), 'utf-8')
const DEFAULT_ADMIN_QUOTA_BYTES = 1024 * 1024 * 1024

const adminServiceWorker = `
const CACHE_NAME = 's3proxy-admin-v1'
const ADMIN_SHELL = ['/admin', '/admin/manifest.webmanifest', '/admin/icon.svg']

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(ADMIN_SHELL)).then(() => self.skipWaiting()),
  )
})

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim()),
  )
})

self.addEventListener('fetch', (event) => {
  const request = event.request
  if (request.method !== 'GET') return

  const url = new URL(request.url)
  if (!url.pathname.startsWith('/admin') || url.pathname.startsWith('/admin/api/')) return

  event.respondWith(
    fetch(request)
      .then((response) => {
        const clone = response.clone()
        caches.open(CACHE_NAME).then((cache) => cache.put(request, clone)).catch(() => {})
        return response
      })
      .catch(() => caches.match(request).then((cached) => cached || caches.match('/admin'))),
  )
})
`.trim()

function formatPercent(used, quota) {
  if (!quota) return 0
  return Number(((used / quota) * 100).toFixed(2))
}

function toPublicAccount(row) {
  return {
    accountId: row.account_id,
    accessKeyId: row.access_key_id,
    endpoint: row.endpoint,
    region: row.region,
    bucket: row.bucket,
    publicBucket: row.public_bucket === 1 || row.public_bucket === true,
    addressingStyle: row.addressing_style ?? 'path',
    payloadSigningMode: row.payload_signing_mode ?? 'unsigned',
    emailOwner: row.email_owner ?? '',
    supabaseAccessTokenExp: row.supabase_access_token_exp ?? null,
    supabaseAccessTokenExperimental: row.supabase_access_token_exp ?? null,
    hasSupabaseAccessToken: Boolean(row.supabase_access_token),
    active: row.active === 1 || row.active === true,
    usedBytes: row.used_bytes ?? 0,
    quotaBytes: row.quota_bytes ?? 0,
    usedPercent: formatPercent(row.used_bytes ?? 0, row.quota_bytes ?? 0),
    addedAt: row.added_at ?? null,
    hasSecret: Boolean(row.secret_key),
  }
}

function normalizeString(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

function normalizeBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0

  const normalized = String(value).trim().toLowerCase()
  if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) return true
  if (['false', '0', 'no', 'n', 'off'].includes(normalized)) return false
  return fallback
}

function normalizeAddressingStyle(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'path'
  if (['path', 'path-style', 'path_style'].includes(raw)) return 'path'
  if (['virtual', 'virtual-hosted', 'virtual_hosted', 'virtual-hosted-style'].includes(raw)) return 'virtual'
  return ''
}

function normalizePayloadSigningMode(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'unsigned'
  if (['unsigned', 'unsigned-payload', 'unsigned_payload'].includes(raw)) return 'unsigned'
  if (['signed', 'strict', 'required'].includes(raw)) return 'signed'
  return ''
}

function readAccountField(payload, existing, aliases = []) {
  for (const alias of aliases) {
    if (payload && typeof payload === 'object' && Object.prototype.hasOwnProperty.call(payload, alias)) {
      return payload[alias]
    }
    const parts = String(alias).split('.')
    let current = payload
    let found = true
    for (const part of parts) {
      if (!current || typeof current !== 'object' || !(part in current)) {
        found = false
        break
      }
      current = current[part]
    }
    if (found && current !== undefined) return current
  }

  return existing
}

function normalizePositiveInteger(value, fallback, fieldName, errors) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) {
    errors.push(`${fieldName} must be a positive number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function normalizeNonNegativeInteger(value, fallback, fieldName, errors) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric < 0) {
    errors.push(`${fieldName} must be a non-negative number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function summarizeS3Error(err) {
  const metadata = err?.$metadata ?? {}
  const statusCode = Number(metadata.httpStatusCode)

  return {
    name: normalizeString(err?.name) || null,
    code: normalizeString(err?.Code ?? err?.code) || null,
    message: normalizeString(err?.message) || null,
    httpStatusCode: Number.isFinite(statusCode) ? statusCode : null,
    requestId: normalizeString(metadata.requestId) || null,
    extendedRequestId: normalizeString(metadata.extendedRequestId) || null,
    cfId: normalizeString(metadata.cfId) || null,
  }
}

function formatS3ErrorSummary(summary) {
  if (!summary) return 'Unknown S3 error'

  const parts = []
  if (summary.name) parts.push(summary.name)
  if (summary.code && summary.code !== summary.name) parts.push(`code=${summary.code}`)
  if (summary.httpStatusCode) parts.push(`status=${summary.httpStatusCode}`)
  if (summary.message) parts.push(summary.message)

  return parts.length > 0 ? parts.join(' | ') : 'Unknown S3 error'
}

function isMissingBucketSummary(summary) {
  if (!summary) return false
  if (summary.httpStatusCode === 404) return true

  const combined = `${summary.name ?? ''} ${summary.code ?? ''} ${summary.message ?? ''}`.toLowerCase()
  return combined.includes('nosuchbucket')
    || combined.includes('notfound')
    || combined.includes('bucket not found')
}

function isLikelyExistingBucketSummary(summary) {
  if (!summary) return false
  if ([301, 307].includes(summary.httpStatusCode)) return true

  const combined = `${summary.name ?? ''} ${summary.code ?? ''} ${summary.message ?? ''}`.toLowerCase()
  return combined.includes('permanentredirect')
    || combined.includes('authorization headermalformed')
}

function toSafeAccountLog(row) {
  return {
    accountId: row.account_id,
    accessKeyIdSuffix: row.access_key_id ? row.access_key_id.slice(-6) : '',
    hasSecretKey: Boolean(row.secret_key),
    endpoint: row.endpoint,
    region: row.region,
    bucket: row.bucket,
    publicBucket: row.public_bucket === 1 || row.public_bucket === true,
    addressingStyle: row.addressing_style ?? 'path',
    payloadSigningMode: row.payload_signing_mode ?? 'unsigned',
    emailOwner: row.email_owner ?? '',
    hasSupabaseAccessToken: Boolean(row.supabase_access_token),
    hasSupabaseAccessTokenExp: Boolean(row.supabase_access_token_exp),
    quotaBytes: row.quota_bytes,
    usedBytes: row.used_bytes,
    active: row.active === 1 || row.active === true,
  }
}

function toIncomingAccountLog(payload) {
  const accessTokenExp = normalizeSupabaseAccessTokenExp(readAccountField(payload, null, [
    'supabaseAccessTokenExp',
    'supabaseAccessTokenExperimental',
    'supabase_access_token_exp',
    'supabase_access_token_experimental',
    'supabase.accessTokenExp',
    'supabase.accessTokenExperimental',
    'supabase.access_token_exp',
    'supabase.accessToken.exp',
    'supabase.accessToken.experimental',
    'supabase.access_token.experimental',
    'supabase.access_token.exp',
  ]))

  return {
    requestedAccountId: normalizeString(payload.accountId ?? payload.account_id),
    endpoint: normalizeString(payload.endpoint),
    region: normalizeString(payload.region),
    bucket: normalizeString(payload.bucket),
    publicBucket: normalizeBoolean(payload.publicBucket ?? payload.public_bucket, false),
    hasAccessKeyId: Boolean(normalizeString(payload.accessKeyId ?? payload.access_key_id)),
    hasSecretAccessKey: Boolean(normalizeString(payload.secretAccessKey ?? payload.secret_key)),
    hasEmailOwner: Boolean(normalizeString(readAccountField(payload, '', ['emailOwner', 'email_owner', 'supabase.emailOwner']))),
    hasSupabaseAccessToken: Boolean(normalizeString(readAccountField(payload, '', [
      'supabaseAccessToken',
      'supabase_access_token',
      'supabase.accessToken',
      'supabase.access_token',
      'supabase.accessToken.value',
    ]))),
    hasSupabaseAccessTokenExp: Boolean(accessTokenExp),
  }
}

function toRtdbAccountDocument(account) {
  return {
    accountId: account.account_id,
    accessKeyId: account.access_key_id,
    secretAccessKey: account.secret_key,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    addressingStyle: account.addressing_style ?? 'path',
    payloadSigningMode: account.payload_signing_mode ?? 'unsigned',
    emailOwner: account.email_owner ?? '',
    supabaseAccessToken: account.supabase_access_token ?? '',
    supabaseAccessTokenExp: account.supabase_access_token_exp ?? null,
    supabaseAccessTokenExperimental: account.supabase_access_token_exp ?? null,
    supabase: {
      accessToken: account.supabase_access_token ?? '',
      accessTokenExp: account.supabase_access_token_exp ?? null,
      accessTokenExperimental: account.supabase_access_token_exp ?? null,
    },
    publicBucket: account.public_bucket === 1,
    quotaBytes: account.quota_bytes,
    usedBytes: account.used_bytes,
    active: account.active === 1,
    addedAt: account.added_at,
  }
}

function toPrivateExportAccount(account) {
  return {
    account_id: account.account_id,
    access_key_id: account.access_key_id,
    secret_key: account.secret_key,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    public_bucket: account.public_bucket === 1 || account.public_bucket === true ? 1 : 0,
    addressing_style: account.addressing_style ?? 'path',
    payload_signing_mode: account.payload_signing_mode ?? 'unsigned',
    email_owner: account.email_owner ?? '',
    supabase_access_token: account.supabase_access_token ?? '',
    supabase_access_token_exp: account.supabase_access_token_exp ?? null,
    quota_bytes: account.quota_bytes ?? 0,
    used_bytes: account.used_bytes ?? 0,
    active: account.active === 1 || account.active === true ? 1 : 0,
    added_at: account.added_at ?? Date.now(),
  }
}

function toRuntimeExportRoute(route) {
  return {
    ...route,
    route_scope: route.route_scope ?? ROUTE_SCOPE.MAIN,
    public_url: route.public_url ?? null,
  }
}

function normalizeAccountPayload(payload, existing = null) {
  const errors = []

  const accountIdInput = normalizeString(payload.accountId ?? payload.account_id ?? existing?.account_id)
  const accountIdValidation = validateAccountIdForRealtime(accountIdInput)
  if (!accountIdValidation.valid) {
    let message = `accountId ${accountIdValidation.reason}`
    const suggestion = suggestAccountId(accountIdInput)
    if (suggestion && suggestion !== accountIdValidation.accountId) {
      message += ` (suggested: ${suggestion})`
    }
    errors.push(message)
  }

  const accessKeyId = normalizeString(payload.accessKeyId ?? payload.access_key_id ?? existing?.access_key_id)
  const secretKey = normalizeString(payload.secretAccessKey ?? payload.secret_key) || existing?.secret_key || ''
  const endpoint = normalizeString(payload.endpoint ?? existing?.endpoint)
  const region = normalizeString(payload.region ?? existing?.region)
  const bucket = normalizeString(payload.bucket ?? existing?.bucket)
  const addressingStyle = normalizeAddressingStyle(payload.addressingStyle ?? payload.addressing_style ?? existing?.addressing_style)
  const payloadSigningMode = normalizePayloadSigningMode(
    payload.payloadSigningMode ?? payload.payload_signing_mode ?? existing?.payload_signing_mode,
  )
  const emailOwnerRaw = readAccountField(payload, existing?.email_owner, [
    'emailOwner',
    'email_owner',
    'supabase.emailOwner',
    'supabase.email_owner',
  ])
  const emailOwner = normalizeString(emailOwnerRaw).toLowerCase()
  const supabaseAccessTokenRaw = readAccountField(payload, existing?.supabase_access_token, [
    'supabaseAccessToken',
    'supabase_access_token',
    'supabase.accessToken',
    'supabase.access_token',
    'supabase.accessToken.value',
  ])
  const supabaseAccessToken = normalizeString(supabaseAccessTokenRaw) || existing?.supabase_access_token || ''
  const supabaseAccessTokenExpInput = readAccountField(
    payload,
    existing?.supabase_access_token_exp,
    [
      'supabaseAccessTokenExp',
      'supabaseAccessTokenExperimental',
      'supabase_access_token_exp',
      'supabase_access_token_experimental',
      'supabase.accessTokenExp',
      'supabase.accessTokenExperimental',
      'supabase.access_token_exp',
      'supabase.accessToken.exp',
      'supabase.accessToken.experimental',
      'supabase.access_token.experimental',
      'supabase.access_token.exp',
    ],
  )
  const supabaseAccessTokenExp = supabaseAccessTokenExpInput === ''
    ? normalizeSupabaseAccessTokenExp(existing?.supabase_access_token_exp)
    : normalizeSupabaseAccessTokenExp(supabaseAccessTokenExpInput)
  const quotaBytes = normalizePositiveInteger(
    payload.quotaBytes ?? payload.quota_bytes,
    existing?.quota_bytes ?? DEFAULT_ADMIN_QUOTA_BYTES,
    'quotaBytes',
    errors,
  )
  const publicBucket = normalizeBoolean(
    payload.publicBucket ?? payload.public_bucket,
    existing ? (existing.public_bucket === 1 || existing.public_bucket === true) : false,
  ) ? 1 : 0
  const usedBytes = normalizeNonNegativeInteger(
    payload.usedBytes ?? payload.used_bytes,
    existing?.used_bytes ?? 0,
    'usedBytes',
    errors,
  )
  const addedAt = normalizeNonNegativeInteger(
    payload.addedAt ?? payload.added_at,
    existing?.added_at ?? Date.now(),
    'addedAt',
    errors,
  )
  const active = normalizeBoolean(payload.active, existing ? (existing.active === 1 || existing.active === true) : true) ? 1 : 0

  if (!accessKeyId) errors.push('accessKeyId is required')
  if (!secretKey) errors.push('secretAccessKey is required')
  if (!endpoint) errors.push('endpoint is required')
  if (!region) errors.push('region is required')
  if (!bucket) errors.push('bucket is required')
  if (!addressingStyle) errors.push('addressingStyle must be one of: path, virtual')
  if (!payloadSigningMode) errors.push('payloadSigningMode must be one of: unsigned, signed')
  if (emailOwner && !isEmailOwner(emailOwner)) errors.push('emailOwner must be a valid email')
  if (supabaseAccessToken && !isSupabaseAccessToken(supabaseAccessToken)) {
    errors.push('supabaseAccessToken must match token format sbp_...')
  }
  if (supabaseAccessTokenExp && !isSupabaseAccessToken(supabaseAccessTokenExp)) {
    errors.push('supabaseAccessTokenExp must match token format sbp_...')
  }

  if (endpoint) {
    try {
      const parsed = new URL(endpoint)
      if (!['http:', 'https:'].includes(parsed.protocol)) {
        errors.push('endpoint must use http or https')
      }
    } catch {
      errors.push('endpoint must be a valid URL')
    }
  }

  if (errors.length > 0) {
    return { errors, row: null, accountId: accountIdValidation.accountId || accountIdInput }
  }

  return {
    errors,
    accountId: accountIdValidation.accountId,
    row: {
      account_id: accountIdValidation.accountId,
      access_key_id: accessKeyId,
      secret_key: secretKey,
      endpoint,
      region,
      bucket,
      addressing_style: addressingStyle,
      payload_signing_mode: payloadSigningMode,
      email_owner: emailOwner,
      supabase_access_token: supabaseAccessToken,
      supabase_access_token_exp: supabaseAccessTokenExp,
      public_bucket: publicBucket,
      quota_bytes: quotaBytes,
      used_bytes: usedBytes,
      active,
      added_at: addedAt,
    },
  }
}

async function verifyBucketExists(accountRow, logger = null) {
  const client = createS3Client(accountRow)
  const attempts = []
  const checks = [
    { operation: 'HeadBucket', command: new HeadBucketCommand({ Bucket: accountRow.bucket }) },
    { operation: 'ListObjectsV2', command: new ListObjectsV2Command({ Bucket: accountRow.bucket, MaxKeys: 1 }) },
  ]

  for (const check of checks) {
    try {
      await client.send(check.command)
      attempts.push({ operation: check.operation, ok: true })
      return {
        exists: true,
        verifiedBy: check.operation,
        attempts,
        detail: `${check.operation} succeeded`,
      }
    } catch (err) {
      const error = summarizeS3Error(err)
      attempts.push({
        operation: check.operation,
        ok: false,
        error,
      })

      logger?.warn({
        accountId: accountRow.account_id,
        bucket: accountRow.bucket,
        operation: check.operation,
        error,
      }, 'admin account bucket verification step failed')

      if (isMissingBucketSummary(error)) {
        return {
          exists: false,
          verifiedBy: null,
          attempts,
          detail: formatS3ErrorSummary(error),
        }
      }

      if (isLikelyExistingBucketSummary(error)) {
        return {
          exists: true,
          verifiedBy: `${check.operation}:${error.name || error.code || error.httpStatusCode || 'redirect'}`,
          attempts,
          detail: formatS3ErrorSummary(error),
        }
      }
    }
  }

  const lastError = attempts.at(-1)?.error ?? null
  return {
    exists: null,
    verifiedBy: null,
    attempts,
    detail: formatS3ErrorSummary(lastError),
  }
}

function readStreamBodyToString(body) {
  if (!body) return Promise.resolve('')
  if (typeof body.transformToString === 'function') {
    return body.transformToString()
  }

  return new Promise((resolve, reject) => {
    const chunks = []
    body.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
    body.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')))
    body.on('error', reject)
  })
}

async function runS3Probe(account) {
  const client = createS3Client(account)
  const probeKey = `${config.ADMIN_TEST_PREFIX}/${account.account_id}-${Date.now()}-${randomBytes(3).toString('hex')}.txt`
  const payload = `s3proxy probe ${new Date().toISOString()}`

  const timings = {}
  const startedAt = Date.now()

  const t1 = Date.now()
  await client.send(new ListObjectsV2Command({ Bucket: account.bucket, MaxKeys: 3 }))
  timings.listMs = Date.now() - t1

  const t2 = Date.now()
  await client.send(new PutObjectCommand({
    Bucket: account.bucket,
    Key: probeKey,
    Body: payload,
    ContentType: 'text/plain; charset=utf-8',
  }))
  timings.putMs = Date.now() - t2

  const t3 = Date.now()
  const getResult = await client.send(new GetObjectCommand({ Bucket: account.bucket, Key: probeKey }))
  const fetchedPayload = await readStreamBodyToString(getResult.Body)
  timings.getMs = Date.now() - t3

  const t4 = Date.now()
  await client.send(new DeleteObjectCommand({ Bucket: account.bucket, Key: probeKey }))
  timings.deleteMs = Date.now() - t4

  return {
    accountId: account.account_id,
    bucket: account.bucket,
    probeKey,
    ok: fetchedPayload === payload,
    bytes: payload.length,
    durationMs: Date.now() - startedAt,
    timings,
  }
}

function parseBodyObject(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return {}
  return body
}

function normalizeQueryString(value) {
  return String(value ?? '').trim()
}

function getPayloadStream(request) {
  if (request.body && typeof request.body.pipe === 'function') {
    return request.body
  }
  if (Buffer.isBuffer(request.body)) {
    return Readable.from(request.body)
  }
  if (request.body instanceof Uint8Array) {
    return Readable.from(Buffer.from(request.body))
  }
  if (typeof request.body === 'string') {
    return Readable.from(Buffer.from(request.body))
  }
  return request.raw
}

async function readRequestBodyBuffer(request, maxBytes = 250 * 1024 * 1024) {
  if (Buffer.isBuffer(request.body)) return request.body
  if (request.body instanceof Uint8Array) return Buffer.from(request.body)
  if (typeof request.body === 'string') return Buffer.from(request.body)

  const source = request.body && typeof request.body[Symbol.asyncIterator] === 'function'
    ? request.body
    : request.raw

  const chunks = []
  let total = 0

  for await (const chunk of source) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    total += buffer.length
    if (total > maxBytes) {
      const err = new Error(`Request body exceeds ${maxBytes} bytes`)
      err.statusCode = 413
      throw err
    }
    chunks.push(buffer)
  }

  return Buffer.concat(chunks)
}

function getManagedFileDirectUrl(route) {
  if (!route) return null
  if ((route.route_scope ?? ROUTE_SCOPE.MAIN) !== ROUTE_SCOPE.PUBLIC) return null
  const account = getAccountById(route.account_id)
  return route.public_url ?? buildDirectPublicObjectUrl(account, route.backend_key)
}

function toAdminTrackedFile(route) {
  return {
    encodedKey: route.encoded_key,
    accountId: route.account_id,
    logicalBucket: route.bucket,
    objectKey: route.object_key,
    backendKey: route.backend_key,
    routeScope: route.route_scope ?? ROUTE_SCOPE.MAIN,
    state: route.state,
    syncState: route.sync_state,
    reconcileStatus: route.reconcile_status,
    contentType: route.content_type ?? 'application/octet-stream',
    sizeBytes: route.size_bytes ?? 0,
    etag: route.etag ?? null,
    uploadedAt: route.uploaded_at ?? null,
    updatedAt: route.updated_at ?? null,
    lastModified: route.last_modified ?? null,
    deletedAt: route.deleted_at ?? null,
    metadataVersion: route.metadata_version ?? 1,
    publicUrl: getManagedFileDirectUrl(route),
  }
}

function isDeletedRoute(route) {
  return !route || route.state === ROUTE_STATE.DELETED || route.deleted_at !== null
}

function matchesManagedFileSearch(route, search) {
  if (!search) return true
  const haystack = [
    route.account_id,
    route.bucket,
    route.object_key,
    route.backend_key,
    route.route_scope,
    route.content_type,
    route.state,
  ].join('\n').toLowerCase()
  return haystack.includes(search.toLowerCase())
}

async function syncRouteAndAccountUsage(routeResult, request) {
  if (!routeResult?.route) return
  syncAccountsFromRows(routeResult.affectedAccounts ?? [])
  refreshMetadataMetrics()

  try {
    await syncRouteToRtdb(routeResult.route)
  } catch (err) {
    request.log.warn({ err, encodedKey: routeResult.route.encoded_key }, 'admin route RTDB sync failed')
  }

  await syncAccountsUsageBatch(routeResult.affectedAccounts ?? [], request.log)
}

function toAdminPublicFile(route) {
  const account = getAccountById(route.account_id)
  const directUrl = route.public_url ?? buildDirectPublicObjectUrl(account, route.backend_key)

  return {
    path: route.object_key,
    encodedKey: route.encoded_key,
    backendKey: route.backend_key,
    bucket: route.bucket ?? PUBLIC_PROXY_BUCKET,
    directUrl,
    accountId: route.account_id,
    contentType: route.content_type ?? 'application/octet-stream',
    sizeBytes: route.size_bytes ?? 0,
    etag: route.etag ?? null,
    uploadedAt: route.uploaded_at ?? null,
    updatedAt: route.updated_at ?? null,
    lastModified: route.last_modified ?? null,
    metadataVersion: route.metadata_version ?? 1,
    routeScope: route.route_scope ?? ROUTE_SCOPE.PUBLIC,
    state: route.state,
  }
}

export default async function adminRoutes(fastify, _opts) {
  try {
    fastify.addContentTypeParser('*', (request, payload, done) => done(null, payload))
  } catch {
    // parser may already exist in current encapsulation context.
  }

  fastify.get('/admin', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('text/html; charset=utf-8').send(adminHtml)
  })

  fastify.get('/admin/icon.svg', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('image/svg+xml; charset=utf-8').send(adminIcon)
  })

  fastify.get('/admin/manifest.webmanifest', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('application/manifest+json').send({
      name: 'S3Proxy Admin',
      short_name: 'S3Proxy',
      description: 'Admin console for S3Proxy accounts, probes and cron jobs',
      start_url: '/admin',
      scope: '/admin/',
      display: 'standalone',
      background_color: '#0b1020',
      theme_color: '#0b1020',
      icons: [{
        src: '/admin/icon.svg',
        sizes: 'any',
        type: 'image/svg+xml',
        purpose: 'any maskable',
      }],
    })
  })

  fastify.get('/admin/sw.js', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    reply.type('application/javascript; charset=utf-8').send(adminServiceWorker)
  })

  fastify.get('/admin/api/overview', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    const stats = getAccountsStats()
    const accounts = getAllAccounts().map(toPublicAccount)
    const rtdb = getRtdbState()

    reply.send({
      status: rtdb.connected ? 'ok' : 'degraded',
      instanceId: config.INSTANCE_ID,
      deployVersion: config.DEPLOY_VERSION,
      stats,
      rtdb,
      jobs: listCronJobs(),
      cronKinds: getCronJobKinds(),
      accounts,
    })
  })

  fastify.get('/admin/api/public-files', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const prefix = normalizeQueryString(request.query?.prefix)
    const files = listPublicRoutes(prefix).map(toAdminPublicFile)
    return reply.send({
      ok: true,
      total: files.length,
      files,
    })
  })

  fastify.get('/admin/api/files', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const accountId = normalizeQueryString(request.query?.accountId)
    const scope = normalizeQueryString(request.query?.scope).toLowerCase()
    const search = normalizeQueryString(request.query?.search)

    let routes = getAllRoutes().filter((route) => !isDeletedRoute(route))

    if (accountId) {
      routes = routes.filter((route) => route.account_id === accountId)
    }

    if ([ROUTE_SCOPE.MAIN, ROUTE_SCOPE.PUBLIC].includes(scope)) {
      routes = routes.filter((route) => (route.route_scope ?? ROUTE_SCOPE.MAIN) === scope)
    }

    if (search) {
      routes = routes.filter((route) => matchesManagedFileSearch(route, search))
    }

    const files = routes.map(toAdminTrackedFile)
    return reply.send({
      ok: true,
      total: files.length,
      files,
    })
  })

  fastify.get('/admin/api/accounts', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    const accounts = getAllAccounts().map(toPublicAccount)
    return reply.send({
      total: accounts.length,
      accounts,
    })
  })

  fastify.post('/admin/api/account-services/preview', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const rawInput = String(payload.rawInput ?? payload.rawText ?? payload.raw ?? '')

    if (!rawInput.trim()) {
      return reply.code(400).send({
        ok: false,
        error: 'rawInput is required',
      })
    }

    const requestedServices = Array.isArray(payload.services)
      ? payload.services.map((item) => normalizeString(item)).filter(Boolean)
      : [normalizeString(payload.service)].filter(Boolean)
    const bucketNameOverride = normalizeString(payload.bucketName ?? payload.preferredBucketName) || undefined

    request.log.info({
      requestedServices: requestedServices.length > 0 ? requestedServices : ['supabaseS3'],
      rawInputLength: rawInput.length,
      lookupRemote: payload.lookupRemote === true,
      createBucketIfMissing: payload.createBucketIfMissing === true,
      bucketNameOverride: bucketNameOverride ?? null,
    }, 'admin account service preview requested')

    const useSupabaseS3 = requestedServices.length === 0 || requestedServices.includes('supabaseS3')
    if (!useSupabaseS3) {
      return reply.code(400).send({
        ok: false,
        error: 'Only service `supabaseS3` is supported in this version',
      })
    }

    const preview = await previewSupabaseS3(rawInput, {
      lookupRemote: payload.lookupRemote === true,
      createBucketIfMissing: payload.createBucketIfMissing === true,
      bucketName: bucketNameOverride,
    })

    request.log.info({
      service: 'supabaseS3',
      missingRequired: preview.missingRequired ?? [],
      warningCount: Array.isArray(preview.warnings) ? preview.warnings.length : 0,
      remote: {
        attempted: preview.remote?.attempted === true,
        ok: preview.remote?.ok === true,
        fallbackToLocal: preview.remote?.fallbackToLocal === true,
        bucketResolved: preview.remote?.bucketResolved ?? null,
        bucketCreated: preview.remote?.bucketCreated === true,
        error: preview.remote?.error ?? null,
      },
    }, 'admin account service preview completed')

    if (Array.isArray(preview.warnings) && preview.warnings.length > 0) {
      request.log.warn({
        service: 'supabaseS3',
        warnings: preview.warnings,
      }, 'admin account service preview has warnings')
    }

    return reply.send({
      ok: true,
      service: 'supabaseS3',
      preview,
    })
  })

  fastify.post('/admin/api/accounts', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const incomingLog = toIncomingAccountLog(payload)
    request.log.info({ incoming: incomingLog }, 'admin account upsert requested')

    const requestedId = normalizeString(payload.accountId ?? payload.account_id)
    const existing = requestedId ? getAccountById(requestedId) : null
    const normalized = normalizeAccountPayload(payload, existing)

    if (normalized.errors.length > 0 || !normalized.row) {
      request.log.warn({
        requestedId: requestedId || null,
        errors: normalized.errors,
      }, 'admin account upsert validation failed')
      return reply.code(400).send({
        ok: false,
        error: 'Invalid account payload',
        errors: normalized.errors,
      })
    }

    const originalRegion = normalized.row.region
    const signingRegion = resolveS3SigningRegion({
      endpoint: normalized.row.endpoint,
      region: originalRegion,
    })
    const regionWasNormalized = signingRegion !== originalRegion
    if (regionWasNormalized) {
      normalized.row.region = signingRegion
      request.log.info({
        accountId: normalized.row.account_id,
        endpoint: normalized.row.endpoint,
        regionInput: originalRegion,
        regionApplied: signingRegion,
      }, 'admin account signing region normalized')
    }

    request.log.info({
      account: toSafeAccountLog(normalized.row),
      existing: Boolean(existing),
    }, 'admin account payload normalized')

    request.log.info({
      accountId: normalized.row.account_id,
      bucket: normalized.row.bucket,
      endpoint: normalized.row.endpoint,
      region: normalized.row.region,
    }, 'admin account bucket verification started')

    const bucketVerification = await verifyBucketExists(normalized.row, request.log)

    if (bucketVerification.exists === false) {
      request.log.warn({
        accountId: normalized.row.account_id,
        bucket: normalized.row.bucket,
        bucketVerification,
      }, 'admin account upsert rejected because bucket was not found')

      return reply.code(400).send({
        ok: false,
        error: `Bucket "${normalized.row.bucket}" does not exist`,
        detail: bucketVerification.detail,
        bucketVerification,
      })
    }

    let bucketWarning = ''
    if (bucketVerification.exists === null) {
      bucketWarning = `Bucket "${normalized.row.bucket}" could not be verified automatically (${bucketVerification.detail}); account is still saved.`
      request.log.warn({
        accountId: normalized.row.account_id,
        bucket: normalized.row.bucket,
        bucketVerification,
      }, 'admin account bucket verification inconclusive')
    } else {
      request.log.info({
        accountId: normalized.row.account_id,
        bucket: normalized.row.bucket,
        verifiedBy: bucketVerification.verifiedBy,
      }, 'admin account bucket verification passed')
    }

    const beforeUpsert = getAccountById(normalized.row.account_id)
    upsertAccount(normalized.row)
    reloadAccountsFromSQLite()

    const updates = {
      [buildRtdbAccountPath(normalized.row.account_id)]: toRtdbAccountDocument(normalized.row),
    }

    let rtdbSynced = true
    const warnings = []
    if (regionWasNormalized) {
      warnings.push(`Region normalized for Supabase S3 signing: ${originalRegion} -> ${signingRegion}`)
    }
    if (bucketWarning) warnings.push(bucketWarning)
    try {
      await rtdbBatchPatch(updates)
      await reloadAccountsFromRTDB()
    } catch (err) {
      rtdbSynced = false
      warnings.push(`Account saved locally, but RTDB sync failed: ${err?.message ?? String(err)}`)
      request.log.warn({ err, accountId: normalized.row.account_id }, 'admin account sync failed')
      reloadAccountsFromSQLite()
    }

    request.log.info({
      accountId: normalized.row.account_id,
      action: beforeUpsert ? 'updated' : 'created',
      rtdbSynced,
      warningCount: warnings.length,
      bucketVerification: {
        exists: bucketVerification.exists,
        verifiedBy: bucketVerification.verifiedBy,
      },
    }, 'admin account upsert completed')

    return reply.send({
      ok: true,
      action: beforeUpsert ? 'updated' : 'created',
      rtdbSynced,
      warning: warnings.length > 0 ? warnings.join(' | ') : undefined,
      bucketVerification,
      account: toPublicAccount(normalized.row),
    })
  })

  fastify.delete('/admin/api/accounts/:accountId', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const accountId = normalizeString(request.params?.accountId)
    request.log.info({ accountId: accountId || null }, 'admin account delete requested')

    if (!accountId) {
      return reply.code(400).send({ ok: false, error: 'accountId is required' })
    }

    const existing = getAccountById(accountId)
    if (!existing) {
      return reply.code(404).send({ ok: false, error: 'account not found' })
    }

    const trackedRoutes = getTrackedRoutesByAccount(accountId)
    if (trackedRoutes.length > 0) {
      return reply.code(409).send({
        ok: false,
        error: `account has ${trackedRoutes.length} tracked route(s), cannot delete`,
      })
    }

    try {
      deleteAccount(accountId)
    } catch (err) {
      const message = err?.message ?? String(err)
      if (message.includes('FOREIGN KEY')) {
        return reply.code(409).send({
          ok: false,
          error: 'account still referenced by object metadata, cannot delete',
        })
      }
      throw err
    }
    reloadAccountsFromSQLite()

    let rtdbSynced = true
    let warning = ''
    try {
      await rtdbBatchPatch({
        [buildRtdbAccountPath(accountId)]: null,
      })
      await reloadAccountsFromRTDB()
    } catch (err) {
      rtdbSynced = false
      warning = `Account deleted locally, but RTDB sync failed: ${err?.message ?? String(err)}`
      request.log.warn({ err, accountId }, 'admin account delete sync failed')
      reloadAccountsFromSQLite()
    }

    request.log.info({
      accountId,
      rtdbSynced,
      warning: warning || null,
    }, 'admin account delete completed')

    return reply.send({
      ok: true,
      accountId,
      rtdbSynced,
      warning: warning || undefined,
    })
  })

  fastify.put('/admin/api/files/:encodedKey', {
    config: { skipAuth: true, rawBody: true },
  }, async (request, reply) => {
    const encodedKey = normalizeQueryString(request.params?.encodedKey)
    if (!encodedKey) {
      return reply.code(400).send({ ok: false, error: 'encodedKey is required' })
    }

    const existing = getAllRoutes().find((route) => route.encoded_key === encodedKey)
    if (!existing || isDeletedRoute(existing)) {
      return reply.code(404).send({ ok: false, error: 'tracked file not found' })
    }

    const account = getAccountById(existing.account_id)
    if (!account) {
      return reply.code(404).send({ ok: false, error: `backend account not found: ${existing.account_id}` })
    }

    let bodyBuffer
    try {
      bodyBuffer = await readRequestBodyBuffer(request)
    } catch (err) {
      const statusCode = Number(err?.statusCode) || 400
      return reply.code(statusCode).send({ ok: false, error: err?.message ?? String(err) })
    }

    if (!bodyBuffer || bodyBuffer.length === 0) {
      return reply.code(400).send({ ok: false, error: 'request body is empty' })
    }

    const contentType = normalizeQueryString(request.headers['x-file-content-type'])
      || normalizeQueryString(request.headers['content-type'])
      || existing.content_type
      || 'application/octet-stream'

    const client = createS3Client(account)
    let putResult
    try {
      putResult = await client.send(new PutObjectCommand({
        Bucket: account.bucket,
        Key: existing.backend_key,
        Body: bodyBuffer,
        ContentType: contentType,
      }))
    } catch (err) {
      return reply.code(502).send({ ok: false, error: err?.message ?? String(err) })
    }

    const now = Date.now()
    const updated = commitUploadedObjectMetadata({
      encoded_key: existing.encoded_key,
      account_id: existing.account_id,
      bucket: existing.bucket,
      object_key: existing.object_key,
      backend_key: existing.backend_key,
      size_bytes: bodyBuffer.length,
      etag: putResult?.ETag ? String(putResult.ETag).replace(/"/g, '') : existing.etag,
      last_modified: now,
      content_type: contentType,
      uploaded_at: existing.uploaded_at ?? now,
      updated_at: now,
      public_url: existing.public_url ?? null,
      route_scope: existing.route_scope ?? ROUTE_SCOPE.MAIN,
      instance_id: config.INSTANCE_ID,
    })

    await syncRouteAndAccountUsage(updated, request)

    return reply.send({
      ok: true,
      message: 'File replaced successfully',
      file: toAdminTrackedFile(updated.route),
    })
  })

  fastify.delete('/admin/api/files/:encodedKey', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const encodedKey = normalizeQueryString(request.params?.encodedKey)
    if (!encodedKey) {
      return reply.code(400).send({ ok: false, error: 'encodedKey is required' })
    }

    const existing = getAllRoutes().find((route) => route.encoded_key === encodedKey)
    if (!existing || isDeletedRoute(existing)) {
      return reply.code(404).send({ ok: false, error: 'tracked file not found' })
    }

    const account = getAccountById(existing.account_id)
    if (!account) {
      return reply.code(404).send({ ok: false, error: `backend account not found: ${existing.account_id}` })
    }

    const client = createS3Client(account)
    try {
      await client.send(new DeleteObjectCommand({
        Bucket: account.bucket,
        Key: existing.backend_key,
      }))
    } catch (err) {
      const message = err?.message ?? String(err)
      const statusCode = Number(err?.$metadata?.httpStatusCode) || 502
      const lower = String(message).toLowerCase()
      const missing = statusCode === 404 || lower.includes('nosuchkey') || lower.includes('not found')
      if (!missing) {
        return reply.code(502).send({ ok: false, error: message })
      }
    }

    const deleted = finalizeRouteDelete(encodedKey, Date.now())
    await syncRouteAndAccountUsage(deleted, request)

    return reply.send({
      ok: true,
      message: 'File deleted successfully',
      file: deleted.route ? toAdminTrackedFile(deleted.route) : null,
    })
  })

  fastify.get('/admin/api/runtime-export', {
    config: { skipAuth: true },
  }, async (_request, reply) => {
    const snapshot = {
      version: 1,
      exportedAt: Date.now(),
      instanceId: config.INSTANCE_ID,
      accounts: getAllAccounts().map(toPrivateExportAccount),
      buckets: getAllBuckets(),
      routes: getAllRoutes().map(toRuntimeExportRoute),
    }

    reply
      .header('Content-Type', 'application/json; charset=utf-8')
      .header('Content-Disposition', `attachment; filename="s3proxy-runtime-export-${Date.now()}.json"`)
      .send(snapshot)
  })

  fastify.post('/admin/api/runtime-import', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const accountsInput = Array.isArray(payload.accounts)
      ? payload.accounts
      : Object.values(payload.accounts ?? {})
    const bucketsInput = Array.isArray(payload.buckets)
      ? payload.buckets
      : Object.values(payload.buckets ?? {})
    const routesInput = Array.isArray(payload.routes)
      ? payload.routes
      : Object.values(payload.routes ?? {})

    if (accountsInput.length === 0 && bucketsInput.length === 0 && routesInput.length === 0) {
      return reply.code(400).send({
        ok: false,
        error: 'Payload must contain at least one of: accounts, buckets, routes',
      })
    }

    const importedAccounts = []
    for (const account of accountsInput) {
      if (!account || typeof account !== 'object') continue
      const normalized = toPrivateExportAccount({
        ...account,
        account_id: account.account_id ?? account.accountId,
        access_key_id: account.access_key_id ?? account.accessKeyId,
        secret_key: account.secret_key ?? account.secretAccessKey,
        addressing_style: account.addressing_style ?? account.addressingStyle,
        payload_signing_mode: account.payload_signing_mode ?? account.payloadSigningMode,
        email_owner: account.email_owner ?? account.emailOwner,
        supabase_access_token: account.supabase_access_token ?? account.supabaseAccessToken,
        supabase_access_token_exp: account.supabase_access_token_exp
          ?? account.supabaseAccessTokenExp
          ?? account.supabaseAccessTokenExperimental,
        public_bucket: account.public_bucket ?? account.publicBucket,
        quota_bytes: account.quota_bytes ?? account.quotaBytes,
        used_bytes: account.used_bytes ?? account.usedBytes,
        added_at: account.added_at ?? account.addedAt,
      })
      if (!normalized.account_id) continue
      upsertAccount(normalized)
      importedAccounts.push(normalized)
    }

    const importedBuckets = []
    for (const bucket of bucketsInput) {
      if (!bucket || typeof bucket !== 'object' || !bucket.bucket) continue
      importedBuckets.push(upsertBucketRecord({
        bucket: bucket.bucket,
        created_at: bucket.created_at ?? bucket.createdAt,
        updated_at: bucket.updated_at ?? bucket.updatedAt,
        deleted_at: bucket.deleted_at ?? bucket.deletedAt ?? null,
        versioning_status: bucket.versioning_status ?? bucket.versioningStatus ?? '',
      }))
    }

    const importedRoutes = []
    for (const route of routesInput) {
      if (!route || typeof route !== 'object') continue
      const normalized = {
        ...route,
        encoded_key: route.encoded_key
          ?? route.encodedKey
          ?? (route.bucket && route.object_key ? encodeKey(route.bucket, route.object_key) : null),
        account_id: route.account_id ?? route.accountId,
        object_key: route.object_key ?? route.objectKey,
        backend_key: route.backend_key ?? route.backendKey,
        size_bytes: route.size_bytes ?? route.sizeBytes ?? 0,
        last_modified: route.last_modified ?? route.lastModified ?? route.uploaded_at ?? route.uploadedAt ?? Date.now(),
        content_type: route.content_type ?? route.contentType ?? null,
        uploaded_at: route.uploaded_at ?? route.uploadedAt ?? Date.now(),
        updated_at: route.updated_at ?? route.updatedAt ?? Date.now(),
        deleted_at: route.deleted_at ?? route.deletedAt ?? null,
        metadata_version: route.metadata_version ?? route.metadataVersion ?? 1,
        route_scope: route.route_scope ?? route.routeScope ?? ROUTE_SCOPE.MAIN,
        public_url: route.public_url ?? route.publicUrl ?? null,
        state: route.state ?? ROUTE_STATE.ACTIVE,
        sync_state: route.sync_state ?? route.syncState ?? 'PENDING_SYNC',
        reconcile_status: route.reconcile_status ?? route.reconcileStatus ?? ROUTE_RECONCILE_STATUS.HEALTHY,
        backend_last_seen_at: route.backend_last_seen_at ?? route.backendLastSeenAt ?? null,
        backend_missing_since: route.backend_missing_since ?? route.backendMissingSince ?? null,
        last_reconciled_at: route.last_reconciled_at ?? route.lastReconciledAt ?? null,
        instance_id: route.instance_id ?? route.instanceId ?? config.INSTANCE_ID,
      }

      if (!normalized.encoded_key || !normalized.account_id || !normalized.bucket || !normalized.object_key) {
        continue
      }

      upsertRoute(normalized)
      importedRoutes.push(normalized)
    }

    reloadAccountsFromSQLite()

    let rtdbSynced = true
    let warning = ''
    try {
      const updates = {
        ...Object.fromEntries(importedAccounts.map((account) => [buildRtdbAccountPath(account.account_id), toRtdbAccountDocument(account)])),
        ...Object.fromEntries(importedRoutes.map((route) => [`/routes/${route.encoded_key}`, buildRtdbRouteDocument(route)])),
      }

      if (Object.keys(updates).length > 0) {
        await rtdbBatchPatch(updates)
        await reloadAccountsFromRTDB()
      }
    } catch (err) {
      rtdbSynced = false
      warning = `Runtime metadata imported locally, but RTDB sync failed: ${err?.message ?? String(err)}`
      request.log.warn({ err }, 'runtime import RTDB sync failed')
      reloadAccountsFromSQLite()
    }

    return reply.send({
      ok: true,
      imported: {
        accounts: importedAccounts.length,
        buckets: importedBuckets.length,
        routes: importedRoutes.length,
        publicFiles: importedRoutes.filter((route) => route.route_scope === ROUTE_SCOPE.PUBLIC).length,
      },
      rtdbSynced,
      warning: warning || undefined,
    })
  })

  fastify.post('/admin/api/cron-jobs', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const saved = saveCronJob(parseBodyObject(request.body))
      return reply.send({ ok: true, job: saved })
    } catch (err) {
      return reply.code(400).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.post('/admin/api/cron-jobs/:jobId/run', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const result = await runCronJobNow(request.params.jobId)
      return reply.send({
        ok: result.lastRunStatus === 'ok',
        jobId: result.job_id,
        lastRunStatus: result.lastRunStatus,
        lastRunError: result.lastRunError,
        report: result.lastRunReport ?? null,
      })
    } catch (err) {
      return reply.code(404).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.delete('/admin/api/cron-jobs/:jobId', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    try {
      const removed = removeCronJob(request.params.jobId)
      if (!removed) {
        return reply.code(404).send({ ok: false, error: 'job not found' })
      }
      return reply.send({ ok: true })
    } catch (err) {
      return reply.code(400).send({ ok: false, error: err?.message ?? String(err) })
    }
  })

  fastify.post('/admin/api/test-s3', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const payload = parseBodyObject(request.body)
    const all = getAllAccounts().filter((item) => item.active === 1 || item.active === true)

    let targets = []
    if (payload.allActive === true) {
      targets = all
    } else if (payload.accountId) {
      targets = all.filter((item) => item.account_id === String(payload.accountId))
    }

    if (targets.length === 0) {
      return reply.code(400).send({ ok: false, error: 'account not found or inactive' })
    }

    const results = []
    for (const account of targets) {
      try {
        const result = await runS3Probe(account)
        results.push(result)
      } catch (err) {
        results.push({
          accountId: account.account_id,
          bucket: account.bucket,
          ok: false,
          error: err?.message ?? String(err),
        })
      }
    }

    return reply.send({
      ok: results.every((item) => item.ok),
      count: results.length,
      results,
    })
  })
}
