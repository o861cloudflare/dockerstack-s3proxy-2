/**
 * src/cronScheduler.js
 * Runtime cron scheduler with persisted jobs + extensible job kinds.
 */

import { randomUUID } from 'crypto'
import { ListObjectsV2Command, PutObjectCommand } from '@aws-sdk/client-s3'

import config from './config.js'
import { createS3Client } from './inventoryScanner.js'
import {
  deleteCronJob,
  getAllActiveAccounts,
  getAllCronJobs,
  getCronJobById,
  upsertCronJob,
} from './db.js'

const jobs = new Map()
let schedulerTimer = null
let activeLogger = console

const JOB_KIND = Object.freeze({
  KEEPALIVE_SCAN: 'keepalive_scan',
  KEEPALIVE_TOUCH: 'keepalive_touch',
  PROBE_ACTIVE_ACCOUNTS: 'probe_active_accounts',
})

function parseCronField(field, min, max) {
  const normalized = String(field).trim()
  if (normalized === '*') return { any: true, values: null }
  if (normalized.startsWith('*/')) {
    const step = Number.parseInt(normalized.slice(2), 10)
    if (!Number.isFinite(step) || step <= 0) return null
    return { any: false, step, values: null }
  }

  const values = new Set()
  for (const chunk of normalized.split(',')) {
    const value = Number.parseInt(chunk.trim(), 10)
    if (!Number.isFinite(value) || value < min || value > max) return null
    values.add(value)
  }
  return { any: false, step: null, values }
}

function parseCronExpression(expression) {
  const fields = String(expression).trim().split(/\s+/)
  if (fields.length !== 5) return null

  const [minuteRaw, hourRaw, dayRaw, monthRaw, weekdayRaw] = fields
  const minute = parseCronField(minuteRaw, 0, 59)
  const hour = parseCronField(hourRaw, 0, 23)
  const day = parseCronField(dayRaw, 1, 31)
  const month = parseCronField(monthRaw, 1, 12)
  const weekday = parseCronField(weekdayRaw, 0, 6)

  if (!minute || !hour || !day || !month || !weekday) return null
  return { minute, hour, day, month, weekday }
}

function matchField(rule, value) {
  if (!rule) return false
  if (rule.any) return true
  if (rule.step) return value % rule.step === 0
  return rule.values?.has(value) ?? false
}

function getDateInTimezone(date, timezone = 'UTC') {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: timezone,
    hour12: false,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    weekday: 'short',
  })

  const parts = formatter.formatToParts(date)
  const read = (type) => parts.find((item) => item.type === type)?.value
  const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 }

  return {
    year: Number.parseInt(read('year') || '0', 10),
    month: Number.parseInt(read('month') || '0', 10),
    day: Number.parseInt(read('day') || '0', 10),
    hour: Number.parseInt(read('hour') || '0', 10),
    minute: Number.parseInt(read('minute') || '0', 10),
    weekday: weekdayMap[read('weekday')] ?? 0,
  }
}

function shouldRun(descriptor, date = new Date()) {
  const rule = descriptor.parsedExpression
  if (!rule) return false

  const zoned = getDateInTimezone(date, descriptor.timezone)
  return matchField(rule.minute, zoned.minute)
    && matchField(rule.hour, zoned.hour)
    && matchField(rule.day, zoned.day)
    && matchField(rule.month, zoned.month)
    && matchField(rule.weekday, zoned.weekday)
}

function parsePayload(payloadJson = '{}') {
  try {
    const parsed = JSON.parse(payloadJson)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {}
    return parsed
  } catch {
    return {}
  }
}

function sanitizeJobInput(payload = {}, existing = null) {
  const kind = String(payload.kind ?? existing?.kind ?? '').trim()
  if (!Object.values(JOB_KIND).includes(kind)) {
    throw new Error(`Unsupported cron kind: ${kind}`)
  }

  const expression = String(payload.expression ?? existing?.expression ?? '').trim()
  const parsedExpression = parseCronExpression(expression)
  if (!parsedExpression) {
    throw new Error(`Invalid cron expression: ${expression}`)
  }

  const timezone = String(payload.timezone ?? existing?.timezone ?? config.CRON_TIMEZONE).trim() || 'UTC'
  const enabled = payload.enabled === undefined
    ? (existing?.enabled === 1 || existing?.enabled === true)
    : Boolean(payload.enabled)

  const name = String(payload.name ?? existing?.name ?? `${kind}-${randomUUID().slice(0, 8)}`).trim()
  if (!name) {
    throw new Error('Cron name is required')
  }

  const payloadObject = payload.payload && typeof payload.payload === 'object' && !Array.isArray(payload.payload)
    ? payload.payload
    : parsePayload(existing?.payload_json)

  return {
    job_id: String(payload.jobId ?? existing?.job_id ?? randomUUID()),
    name,
    kind,
    expression,
    timezone,
    enabled,
    payload_json: JSON.stringify(payloadObject),
    parsedExpression,
    source: String(payload.source ?? existing?.source ?? 'user'),
  }
}

async function runKeepaliveScan(account, jobPayload = {}) {
  const client = createS3Client(account)
  const maxKeys = Number(jobPayload.maxKeys ?? 1)
  const prefix = String(jobPayload.prefix ?? config.CRON_KEEPALIVE_PREFIX)

  await client.send(new ListObjectsV2Command({
    Bucket: account.bucket,
    MaxKeys: maxKeys,
    Prefix: prefix,
  }))

  return {
    operation: 'ListObjectsV2',
    maxKeys,
    prefix,
  }
}

async function runKeepaliveTouch(account, jobPayload = {}) {
  const client = createS3Client(account)
  const prefix = String(jobPayload.prefix ?? config.CRON_KEEPALIVE_PREFIX).replace(/\/$/, '')
  const key = `${prefix}/${account.account_id}.txt`
  const bodyPrefix = String(jobPayload.contentPrefix ?? config.CRON_KEEPALIVE_CONTENT_PREFIX)

  await client.send(new PutObjectCommand({
    Bucket: account.bucket,
    Key: key,
    Body: `${bodyPrefix} ${new Date().toISOString()}\n`,
    ContentType: 'text/plain; charset=utf-8',
  }))

  return {
    operation: 'PutObject',
    key,
    prefix,
  }
}

async function runProbeActiveAccounts(account, jobPayload = {}) {
  const client = createS3Client(account)
  const prefix = String(jobPayload.prefix ?? config.ADMIN_TEST_PREFIX).replace(/\/$/, '')
  const key = `${prefix}/${account.account_id}-${Date.now()}.txt`
  const payload = `probe ${new Date().toISOString()}`

  await client.send(new PutObjectCommand({
    Bucket: account.bucket,
    Key: key,
    Body: payload,
    ContentType: 'text/plain; charset=utf-8',
  }))
  await client.send(new ListObjectsV2Command({ Bucket: account.bucket, Prefix: prefix, MaxKeys: 2 }))

  return {
    operation: 'PutObject+ListObjectsV2',
    key,
    prefix,
    bytes: payload.length,
  }
}

function toErrorMessage(err) {
  return err?.message ?? String(err)
}

async function executeJobByKind(descriptor) {
  const payload = parsePayload(descriptor.payload_json)
  const accounts = getAllActiveAccounts()
  const startedAt = Date.now()
  const report = {
    jobId: descriptor.job_id,
    name: descriptor.name,
    kind: descriptor.kind,
    source: descriptor.source,
    timezone: descriptor.timezone,
    payload,
    startedAt,
    finishedAt: null,
    durationMs: 0,
    skipped: false,
    ok: true,
    targetCount: accounts.length,
    successCount: 0,
    errorCount: 0,
    message: '',
    accountResults: [],
  }

  if (accounts.length === 0) {
    report.skipped = true
    report.message = 'No active account available for this cron job.'
    report.finishedAt = Date.now()
    report.durationMs = report.finishedAt - startedAt
    activeLogger.warn?.({ job: descriptor.job_id }, 'cron skipped because no active account')
    return report
  }

  for (const account of accounts) {
    const itemStartedAt = Date.now()
    const item = {
      accountId: account.account_id,
      bucket: account.bucket,
      endpoint: account.endpoint,
      startedAt: itemStartedAt,
      finishedAt: null,
      durationMs: 0,
      ok: false,
      detail: null,
      error: null,
    }

    try {
      if (descriptor.kind === JOB_KIND.KEEPALIVE_TOUCH) {
        item.detail = await runKeepaliveTouch(account, payload)
      } else if (descriptor.kind === JOB_KIND.PROBE_ACTIVE_ACCOUNTS) {
        item.detail = await runProbeActiveAccounts(account, payload)
      } else {
        item.detail = await runKeepaliveScan(account, payload)
      }
      item.ok = true
      report.successCount += 1
    } catch (err) {
      item.error = toErrorMessage(err)
      report.errorCount += 1
      report.ok = false
      activeLogger.warn?.({ err, job: descriptor.job_id, accountId: account.account_id }, 'cron job account execution failed')
    } finally {
      item.finishedAt = Date.now()
      item.durationMs = item.finishedAt - itemStartedAt
      report.accountResults.push(item)
    }
  }

  report.finishedAt = Date.now()
  report.durationMs = report.finishedAt - startedAt
  report.message = report.ok
    ? `Completed ${report.successCount}/${report.targetCount} account(s) successfully.`
    : `Completed with ${report.errorCount} error(s) across ${report.targetCount} account(s).`

  return report
}

function toRuntimeDescriptor(row) {
  const parsedExpression = parseCronExpression(row.expression)
  if (!parsedExpression) return null

  return {
    ...row,
    enabled: row.enabled === 1 || row.enabled === true,
    parsedExpression,
    lastRunAt: null,
    lastRunStatus: null,
    lastRunError: null,
    lastDurationMs: null,
    lastTriggerMinute: null,
    lastRunReport: null,
  }
}

function upsertRuntimeDescriptor(row) {
  const descriptor = toRuntimeDescriptor(row)
  if (!descriptor) {
    throw new Error(`Invalid expression in DB for ${row.job_id}`)
  }
  const previous = jobs.get(row.job_id)
  if (previous) {
    descriptor.lastRunAt = previous.lastRunAt
    descriptor.lastRunStatus = previous.lastRunStatus
    descriptor.lastRunError = previous.lastRunError
    descriptor.lastDurationMs = previous.lastDurationMs
    descriptor.lastTriggerMinute = previous.lastTriggerMinute
    descriptor.lastRunReport = previous.lastRunReport
  }
  jobs.set(row.job_id, descriptor)
  return descriptor
}

async function runDescriptor(descriptor) {
  const startedAt = Date.now()
  descriptor.lastRunAt = startedAt

  const report = await executeJobByKind(descriptor)
  descriptor.lastRunReport = report
  descriptor.lastRunStatus = report.ok ? 'ok' : 'error'
  descriptor.lastRunError = report.ok
    ? null
    : (report.accountResults.find((item) => !item.ok)?.error || report.message)
  descriptor.lastDurationMs = Date.now() - startedAt

  return report
}

function ensureDefaultJobs() {
  if (!config.CRON_KEEPALIVE_ENABLED) return

  const defaults = getAllCronJobs().filter((job) => job.source === 'system')
  const hasKeepalive = defaults.some((job) => job.job_id === 'system.supabase-keepalive')
  if (hasKeepalive) return

  const defaultKind = String(config.CRON_KEEPALIVE_MODE).trim().toLowerCase() === 'touch'
    ? JOB_KIND.KEEPALIVE_TOUCH
    : JOB_KIND.KEEPALIVE_SCAN

  upsertCronJob({
    job_id: 'system.supabase-keepalive',
    name: 'Supabase Keepalive',
    kind: defaultKind,
    expression: config.CRON_KEEPALIVE_EXPRESSION,
    timezone: config.CRON_TIMEZONE,
    enabled: true,
    source: 'system',
    payload_json: {
      prefix: config.CRON_KEEPALIVE_PREFIX,
      contentPrefix: config.CRON_KEEPALIVE_CONTENT_PREFIX,
      maxKeys: 1,
    },
  })
}

function rebuildRuntimeJobs() {
  jobs.clear()
  for (const row of getAllCronJobs()) {
    try {
      upsertRuntimeDescriptor(row)
    } catch (err) {
      activeLogger.error?.({ err, jobId: row.job_id }, 'skip invalid cron row')
    }
  }
}

function schedulerTick() {
  const now = new Date()

  for (const descriptor of jobs.values()) {
    if (!descriptor.enabled) continue
    if (!shouldRun(descriptor, now)) continue

    const minuteKey = `${descriptor.timezone}:${now.toISOString().slice(0, 16)}`
    if (descriptor.lastTriggerMinute === minuteKey) continue
    descriptor.lastTriggerMinute = minuteKey

    runDescriptor(descriptor).then((report) => {
      if (!report.ok) {
        activeLogger.error?.({ job: descriptor.job_id, report }, 'cron job finished with errors')
      }
    }).catch((err) => {
      activeLogger.error?.({ err, job: descriptor.job_id }, 'cron job failed unexpectedly')
    })
  }
}

export function listCronJobs() {
  return [...jobs.values()]
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((job) => ({
      jobId: job.job_id,
      name: job.name,
      kind: job.kind,
      expression: job.expression,
      timezone: job.timezone,
      enabled: job.enabled,
      source: job.source,
      payload: parsePayload(job.payload_json),
      lastRunAt: job.lastRunAt,
      lastRunStatus: job.lastRunStatus,
      lastRunError: job.lastRunError,
      lastDurationMs: job.lastDurationMs,
      lastRunReport: job.lastRunReport,
    }))
}

export function getCronJobKinds() {
  return Object.values(JOB_KIND)
}

export function saveCronJob(payload) {
  const existing = payload.jobId ? getCronJobById(payload.jobId) : null
  const normalized = sanitizeJobInput(payload, existing)
  const saved = upsertCronJob(normalized)
  const descriptor = upsertRuntimeDescriptor(saved)

  return {
    jobId: descriptor.job_id,
    name: descriptor.name,
    kind: descriptor.kind,
    expression: descriptor.expression,
    timezone: descriptor.timezone,
    enabled: descriptor.enabled,
    source: descriptor.source,
    payload: parsePayload(descriptor.payload_json),
  }
}

export function removeCronJob(jobId) {
  const current = getCronJobById(jobId)
  if (!current) return false
  if (current.source === 'system') {
    throw new Error('System cron job cannot be deleted')
  }

  deleteCronJob(jobId)
  jobs.delete(jobId)
  return true
}

export async function runCronJobNow(jobId) {
  const descriptor = jobs.get(jobId)
  if (!descriptor) {
    throw new Error(`Cron job not found: ${jobId}`)
  }
  const report = await runDescriptor(descriptor)
  return {
    ...descriptor,
    lastRunReport: report,
  }
}

export async function startCronScheduler(logger = console) {
  activeLogger = logger

  if (!config.CRON_ENABLED) {
    logger.info?.('cron scheduler disabled by CRON_ENABLED=false')
    return
  }

  ensureDefaultJobs()
  rebuildRuntimeJobs()

  if (config.CRON_RUN_ON_START) {
    for (const descriptor of jobs.values()) {
      if (!descriptor.enabled || descriptor.source !== 'system') continue
      runDescriptor(descriptor).then((report) => {
        if (!report.ok) {
          logger.error?.({ job: descriptor.job_id, report }, 'initial cron run finished with errors')
        }
      }).catch((err) => {
        logger.error?.({ err, job: descriptor.job_id }, 'initial cron run failed')
      })
    }
  }

  if (schedulerTimer) clearInterval(schedulerTimer)
  schedulerTimer = setInterval(schedulerTick, 10_000)
  schedulerTimer.unref?.()

  logger.info?.({ jobs: listCronJobs() }, 'cron scheduler started')
}

export function stopCronScheduler() {
  if (schedulerTimer) clearInterval(schedulerTimer)
  schedulerTimer = null
  jobs.clear()
}
