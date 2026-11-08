/** Local file Source: task files dropped into the inbox directory become Triggers; replies are written beside them as answer files. */
import { setTimeout as delay } from 'node:timers/promises'
import { mkdir, readdir, readFile, rename, stat, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import { z } from 'zod'
import { key, threadKey, type OutboundMessage, type Trigger } from '../core/index.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { SourceKind } from './kind.ts'

const ConfigSchema = z.object({
  pollSeconds: z.number().int().min(5).max(3600).default(15),
}).strict()
export type LocalConfig = z.infer<typeof ConfigSchema>

/** The inbox lives under the state directory so the Channel can find it from `commands.deployment` alone. */
const inboxDir = (stateDir: string): string => join(stateDir, 'inbox')
/** A task file's stem names its Thread, so resubmitting the same file name continues the conversation. */
const fileStem = (name: string): string => name.replace(/\.(md|txt)$/i, '')

/**
 * The Trigger of one task file. The operator is the only sender, and a task file is a direct request, so every
 * Trigger is `addressed: 'direct'`. The id carries the file's mtime and size, so an interrupted round (received but
 * not yet archived) replays into a duplicate instead of a second Ticket.
 */
export function localTrigger(name: string, text: string, at: string, revision: string): Trigger {
  const stem = fileStem(name)
  return {
    id: key('local', `${stem}.${revision}`),
    source: 'local',
    threads: [{ source: 'local', id: stem }],
    actor: { source: 'local', id: 'operator' },
    human: true,
    addressed: 'direct',
    at, text,
    channel: { source: 'local', thread: `local:${stem}`, address: { file: name } },
    meta: { file: name, root: stem },
  }
}

/** Reads every task file in the inbox once, takes each in, and archives the file so the next round sees only new tasks. */
export async function takeTasks(ctx: SourceContext, _config: LocalConfig, signal: AbortSignal): Promise<number> {
  const dir = inboxDir(ctx.deployment.stateDir)
  const done = join(dir, 'processed')
  await mkdir(done, { recursive: true })
  const entries = (await readdir(dir, { withFileTypes: true }))
    .filter(entry => entry.isFile() && /\.(md|txt)$/i.test(entry.name))
  let taken = 0
  for (const entry of entries) {
    if (signal.aborted) throw new Error('The local Source stopped during the read')
    const path = join(dir, entry.name)
    const size = await stat(path)
    const text = await readFile(path, 'utf8')
    const result = await ctx.receive(localTrigger(entry.name, text, size.mtime.toISOString(), `${size.mtimeMs}.${size.size}`))
    if (result !== 'filtered') ctx.threads.describe(threadKey({ source: 'local', id: fileStem(entry.name) }), { label: fileStem(entry.name) })
    await rename(path, join(done, `${Date.now()}-${entry.name}`)).catch(() => {})
    taken++
  }
  return taken
}

/** Writes one answer file per message id under `<inbox>/answers`; an existing file means the reply was already delivered. */
export async function sendLocal(host: Pick<SourceContext, 'commands'>, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const name = `${message.id.replace(/[^A-Za-z0-9._-]/g, '_')}.md`
  const file = join(inboxDir(host.commands.deployment.stateDir), 'answers', name)
  try {
    await mkdir(join(inboxDir(host.commands.deployment.stateDir), 'answers'), { recursive: true })
    await writeFile(file, message.text, { encoding: 'utf8', flag: 'wx' })
    return 'sent'
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') return 'sent'
    throw error
  }
}

export const localKind: SourceKind<LocalConfig> = {
  kind: 'local',
  title: { zh: '本地任务', en: 'Local tasks' },
  description: { zh: '收件目录里的任务文件：把 .md 或 .txt 任务文件放进 stateDir/inbox 即可派活，回复写在 stateDir/inbox/answers。',
    en: 'Task files in an inbox directory: drop a .md or .txt task into stateDir/inbox; replies are written to stateDir/inbox/answers.' },
  schema: ConfigSchema,
  fields: [
    { key: 'pollSeconds', type: 'number', advanced: true, min: 5, max: 3600, label: { zh: '轮询间隔 / 秒', en: 'Polling interval / seconds' } },
  ],
  health: ['local'],
  channel: {
    send: sendLocal,
    link: address => ({ url: '', label: { zh: `任务文件 ${String(address.file ?? '')}`, en: `Task file ${String(address.file ?? '')}` } }),
  },
  threadContext: {
    messageId: trigger => String(trigger.meta.file ?? trigger.id),
    read: async (_commands, trigger, _part, _cursor) => ({
      messages: [{ id: String(trigger.meta.file ?? trigger.id), from: 'operator', at: trigger.at, text: trigger.text }],
      body: { task: trigger.text },
    }),
  },
  policy: {
    schema: z.object({ allowed: z.array(z.object({ id: z.string().min(1), name: z.string().trim().max(100).optional() }).strict()).default([]) }).strict(),
    fields: [{ key: 'allowed', type: 'rows', label: { zh: '能发起任务的发送者', en: 'Senders who may originate work' }, columns: [
      { key: 'id', type: 'text', label: { zh: '发送者', en: 'Sender' } },
      { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] }],
    explicit: { zh: '把任务文件放进收件箱目录（stateDir/inbox）。', en: 'Drop a task file into the inbox directory (stateDir/inbox).' },
  },
  environment: {
    variables: [{ key: 'DSH_SUPERAGENT_LOCAL_FILE', description: {
      zh: '发起这个工单的任务文件名；工单不是从本地来源来的时为空',
      en: 'The task file that started the ticket; empty when the ticket did not start on this Source' } }],
    values: ({ trigger }) => ({
      DSH_SUPERAGENT_LOCAL_FILE: trigger?.source === 'local' ? String(trigger.meta.file ?? '') : '' }),
  },
}

async function watch(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the accounts; until then the Source reads nothing.
  if (!ctx.configured()) { ctx.report('local', health('paused', 'setupPending')); return }
  let failures = 0
  while (!signal.aborted) {
    const config = ctx.config<LocalConfig>('local')
    if (!config) ctx.report('local', health('paused', 'sourceOff'))
    else {
      const at = new Date().toISOString()
      try {
        const taken = await takeTasks(ctx, config, signal)
        failures = 0
        ctx.report('local', taken ? { state: 'ready', detail: `took ${taken} task file(s)`, at }
          : { state: 'ready', detail: 'waiting for task files in the inbox', at })
      } catch (error) {
        failures += 1
        // One failed round is retried on the next round; three in a row need a person.
        if (!signal.aborted && failures >= 3) {
          ctx.report('local', health('error', 'pollFailures', { count: String(failures), error: String(error) }))
        }
      }
    }
    await delay((config?.pollSeconds ?? 15) * 1000, undefined, { signal }).catch(() => {})
  }
}

export const name = 'dsh-superagent-local'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(localKind), 'local: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, localKind)
    const controller = new AbortController()
    // DSH ends its process on an unhandled rejection, so a failure that escapes the loop becomes the Source's health.
    const running = watch(source, controller.signal)
      .catch((error: unknown) => { source.report('local', { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  })
}
