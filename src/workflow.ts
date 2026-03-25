import { WorkflowEntrypoint } from 'cloudflare:workers'
import type { WorkflowEvent, WorkflowStep } from 'cloudflare:workers'

// Optional — pass { prefix: 'open_pms' } to restrict the purge to keys
// matching a specific kvPrefix, rather than clearing the whole namespace.
type Params = {
  prefix?: string
}

export class CachePurgeWorkflow extends WorkflowEntrypoint<Env, Params> {
  async run(event: WorkflowEvent<Params>, step: WorkflowStep) {
    const prefix = event.payload?.prefix

    // Step 1 — list every key in the namespace.
    // KV list() is paginated (max 1000 keys per call), so we loop until
    // list_complete is true. Keeping this in its own step means a restart
    // won't re-run a partially completed delete — it picks up from here.
    const allKeys: string[] = await step.do(
      'list all KV keys',
      async () => {
        const keys: string[] = []
        let cursor: string | undefined

        do {
          const page = await this.env.KV.list({
            ...(prefix ? { prefix } : {}),
            ...(cursor  ? { cursor }  : {}),
          })

          for (const key of page.keys) {
            keys.push(key.name)
          }

          cursor = page.list_complete ? undefined : (page as any).cursor
        } while (cursor !== undefined)

        return keys
      }
    )

    if (allKeys.length === 0) {
      return { deleted: 0, keys: [] }
    }

    // Step 2 — delete every collected key.
    // KV has no bulk-delete API so this fans out one delete per key.
    // Deleting an already-deleted key is a silent no-op, so retries are safe.
    await step.do(
      `delete ${allKeys.length} keys`,
      async () => {
        await Promise.all(allKeys.map(key => this.env.KV.delete(key)))
      }
    )

    return {
      deleted: allKeys.length,
      keys: allKeys,
    }
  }
}