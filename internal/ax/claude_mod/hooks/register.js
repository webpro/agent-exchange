async function report($, turn, ended = false) {
  try {
    const command = await $.env.get('AX_CLAUDE_OBSERVER');
    if (!command) return;
    const session = await $.session.id();
    let context = {};
    try {
      context = (await $.session.usage()).context;
    } catch {}
    const usage = { context_observed_at_ms: Date.now() };
    if (!ended && Number.isSafeInteger(context.tokens) && context.tokens >= 0) {
      usage.context_used_tokens = context.tokens;
    }
    if (Number.isSafeInteger(context.window) && context.window > 0) {
      usage.context_window_tokens = context.window;
    }
    const payload = { hook_event_name: 'ContextUsage', session_id: session, context_usage: usage };
    if (turn) {
      payload.hook_event_name = turn.status === 'inProgress' ? 'ClaudeTurnStart' : 'ClaudeTurnComplete';
      payload.claude_turn = turn;
    }
    await $.process.run([command, 'hook'], {
      stdin: JSON.stringify(payload),
      timeoutMs: 1000,
    });
  } catch {}
}

const pass = ($, e, next) => next(e);

export function register(on) {
  on('turn.start', async ($, e, next) => {
    if (!e.agentId) await report($, { id: e.turnId, status: 'inProgress', at_ms: Date.now() });
    return next(e);
  }).catch(pass);
  on('turn.complete', async ($, e, next) => {
    if (!e.agentId) {
      await report($, { id: e.turnId, status: e.isAborted ? 'interrupted' : 'completed', at_ms: Date.now() });
    }
    return next(e);
  }).catch(pass);
  on('session.start', async ($, e, next) => {
    await report($);
    return next(e);
  }).catch(pass);
  on('session.measure', async ($, e, next) => {
    await report($);
    return next(e);
  }).catch(pass);
  on('session.end', async ($, e, next) => {
    await report($, undefined, true);
    return next(e);
  }).catch(pass);
}
