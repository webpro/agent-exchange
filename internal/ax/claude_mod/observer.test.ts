import { expect, test } from 'claude-code/testing';

function fixture(on, context = { tokens: 32000, window: 128000, percent: 99 }) {
  const sent = [];
  on('env.get', () => ({ value: '/test/ax' }));
  on('session.id', () => ({ value: '11111111-1111-4111-8111-111111111111' }));
  on('session.usage', () => ({ value: { context, rateLimits: [] } }));
  on('process.run', ($, e) => {
    expect(e.argv).toEqual(['/test/ax', 'hook']);
    expect(e.init.timeoutMs).toBe(1000);
    sent.push(JSON.parse(e.init.stdin));
    return { value: { exitCode: 0, stdout: '', stderr: '' } };
  });
  on('session.measure', ($, e) => ({ changed: e.changed }));
  on('session.start', ($, e) => ({ cwd: e.cwd }));
  on('session.compact', () => ({ messages: [{ role: 'user', text: 'summary', toolUses: [] }] }));
  return { context, sent };
}

const measure = { context: { tokens: 32000, window: 128000 }, rateLimits: [], changed: ['context'] };

test('reports native absolute counts without changing the event', async ($, on) => {
  const { sent } = fixture(on);
  expect(await $.session.measure(measure)).toEqual({ changed: ['context'] });
  expect(sent.length).toBe(1);
  expect(sent[0].hook_event_name).toBe('ContextUsage');
  expect(sent[0].session_id).toBe('11111111-1111-4111-8111-111111111111');
  expect(sent[0].context_usage.context_used_tokens).toBe(32000);
  expect(sent[0].context_usage.context_window_tokens).toBe(128000);
  expect(sent[0].context_usage.context_observed_at_ms).toBeGreaterThan(0);
  expect(sent[0].context_usage.context_used_percent).toBeUndefined();
});

test('startup and missing readings remain unknown', async ($, on) => {
  const { sent } = fixture(on, { window: 128000 });
  await $.session.start({ cwd: '/test', surface: 'terminal', isInteractive: true });
  expect(sent.length).toBe(1);
  expect(sent[0].context_usage.context_used_tokens).toBeUndefined();
  expect(sent[0].context_usage.context_window_tokens).toBe(128000);
});

test('a missing window is omitted and a native zero is retained', async ($, on) => {
  const { sent } = fixture(on, { tokens: 0 });
  await $.session.measure(measure);
  expect(sent[0].context_usage.context_used_tokens).toBe(0);
  expect(sent[0].context_usage.context_window_tokens).toBeUndefined();
});

test('measurements follow native missing and restored context values', async ($, on) => {
  const { sent, context } = fixture(on);
  await $.session.measure(measure);
  delete context.tokens;
  await $.session.measure({ ...measure, changed: ['rateLimits'] });
  expect(sent.length).toBe(2);
  expect(sent[1].context_usage.context_used_tokens).toBeUndefined();
  expect(sent[1].context_usage.context_window_tokens).toBe(128000);
  context.tokens = 8000;
  await $.session.measure({ ...measure, context: { tokens: 8000, window: 128000 } });
  expect(sent[2].context_usage.context_used_tokens).toBe(8000);
});

test('a subagent compaction does not invalidate the main conversation', async ($, on) => {
  const { sent } = fixture(on);
  await $.session.compact({ trigger: 'auto', messages: [{ role: 'user', text: 'prompt', toolUses: [] }], agentId: 'child' });
  expect(sent.length).toBe(0);
});

test('compaction is left to the settings hook even when the event runs', async ($, on) => {
  const { sent } = fixture(on);
  await $.session.compact({ trigger: 'auto', messages: [{ role: 'user', text: 'prompt', toolUses: [] }] });
  expect(sent.length).toBe(0);
});

test('broker absence or process failure cannot fail the native event', async ($, on) => {
  let attempts = 0;
  on('env.get', () => ({ value: '/test/ax' }));
  on('session.id', () => ({ value: '11111111-1111-4111-8111-111111111111' }));
  on('session.usage', () => ({ value: { context: { tokens: 32000, window: 128000 }, rateLimits: [] } }));
  on('process.run', () => { attempts++; throw new Error('broker gone'); });
  on('session.measure', ($, e) => ({ changed: e.changed }));
  expect(await $.session.measure(measure)).toEqual({ changed: ['context'] });
  expect(attempts).toBe(1);
});

for (const answer of ['', 'partial generation']) {
  test('reports aborted turn: ' + (answer ? 'generation' : 'tool'), async ($, on) => {
    const { sent } = fixture(on);
    on('turn.complete', ($, e) => ({ text: e.answer }));
    const result = await $.turn.complete({ answer, durationMs: 12, isAborted: true, reason: 'aborted', turnId: 'turn-123' });
    expect(result).toEqual({ text: answer });
    expect(sent.length).toBe(1);
    expect(sent[0].hook_event_name).toBe('ClaudeTurnComplete');
    expect(sent[0].claude_turn.id).toBe('turn-123');
    expect(sent[0].claude_turn.status).toBe('interrupted');
    expect(sent[0].claude_turn.at_ms).toBeGreaterThan(0);
  });
}

test('normal completion and subagent interruption emit no interrupted report', async ($, on) => {
  const { sent } = fixture(on);
  on('turn.complete', ($, e) => ({ text: e.answer }));
  await $.turn.complete({ answer: 'OK', durationMs: 12, isAborted: false, reason: 'answer', turnId: 'normal' });
  await $.turn.complete({ answer: '', durationMs: 12, isAborted: true, reason: 'aborted', turnId: 'child-turn', agentId: 'child' });
  expect(sent.length).toBe(1);
  expect(sent[0].claude_turn.status).toBe('completed');
});

test('queued main turn reports busy after an aborted turn without UserPromptSubmit', async ($, on) => {
  const { sent } = fixture(on);
  on('turn.start', ($, e) => ({ turnId: e.turnId }));
  on('turn.complete', ($, e) => ({ text: e.answer }));
  await $.turn.start({ turnId: 'first', text: 'sleep' });
  await $.turn.complete({ answer: '', durationMs: 12, isAborted: true, reason: 'aborted', turnId: 'first' });
  await $.turn.start({ turnId: 'queued', text: 'queued essay' });
  expect(sent[sent.length - 1].hook_event_name).toBe('ClaudeTurnStart');
});
