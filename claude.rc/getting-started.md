# Your first exchange

AX connects named coding agents on the same machine. You can pair different harnesses, work across repositories, and keep using your existing conversations.

## Install AX

Run these commands in a POSIX shell on macOS, Linux, or WSL 2:

```sh
curl -fsSL https://useax.dev/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
ax version
ax doctor
```

Or give your coding agent this prompt:

> Read https://useax.dev/agents.md and install Agent Exchange.

The [installation guide](../AGENTS.md) covers platform requirements, fish shell setup, and updates. Install and sign in to Claude Code, Codex CLI, or another supported harness separately.

## Open two terminals

In the first terminal, start Claude as `api`:

```sh
ax claude --name api
```

In the second, start Codex as `web`:

```sh
ax codex --name web
```

The terminals can be in different repositories. For this local setup, both sessions run on the same machine as the same OS user. On Windows, run both inside the same WSL 2 distribution. To connect another machine, follow [Tailscale setup](tailscale.md).

Confirm Claude's development Channel prompt, then submit any prompt in its window to release held mail. Codex needs native queue support. See [supported harnesses](harnesses.md).

## Send a message

Ask Codex:

> Ask api whether the schema is ready.

Codex sends through AX and ends its turn. AX wakes Claude with the request, and Claude can reply into the Codex conversation. No need to copy text between terminals or ask an agent to keep polling.

A sent message is queued durably. A reply can take time while the other agent works, and a delivery acknowledgment does not prove the task finished.

## Bring an existing conversation

Use the native resume syntax through AX:

```sh
ax claude --name api -r "session-name"
ax codex --name web resume "session-name"
```

Close the old terminal before adopting its conversation through AX. Once a name is bound, launching that name without extra arguments resumes its saved conversation. [Sessions and permissions](README.md) explains how selection and delegation work.

## Try a useful handoff

Ask an implementation agent to send a concrete task to a reviewer:

> Ask reviewer to review PR #42, check the changed behavior, and post its findings on GitHub.

The recipient keeps the task’s scope and its own native permission controls. Be explicit about external actions such as posting a review.

If an agent is missing or a reply does not arrive, start with the [delivery and recovery guide](troubleshooting.md).
