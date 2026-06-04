// `npx rogerthat listen-here` smoke test.
// Boots the app on a real TCP port (since runListenHere uses global `fetch` with
// a URL string, we need a reachable origin) and exercises the receiver end-to-end:
// stdout dump, --inbox file, --on-message hook, and reconnect with `since`.

import { serve, type ServerType } from "@hono/node-server";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import { runListenHere } from "../src/listen-here.js";

type ServerCtx = {
  server: ServerType;
  origin: string;
  channelId: string;
  channelToken: string;
  alphaSession: string;
  betaSession: string;
  tmp: string;
};

async function boot(): Promise<ServerCtx> {
  const app = createApp({ publicOrigin: "http://127.0.0.1:0", authRequired: true });
  return new Promise((resolve, reject) => {
    const server = serve({ fetch: app.fetch, hostname: "127.0.0.1", port: 0 }, async (info) => {
      const origin = `http://127.0.0.1:${info.port}`;
      try {
        const created = await fetch(`${origin}/api/channels`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ retention: "none" }),
        }).then((r) => r.json() as Promise<{ channel_id: string; join_token: string }>);
        const channelId = created.channel_id;
        const channelToken = created.join_token;
        const joinOne = async (callsign: string) =>
          fetch(`${origin}/api/channels/${channelId}/join`, {
            method: "POST",
            headers: { "content-type": "application/json", authorization: `Bearer ${channelToken}` },
            body: JSON.stringify({ callsign }),
          }).then((r) => r.json() as Promise<{ session_id: string }>);
        const alpha = await joinOne("alpha");
        const beta = await joinOne("beta");
        resolve({
          server,
          origin,
          channelId,
          channelToken,
          alphaSession: alpha.session_id,
          betaSession: beta.session_id,
          tmp: mkdtempSync(join(tmpdir(), "rogerthat-listen-here-")),
        });
      } catch (err) {
        reject(err);
      }
    });
  });
}

function shutdown(ctx: ServerCtx): Promise<void> {
  return new Promise((resolve) => ctx.server.close(() => resolve()));
}

async function sendFromBeta(
  ctx: ServerCtx,
  text: string,
  to = "alpha",
  priority?: "min" | "low" | "default" | "high" | "urgent",
  suggestedReplies?: string[],
): Promise<void> {
  await fetch(`${ctx.origin}/api/channels/${ctx.channelId}/send`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${ctx.channelToken}`,
      "x-session-id": ctx.betaSession,
    },
    body: JSON.stringify({
      to,
      text,
      ...(priority ? { priority } : {}),
      ...(suggestedReplies ? { suggested_replies: suggestedReplies } : {}),
    }),
  });
}

/** Run runListenHere with a soft timeout; returns the exit code. The caller
 *  signals shutdown via SIGINT by sending it to the current process — vitest
 *  doesn't have a clean abort hook for an in-process call, so we just race
 *  the listen-here promise against a deadline.
 *
 *  IMPORTANT: clear the timeout once the listener resolves. Otherwise a stale
 *  setTimeout sits in the event loop and fires `process.emit("SIGINT")` after
 *  the test finishes — which lands on whichever listener happens to be running
 *  in the NEXT test and kills it prematurely. */
async function runListener(args: string[], deadlineMs: number): Promise<{ code: number }> {
  let timer: NodeJS.Timeout | undefined;
  const done = runListenHere(args).then((code) => {
    if (timer) clearTimeout(timer);
    return { code };
  });
  const timeout = new Promise<{ code: number }>((resolve) => {
    timer = setTimeout(() => {
      process.emit("SIGINT");
      resolve({ code: -1 });
    }, deadlineMs);
  });
  return Promise.race([done, timeout]);
}

let ctx: ServerCtx;

beforeEach(async () => {
  ctx = await boot();
});

afterEach(async () => {
  await shutdown(ctx);
});

describe("rogerthat listen-here", () => {
  it("--inbox: appends each message as a JSON line", async () => {
    const inbox = join(ctx.tmp, "rr-inbox.jsonl");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--quiet",
      ],
      1500,
    );
    // Give the SSE connection a tick to open before we send.
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "hello-one");
    await sendFromBeta(ctx, "hello-two");
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    expect(existsSync(inbox)).toBe(true);
    const lines = readFileSync(inbox, "utf8").trim().split("\n").filter((l) => !l.startsWith("⟲"));
    expect(lines).toHaveLength(2);
    const parsed = lines.map((l) => JSON.parse(l) as { from: string; text: string });
    expect(parsed[0].text).toBe("hello-one");
    expect(parsed[1].text).toBe("hello-two");
    expect(parsed[0].from).toBe("beta");
  });

  it("--format text: writes '[<from>] <text>' lines, newlines collapsed", async () => {
    const inbox = join(ctx.tmp, "rr-inbox.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--format", "text",
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "one-liner");
    await sendFromBeta(ctx, "multi\nline\nbody");
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    expect(existsSync(inbox)).toBe(true);
    const lines = readFileSync(inbox, "utf8").trim().split("\n").filter((l) => !l.startsWith("⟲"));
    expect(lines).toEqual(["[beta] one-liner", "[beta] multi line body"]);
  });

  it("--format text: surfaces priority as a [priority] prefix", async () => {
    const inbox = join(ctx.tmp, "rr-prio.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--format", "text",
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "background update", "alpha", "low");
    await sendFromBeta(ctx, "regular thing", "alpha");
    await sendFromBeta(ctx, "wake up!", "alpha", "urgent");
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    const lines = readFileSync(inbox, "utf8").trim().split("\n").filter((l) => !l.startsWith("⟲"));
    expect(lines).toEqual([
      "[low] [beta] background update",
      "[beta] regular thing",
      "[urgent] [beta] wake up!",
    ]);
  });

  it("--format text: appends suggested_replies as → [r1] [r2] suffix", async () => {
    const inbox = join(ctx.tmp, "rr-replies.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--format", "text",
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "deploy ahora?", "alpha", undefined, ["yes", "no", "show diff"]);
    await sendFromBeta(ctx, "no chips here", "alpha");
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    const lines = readFileSync(inbox, "utf8").trim().split("\n").filter((l) => !l.startsWith("⟲"));
    expect(lines).toEqual([
      "[beta] deploy ahora?  → [yes] [no] [show diff]",
      "[beta] no chips here",
    ]);
  });

  it("--format jsonl: passes attachments through verbatim", async () => {
    const inbox = join(ctx.tmp, "rr-att.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    // 1x1 transparent PNG, base64 ~68 chars
    const tinyPng =
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkAAIAAAoAAv/lxKUAAAAASUVORK5CYII=";
    await fetch(`${ctx.origin}/api/channels/${ctx.channelId}/send`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${ctx.channelToken}`,
        "x-session-id": ctx.betaSession,
      },
      body: JSON.stringify({
        to: "alpha",
        text: "see attached",
        attachments: [{ mime: "image/png", data_base64: tinyPng, filename: "tiny.png" }],
      }),
    });
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    const parsed = JSON.parse(readFileSync(inbox, "utf8").trim()) as {
      attachments?: { mime: string; data_base64: string; filename?: string }[];
    };
    expect(parsed.attachments).toBeTruthy();
    expect(parsed.attachments!).toHaveLength(1);
    expect(parsed.attachments![0].mime).toBe("image/png");
    expect(parsed.attachments![0].data_base64).toBe(tinyPng);
    expect(parsed.attachments![0].filename).toBe("tiny.png");
  });

  it("rejects attachments over the size cap (413)", async () => {
    // 600KB of valid base64 ('A' repeated produces valid base64)
    const huge = "A".repeat(600 * 1024);
    const res = await fetch(`${ctx.origin}/api/channels/${ctx.channelId}/send`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${ctx.channelToken}`,
        "x-session-id": ctx.betaSession,
      },
      body: JSON.stringify({
        to: "alpha",
        text: "x",
        attachments: [{ mime: "image/png", data_base64: huge }],
      }),
    });
    expect(res.status).toBe(413);
    const body = (await res.json()) as { code: string };
    expect(body.code).toBe("invalid");
  });

  it("rejects attachments with non-allowlisted MIME (400)", async () => {
    const res = await fetch(`${ctx.origin}/api/channels/${ctx.channelId}/send`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${ctx.channelToken}`,
        "x-session-id": ctx.betaSession,
      },
      body: JSON.stringify({
        to: "alpha",
        text: "x",
        attachments: [{ mime: "application/x-msdownload", data_base64: "AAAA" }],
      }),
    });
    expect(res.status).toBe(400);
  });

  it("--format jsonl: passes suggested_replies through verbatim", async () => {
    const inbox = join(ctx.tmp, "rr-replies-json.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "approve?", "alpha", undefined, ["approve", "deny"]);
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    const parsed = JSON.parse(readFileSync(inbox, "utf8").trim()) as { suggested_replies?: string[] };
    expect(parsed.suggested_replies).toEqual(["approve", "deny"]);
  });

  it("--min-priority filters out lower-priority messages entirely", async () => {
    const inbox = join(ctx.tmp, "rr-minprio.log");
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--inbox", inbox,
        "--format", "text",
        "--min-priority", "high",
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "noise 1", "alpha", "low");
    await sendFromBeta(ctx, "noise 2", "alpha"); // default = rank 2 < high(3)
    await sendFromBeta(ctx, "signal!", "alpha", "high");
    await sendFromBeta(ctx, "EMERGENCY", "alpha", "urgent");
    await new Promise((r) => setTimeout(r, 400));
    process.emit("SIGINT");
    await listener;
    const lines = readFileSync(inbox, "utf8").trim().split("\n").filter((l) => !l.startsWith("⟲"));
    expect(lines).toEqual([
      "[high] [beta] signal!",
      "[urgent] [beta] EMERGENCY",
    ]);
  });

  it("--on-message: spawns the hook with RR_* env vars set", async () => {
    const marker = join(ctx.tmp, "hook-marker.txt");
    // POSIX-friendly hook: append $RR_FROM:$RR_MESSAGE to a marker file.
    const hook = `printf '%s\\n' "$RR_FROM:$RR_MESSAGE" >> ${marker.replace(/'/g, "'\\''")}`;
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", ctx.channelToken,
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--on-message", hook,
        "--quiet",
      ],
      1500,
    );
    await new Promise((r) => setTimeout(r, 200));
    await sendFromBeta(ctx, "from-hook");
    // Give the spawned shell a moment to flush.
    await new Promise((r) => setTimeout(r, 500));
    process.emit("SIGINT");
    await listener;
    expect(existsSync(marker)).toBe(true);
    const out = readFileSync(marker, "utf8").trim();
    expect(out).toBe("beta:from-hook");
  });

  it("rejects bad bearer with non-zero exit", async () => {
    const listener = runListener(
      [
        "--channel", ctx.channelId,
        "--token", "wrong-token",
        "--session", ctx.alphaSession,
        "--origin", ctx.origin,
        "--quiet",
      ],
      1500,
    );
    const result = await listener;
    expect(result.code).toBe(1);
  });
});
