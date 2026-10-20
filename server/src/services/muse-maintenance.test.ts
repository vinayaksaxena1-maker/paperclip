import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { eq, sql } from "drizzle-orm";
import { agents, companies, createDb, museAgentBindings } from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../__tests__/helpers/embedded-postgres.js";
import { startMuseMaintenance } from "./muse-maintenance.js";

const effects = vi.hoisted(() => ({
  endQualification: vi.fn().mockResolvedValue(undefined),
  revokeOAuthGrant: vi.fn().mockResolvedValue(undefined),
  reconcileTerminalAssignments: vi.fn().mockResolvedValue(undefined),
  start: vi.fn(),
  stop: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("./muse-runner-broker.js", () => ({ museRunnerBroker: () => effects }));
vi.mock("./muse-receiver.js", () => ({ museReceiver: () => effects }));

const support = await getEmbeddedPostgresTestSupport();
const describeDatabase = support.supported ? describe : describe.skip;

describeDatabase("Muse maintenance background admission", () => {
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>>;
  let db: ReturnType<typeof createDb>;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("muse-maintenance-");
    db = createDb(database.connectionString, { maxConnections: 1 });
  });
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  });
  afterEach(async () => {
    vi.useRealTimers();
    await db.execute(sql`TRUNCATE TABLE companies CASCADE`);
  });
  afterAll(async () => {
    await db?.$client.end({ timeout: 1 });
    await database?.cleanup();
  });

  async function fixture(expired = false) {
    const [company] = await db.insert(companies).values({ name: "Maintenance", issuePrefix: `MT${randomUUID().slice(0, 6)}` }).returning();
    const [agent] = await db.insert(agents).values({ companyId: company.id, name: "Muse agent" }).returning();
    const [binding] = await db.insert(museAgentBindings).values({
      companyId: company.id,
      agentId: agent.id,
      operatorId: "synthetic-operator",
      qualificationId: randomUUID(),
      qualificationExpiresAt: new Date(Date.now() + (expired ? -1 : 1) * 3_600_000),
    }).returning();
    return binding;
  }

  function observeDatabase(failFirstUpdate = false) {
    const calls: string[] = [];
    const observed = new Proxy(db, { get(target, key) {
      if (!["select", "update", "insert", "delete", "execute", "transaction"].includes(String(key))) {
        return Reflect.get(target, key);
      }
      return (...args: unknown[]) => {
        calls.push(String(key));
        if (key === "update" && failFirstUpdate) {
          failFirstUpdate = false;
          throw new Error("temporary database failure");
        }
        return Reflect.apply(Reflect.get(target, key), target, args);
      };
    } });
    return { observed, calls };
  }

  it("leaves PostgreSQL untouched in standby and resumes maintenance after claim", async () => {
    const binding = await fixture();
    const { observed, calls } = observeDatabase();
    let enabled = false;
    const stop = startMuseMaintenance(observed, { isBackgroundWorkEnabled: () => enabled });
    try {
      await vi.advanceTimersByTimeAsync(10 * 60_000);
      expect(calls).toEqual([]);
      expect(effects.reconcileTerminalAssignments).not.toHaveBeenCalled();
      enabled = true;
      await vi.advanceTimersByTimeAsync(5000);
      await vi.waitFor(() => expect(effects.reconcileTerminalAssignments).toHaveBeenCalledWith(binding.companyId, binding.agentId, binding.id));
      const [saved] = await db.select().from(museAgentBindings).where(eq(museAgentBindings.id, binding.id));
      expect(saved.cadenceEvidenceIncompleteAt).toBeInstanceOf(Date);
      expect(calls.filter(call => call === "update")).toHaveLength(1);
      enabled = false;
      const settledCalls = [...calls];
      await vi.advanceTimersByTimeAsync(10 * 60_000);
      expect(calls).toEqual(settledCalls);
      enabled = true;
      await vi.advanceTimersByTimeAsync(5000);
      await vi.waitFor(() => expect(effects.reconcileTerminalAssignments).toHaveBeenCalledTimes(2));
      expect(calls.filter(call => call === "update")).toHaveLength(1);
    } finally {
      await stop();
    }
  });

  it("keeps ordinary startup and qualification deadline enforcement enabled by default", async () => {
    const binding = await fixture(true);
    const stop = startMuseMaintenance(db);
    try {
      await vi.waitFor(() => expect(effects.endQualification).toHaveBeenCalledWith({
        companyId: binding.companyId,
        agentId: binding.agentId,
        bindingId: binding.id,
        qualificationId: binding.qualificationId,
        operatorId: binding.operatorId,
      }));
      await vi.waitFor(() => expect(effects.reconcileTerminalAssignments).toHaveBeenCalled());
    } finally {
      await stop();
    }
  });

  it("retries startup evidence marking before reconciliation after a database failure", async () => {
    const binding = await fixture();
    const { observed, calls } = observeDatabase(true);
    const stop = startMuseMaintenance(observed);
    try {
      await Promise.resolve();
      expect(effects.reconcileTerminalAssignments).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(5000);
      await vi.waitFor(() => expect(effects.reconcileTerminalAssignments).toHaveBeenCalled());
      const [saved] = await db.select().from(museAgentBindings).where(eq(museAgentBindings.id, binding.id));
      expect(saved.cadenceEvidenceIncompleteAt).toBeInstanceOf(Date);
      expect(calls.filter(call => call === "update")).toHaveLength(2);
    } finally {
      await stop();
    }
  });
});
