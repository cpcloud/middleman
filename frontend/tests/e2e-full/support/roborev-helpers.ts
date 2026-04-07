import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import type { Page } from "@playwright/test";
import { expect } from "@playwright/test";

const thisDir = path.dirname(
  fileURLToPath(import.meta.url),
);

function readEnvFile(): Record<string, string> {
  const envPath = path.resolve(
    thisDir,
    "../../../../tests/integration/.env",
  );
  const content = readFileSync(envPath, "utf-8");
  const env: Record<string, string> = {};
  for (const line of content.split("\n")) {
    const eq = line.indexOf("=");
    if (eq > 0) {
      env[line.slice(0, eq)!] = line.slice(eq + 1);
    }
  }
  return env;
}

function composeExec(cmd: string): void {
  const env = readEnvFile();
  execSync(`docker compose ${cmd}`, {
    cwd: env["COMPOSE_DIR"],
    env: { ...process.env, ...env },
    stdio: "pipe",
    timeout: 30_000,
  });
}

export function stopDaemon(): void {
  composeExec("stop roborev");
}

export function startDaemon(): void {
  composeExec("start roborev");
}

export function restartDaemon(): void {
  composeExec("restart roborev");
}

export async function waitForReviewsReady(
  page: Page,
): Promise<void> {
  await page.goto("/reviews");
  await expect(
    page.locator(".job-table"),
  ).toBeVisible({ timeout: 15_000 });
}

export async function waitForJobRows(
  page: Page,
  min: number,
): Promise<void> {
  const rows = page.locator(".job-row");
  await expect(
    async () => {
      const count = await rows.count();
      expect(count).toBeGreaterThanOrEqual(min);
    },
  ).toPass({ timeout: 10_000 });
}

export async function openDrawer(
  page: Page,
  jobId: number,
): Promise<void> {
  await page.goto(`/reviews/${jobId}`);
  await expect(
    page.locator(".drawer"),
  ).toBeVisible({ timeout: 10_000 });
}
