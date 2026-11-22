/**
 * Cloudflare Worker Cron Trigger for Kyle's News
 * 準時於台灣時間 07:30、09:15（財經皓角直播後）、12:30、16:00 與 21:30
 * 透過 GitHub workflow_dispatch API 觸發更新，繞過 GitHub Actions 免費排程佇列延遲。
 *
 * 所需環境變數／Secret：
 * - GITHUB_PAT: 僅限 `yikai02/news-dashboard` 且僅需 `Actions: Read and write` 權限的 Fine-grained PAT
 */
const MORNING_CRONS = new Set(["30 23 * * *", "15 1 * * *", "30 4 * * *"]);

export default {
  async scheduled(event, env) {
    const edition = MORNING_CRONS.has(event.cron) ? "morning" : "evening";
    const owner = env.GITHUB_OWNER || "yikai02";
    const repo = env.GITHUB_REPO || "news-dashboard";
    const workflow = env.WORKFLOW_FILE || "update_and_deploy.yml";

    if (!env.GITHUB_PAT) {
      throw new Error("Missing GITHUB_PAT secret on Cloudflare Worker");
    }

    const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow}/dispatches`;
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.GITHUB_PAT}`,
        Accept: "application/vnd.github+json",
        "User-Agent": "Cloudflare-NewsDashboard-Cron",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        ref: "main",
        inputs: { edition },
      }),
    });

    if (!resp.ok) {
      const detail = await resp.text();
      throw new Error(`GitHub dispatch failed (${resp.status}): ${detail}`);
    }
  },
};
