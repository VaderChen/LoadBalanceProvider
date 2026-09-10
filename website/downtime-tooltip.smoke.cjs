const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

const html = fs.readFileSync(path.join(__dirname, "main.html"), "utf8");
const start = html.indexOf("    function buildProviderUsageTooltip(");
const end = html.indexOf("    function buildRemainingUsageRows(", start);
assert.ok(start >= 0 && end > start, "找不到泡泡產生函式");
const context = vm.createContext({
  buildRemainingUsageRows: rows => rows.push({ label: "更新時間", value: "無" }),
  escapeHtml: value => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;")
});
vm.runInContext(html.slice(start, end), context);

const cases = [
  ["日間", { enabled: true, start: "04:00", end: "05:00" }, "每日 04:00–05:00（台北時間）"],
  ["午後", { enabled: true, start: "16:00", end: "17:00" }, "每日 16:00–17:00（台北時間）"],
  ["跨午夜", { enabled: true, start: "23:00", end: "01:00" }, "每日 23:00–次日 01:00（台北時間）"],
  ["關閉", { enabled: false, start: "04:00", end: "05:00" }, "未啟用"],
  ["無設定", undefined, "未啟用"],
  ["缺少時間", { enabled: true, start: "04:00" }, "未啟用"]
];
for (const [name, downtime, expected] of cases) {
  const result = context.buildProviderUsageTooltip({}, { downtime });
  assert.ok(result.includes("停機時段") && result.includes(expected), name);
  assert.equal((result.match(/停機時段/g) || []).length, 1, name);
  console.log(`PASS ${name}: ${expected}`);
}
assert.ok(context.buildProviderUsageTooltip({}, undefined).includes("未啟用"));
const escaped = context.buildProviderUsageTooltip({}, { downtime: { enabled: true, start: "<img>", end: "05:00" } });
assert.ok(!escaped.includes("<img>"), "排程文字須經 HTML 跳脫");
console.log("PASS 缺少來源及 HTML 跳脫");
