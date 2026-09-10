# 前端套件

測試對話頁面使用以下本機封裝套件，執行時不向 CDN 下載程式：

| 套件 | 版本 | 用途 | 授權檔 |
| --- | --- | --- | --- |
| marked | 18.0.12 | Markdown 解析 | marked.LICENSE |
| DOMPurify | 3.4.15 | 清理輸出的 HTML | dompurify.LICENSE、dompurify.LICENSE-MPL |
| lucide | 1.44.0 | 操作圖示 | lucide.LICENSE |
| eventsource-parser | 4.1.0 | SSE 串流事件解析 | eventsource-parser.LICENSE |

來源為同名 npm 套件發布檔。`eventsource-parser.js` 是原套件的 `dist/index.js`，另保留其 `parse.js`、`errors.js` 依賴。更新時須一併更新版本及授權檔；請勿省略 Markdown HTML 清理。
