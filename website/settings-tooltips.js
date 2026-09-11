(() => {
  const descriptions = {
    globalDispatchRateEnabled: '開啟後，所有 Provider 推論請求合計每 5 秒最多發送一筆，不累積突發額度。重試與直連診斷也受限；關閉後仍保留每個來源原有的 10 秒間隔。僅限本服務程序，不跨部署共用。',
    globalConcurrencyEnabled: '開啟後，全站上游推論併發上限為啟用且未處於停機時段的對話 Provider 帳號條目數 × 2，不計分類器及空端點；一般冷卻不改變計數。包含重試與直連診斷，沒有啟用來源時保留 2 條診斷額度。這是全站總額度，不強制每帳號各占 2 條；各來源自己的最大併發仍有效。上限降低不取消既有連線。',
    cooldownSingleProbeEnabled: '開啟後，來源或模型冷卻會保守地限制整個 Provider；冷卻結束後只允許一條上游推論連線探測。正式代理確認成功才解除探測限制，失敗依既有冷卻策略處理；直連診斷不會解除限制。不改變對話綁定。',
    conversationAffinityTTLMinutes: '保留對話與來源配對的時間。調長可延長配對保留；是否可沿用仍受來源狀態與安全續接規則限制。',
    conversationAffinityQuotaTolerancePoints: '沿用來源時可接受的剩餘配額差距，單位是百分點。值越大越偏向保留原配對；不代表可用配額，也不會授權任意切換回合中的來源。',
    responseRouteMaxEntries: '最多保留多少筆 Response ID 與來源的路由紀錄。不是請求次數或併發上限；較大的值可保留更多續接紀錄，但會增加記憶體使用。',
    providerCapacityCooldownSeconds: '容量或限流錯誤的基本冷卻秒數。暫時過載另有退避機制，上游提供的等待時間也可能影響實際冷卻；不是所有錯誤都固定等待此秒數。',
    providerServerErrorCooldownSeconds: '一般可重試伺服器錯誤後暫停選用來源或模型的秒數。調長可降低重送頻率，但恢復較慢；上游有 Retry-After 時依其提示處理。',
    providerRetryRounds: '選路可增加的重試輪數。0 表示不增加輪次，不等於禁止所有重試；實際請求仍受總嘗試額度、來源綁定與安全重播限制。',
    providerRetrySourcesPerRound: '一輪自動選路最多考慮的來源數，0 表示不限制此項。調小可減少跨來源嘗試；不會解除既有回合綁定或其他嘗試上限。',
    providerRetryWaitSeconds: '代理願意累計等待冷卻的秒數，0 表示不等待。首次排隊與失敗後等待分開累計；等待期間不派送推論請求，上游執行時間不計入。',
    persistQuotaCooldown: '將長時間配額冷卻保存到磁碟，重新啟動後仍可恢復，避免對尚未恢復配額的來源重複送出請求。',
    maxBindingsPerProvider: '新對話分配時，每個來源可承載的綁定數目標。不是實際連線或併發上限；既有綁定不受影響，所有來源達標時仍可能繼續分配。'
  };
  const bubble = document.createElement('div');
  bubble.id = 'settingsParameterTooltip';
  bubble.className = 'parameter-tooltip';
  bubble.setAttribute('role', 'tooltip');
  bubble.setAttribute('popover', 'manual');
  bubble.hidden = true;
  document.body.append(bubble);
  let active = null;
  const position = () => {
    if (!active) return;
    const rect = active.getBoundingClientRect();
    const width = document.documentElement.clientWidth;
    const height = window.innerHeight;
    const bounds = bubble.getBoundingClientRect();
    bubble.style.left = `${Math.max(8, Math.min(rect.left, width - bounds.width - 8))}px`;
    const below = rect.bottom;
    bubble.style.top = `${Math.max(8, Math.min(below + bounds.height <= height - 8 ? below : rect.top - bounds.height, height - bounds.height - 8))}px`;
  };
  const hide = () => {
    active = null;
    if (bubble.hidePopover && bubble.matches(':popover-open')) bubble.hidePopover();
    bubble.hidden = true;
  };
  const show = (title, text) => {
    active = title;
    bubble.textContent = text;
    bubble.hidden = false;
    if (bubble.showPopover && !bubble.matches(':popover-open')) bubble.showPopover();
    position();
  };
  for (const [id, text] of Object.entries(descriptions)) {
    const field = document.getElementById(id);
    const title = field?.closest('.field')?.querySelector(':scope > span') || field?.closest('label')?.querySelector('span');
    if (!title) continue;
    title.classList.add('parameter-help');
    title.tabIndex = 0;
    const description = document.createElement('span');
    description.id = `${id}-description`;
    description.hidden = true;
    description.textContent = text;
    document.body.append(description);
    title.setAttribute('aria-describedby', description.id);
    field.setAttribute('aria-describedby', description.id);
    title.addEventListener('mouseenter', () => show(title, text));
    title.addEventListener('focus', () => show(title, text));
    title.addEventListener('mouseleave', event => {
      if (!bubble.contains(event.relatedTarget) && document.activeElement !== title) hide();
    });
    title.addEventListener('blur', hide);
  }
  bubble.addEventListener('mouseleave', () => {
    if (document.activeElement !== active) hide();
  });
  document.addEventListener('keydown', event => { if (event.key === 'Escape') hide(); });
  window.addEventListener('resize', hide);
  window.addEventListener('scroll', hide, true);
})();
