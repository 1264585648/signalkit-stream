"use strict";

const state = {
  overview: null,
  events: [],
  eventTotal: 0,
  offset: 0,
  limit: 30,
  hasMore: false,
  loadingEvents: false,
  selectedEvent: null,
  retrySink: null,
  refreshTimer: null,
  lastRefresh: null,
};

const $ = (id) => document.getElementById(id);
const elements = {
  refreshButton: $("refreshButton"),
  refreshInterval: $("refreshInterval"),
  globalNotice: $("globalNotice"),
  noticeTitle: $("noticeTitle"),
  noticeMessage: $("noticeMessage"),
  noticeRetry: $("noticeRetry"),
  connectionCard: $("connectionCard"),
  connectionLabel: $("connectionLabel"),
  databaseLabel: $("databaseLabel"),
  footerDatabase: $("footerDatabase"),
  pageTitle: $("pageTitle"),
  healthHero: $("healthHero"),
  healthSummary: $("healthSummary"),
  overviewHeading: $("overviewHeading"),
  schemaBadge: $("schemaBadge"),
  lastUpdated: $("lastUpdated"),
  metricSignals: $("metricSignals"),
  metricLastHour: $("metricLastHour"),
  metricActivity: $("metricActivity"),
  metricSources: $("metricSources"),
  metricSourcesHint: $("metricSourcesHint"),
  metricDelivery: $("metricDelivery"),
  recentSignalList: $("recentSignalList"),
  attentionList: $("attentionList"),
  navSignalCount: $("navSignalCount"),
  navDeliveryCount: $("navDeliveryCount"),
  sourceHealthDot: $("sourceHealthDot"),
  signalSearch: $("signalSearch"),
  sourceFilter: $("sourceFilter"),
  kindFilter: $("kindFilter"),
  clearFilters: $("clearFilters"),
  emptyClearFilters: $("emptyClearFilters"),
  resultCount: $("resultCount"),
  signalList: $("signalList"),
  signalEmpty: $("signalEmpty"),
  pagination: $("pagination"),
  previousPage: $("previousPage"),
  nextPage: $("nextPage"),
  pageStatus: $("pageStatus"),
  sourceGrid: $("sourceGrid"),
  sourceEmpty: $("sourceEmpty"),
  deliveryGrid: $("deliveryGrid"),
  deliveryEmpty: $("deliveryEmpty"),
  actionModeChip: $("actionModeChip"),
  detailDrawer: $("detailDrawer"),
  drawerKind: $("drawerKind"),
  drawerTitle: $("drawerTitle"),
  drawerBody: $("drawerBody"),
  copySignalJson: $("copySignalJson"),
  openOriginal: $("openOriginal"),
  retryDialog: $("retryDialog"),
  retryDialogCopy: $("retryDialogCopy"),
  confirmRetry: $("confirmRetry"),
  toastRegion: $("toastRegion"),
};

function createElement(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { Accept: "application/json", ...(options.headers || {}) },
  });
  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }
  if (!response.ok) {
    const message = payload?.error?.message || `Request failed with HTTP ${response.status}`;
    const error = new Error(message);
    error.code = payload?.error?.code || "request_failed";
    error.status = response.status;
    throw error;
  }
  return payload;
}

function setConnection(status, label, detail) {
  elements.connectionCard.classList.remove("is-online", "is-offline");
  if (status === "online") elements.connectionCard.classList.add("is-online");
  if (status === "offline") elements.connectionCard.classList.add("is-offline");
  elements.connectionLabel.textContent = label;
  elements.databaseLabel.textContent = detail || "";
}

function showNotice(title, message) {
  elements.noticeTitle.textContent = title;
  elements.noticeMessage.textContent = message;
  elements.globalNotice.hidden = false;
}

function hideNotice() {
  elements.globalNotice.hidden = true;
}

function formatCount(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat(undefined, { notation: number >= 10000 ? "compact" : "standard" }).format(number);
}

function parseDate(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function relativeTime(value) {
  const date = parseDate(value);
  if (!date) return "Never";
  const seconds = Math.round((date.getTime() - Date.now()) / 1000);
  const absolute = Math.abs(seconds);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (absolute < 60) return formatter.format(seconds, "second");
  const minutes = Math.round(seconds / 60);
  if (Math.abs(minutes) < 60) return formatter.format(minutes, "minute");
  const hours = Math.round(minutes / 60);
  if (Math.abs(hours) < 24) return formatter.format(hours, "hour");
  const days = Math.round(hours / 24);
  if (Math.abs(days) < 30) return formatter.format(days, "day");
  return date.toLocaleString();
}

function exactTime(value) {
  const date = parseDate(value);
  return date ? date.toLocaleString() : "—";
}

function truncate(value, length = 120) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.length > length ? `${text.slice(0, length - 1)}…` : text;
}

function sourceInitial(source) {
  const parts = String(source || "?").split(/[-_:/]/).filter(Boolean);
  return (parts.length > 1 ? parts.slice(0, 2).map((part) => part[0]).join("") : String(source || "?").slice(0, 2)).toUpperCase();
}

function healthClass(status) {
  const value = String(status || "").toLowerCase();
  if (["healthy", "success", "current", "enabled"].includes(value)) return "is-healthy";
  if (["failed", "error", "dead", "unhealthy", "disabled"].includes(value)) return "is-danger";
  return "is-warning";
}

function safeExternalUrl(value) {
  try {
    const url = new URL(value);
    return ["http:", "https:"].includes(url.protocol) ? url.href : null;
  } catch {
    return null;
  }
}

function updateDocumentTitle() {
  const attention = Number(state.overview?.delivery_attention || 0);
  document.title = attention > 0
    ? `(${attention}) SignalKit Operator Console`
    : "SignalKit Operator Console";
}

async function refreshAll({ quiet = false } = {}) {
  if (!quiet) elements.refreshButton.classList.add("is-refreshing");
  setConnection("connecting", "Refreshing", "Reading persisted state…");
  try {
    const [overview, recent] = await Promise.all([
      api("/api/overview"),
      api("/api/events?limit=5&offset=0"),
    ]);
    state.overview = overview;
    state.lastRefresh = new Date();
    renderOverview(overview, recent.items || []);
    populateFilters(overview.facets || {});
    hideNotice();
    setConnection("online", "Connected", overview.snapshot.database);
    elements.footerDatabase.textContent = overview.snapshot.database;
    await loadEvents({ keepOffset: true, quiet: true });
  } catch (error) {
    setConnection("offline", "Unavailable", "Check the database path");
    showNotice(
      error.code === "database_missing" ? "Database not found" : "Unable to read Stream state",
      error.message,
    );
    renderUnavailable(error);
  } finally {
    elements.refreshButton.classList.remove("is-refreshing");
  }
}

function renderUnavailable(error) {
  elements.healthHero.classList.remove("is-healthy", "is-warning");
  elements.healthHero.classList.add("is-danger");
  elements.overviewHeading.textContent = error.code === "database_missing" ? "No Stream database yet" : "Stream state is unavailable";
  elements.healthSummary.textContent = error.message;
  elements.schemaBadge.textContent = "Schema unavailable";
  elements.schemaBadge.classList.remove("is-current");
  for (const node of [elements.metricSignals, elements.metricLastHour, elements.metricActivity, elements.metricSources, elements.metricDelivery]) {
    node.textContent = "—";
  }
  elements.recentSignalList.replaceChildren(emptyCompact("No recent signals available."));
  elements.attentionList.replaceChildren(attentionItem("Database unavailable", error.message, "is-danger"));
}

function renderOverview(overview, recentEvents) {
  const snapshot = overview.snapshot;
  const sources = snapshot.sources || [];
  const sinks = snapshot.sinks || [];
  const unhealthySources = sources.filter((source) => healthClass(source.status) !== "is-healthy");
  const healthySources = sources.length - unhealthySources.length;
  const deliveryAttention = Number(overview.delivery_attention || 0);
  const schemaCurrent = snapshot.schema_status === "current";

  elements.healthHero.classList.remove("is-healthy", "is-warning", "is-danger");
  let heroClass = "is-healthy";
  let heading = "Your signal stream is healthy";
  let summary = `${healthySources} of ${sources.length} sources are healthy and delivery has no unresolved failures.`;
  if (!schemaCurrent || deliveryAttention > 0 || unhealthySources.length > 0) {
    heroClass = !schemaCurrent || unhealthySources.some((source) => healthClass(source.status) === "is-danger") ? "is-danger" : "is-warning";
    heading = heroClass === "is-danger" ? "The stream needs attention" : "The stream is operating with warnings";
    const parts = [];
    if (!schemaCurrent) parts.push(`database schema is ${snapshot.schema_status}`);
    if (unhealthySources.length) parts.push(`${unhealthySources.length} source${unhealthySources.length === 1 ? "" : "s"} not healthy`);
    if (deliveryAttention) parts.push(`${deliveryAttention} delivery row${deliveryAttention === 1 ? "" : "s"} need review`);
    summary = `${parts.join(", ")}.`;
  } else if (sources.length === 0) {
    heroClass = "is-warning";
    heading = "Stream storage is ready";
    summary = "The database is current, but no source workers have persisted health state yet.";
  }
  elements.healthHero.classList.add(heroClass);
  elements.overviewHeading.textContent = heading;
  elements.healthSummary.textContent = summary;
  elements.schemaBadge.textContent = `Schema ${snapshot.schema_version}/${snapshot.supported_schema_version}`;
  elements.schemaBadge.classList.toggle("is-current", schemaCurrent);
  elements.lastUpdated.textContent = `Updated ${relativeTime(overview.snapshot.collected_at)}`;

  elements.metricSignals.textContent = formatCount(snapshot.signals_total);
  elements.metricLastHour.textContent = formatCount(overview.activity.last_hour);
  elements.metricActivity.textContent = formatCount(overview.activity.last_24h);
  elements.metricSources.textContent = sources.length ? `${healthySources}/${sources.length}` : "0";
  elements.metricSourcesHint.textContent = sources.length ? `${unhealthySources.length} source${unhealthySources.length === 1 ? "" : "s"} need attention` : "No source runs recorded";
  elements.metricDelivery.textContent = formatCount(deliveryAttention);
  elements.navSignalCount.textContent = formatCount(snapshot.signals_total);
  elements.navDeliveryCount.textContent = String(deliveryAttention);
  elements.navDeliveryCount.hidden = deliveryAttention === 0;
  elements.sourceHealthDot.className = `nav-dot ${heroClass}`;
  elements.databaseLabel.textContent = snapshot.database;
  elements.footerDatabase.textContent = snapshot.database;
  elements.actionModeChip.textContent = overview.actions_enabled ? "Actions enabled" : "Read-only";
  elements.actionModeChip.className = `mode-chip ${overview.actions_enabled ? "is-enabled" : "is-readonly"}`;

  renderRecentSignals(recentEvents);
  renderAttention(snapshot, unhealthySources, deliveryAttention);
  renderSources(sources);
  renderDelivery(sinks, Boolean(overview.actions_enabled));
  updateDocumentTitle();
}

function renderRecentSignals(events) {
  elements.recentSignalList.replaceChildren();
  if (!events.length) {
    elements.recentSignalList.append(emptyCompact("No signals have been collected yet."));
    return;
  }
  for (const event of events) {
    const button = createElement("button", "compact-event");
    button.type = "button";
    button.dataset.eventId = event.id;
    const avatar = createElement("span", "source-avatar", sourceInitial(event.source));
    const copy = createElement("span", "compact-event-copy");
    copy.append(
      createElement("strong", "", event.title || truncate(event.content, 70) || "Untitled signal"),
      createElement("span", "", `${event.source} · ${event.kind} · ${event.author || "Unknown author"}`),
    );
    const time = createElement("time", "", relativeTime(event.collected_at));
    time.dateTime = event.collected_at;
    button.append(avatar, copy, time);
    button.addEventListener("click", () => openEvent(event.id));
    elements.recentSignalList.append(button);
  }
}

function emptyCompact(message) {
  const item = createElement("div", "attention-item");
  item.append(createElement("span", "attention-dot"));
  const copy = createElement("div");
  copy.append(createElement("strong", "", message), createElement("p", "", "The console will update automatically when data arrives."));
  item.append(copy);
  return item;
}

function attentionItem(title, copy, className = "") {
  const item = createElement("div", `attention-item ${className}`.trim());
  item.append(createElement("span", "attention-dot"));
  const content = createElement("div");
  content.append(createElement("strong", "", title), createElement("p", "", copy));
  item.append(content);
  return item;
}

function renderAttention(snapshot, unhealthySources, deliveryAttention) {
  const items = [];
  if (snapshot.schema_status !== "current") {
    items.push(attentionItem("Schema mismatch", `Database schema is ${snapshot.schema_status}. Verify before continuing operations.`, "is-danger"));
  }
  for (const source of unhealthySources.slice(0, 3)) {
    items.push(attentionItem(source.source_key, source.last_error || `Source status is ${source.status}.`, healthClass(source.status) === "is-danger" ? "is-danger" : ""));
  }
  if (deliveryAttention > 0) {
    items.push(attentionItem("Delivery backlog", `${deliveryAttention} failed or dead rows need review.`, "is-danger"));
  }
  if (!items.length) {
    items.push(attentionItem("No active incidents", "Schema, sources, and delivery state look healthy.", "is-healthy"));
  }
  elements.attentionList.replaceChildren(...items);
}

function populateFilters(facets) {
  const selectedSource = elements.sourceFilter.value;
  const selectedKind = elements.kindFilter.value;
  replaceOptions(elements.sourceFilter, "All sources", facets.sources || [], selectedSource);
  replaceOptions(elements.kindFilter, "All kinds", facets.kinds || [], selectedKind);
}

function replaceOptions(select, label, values, selected) {
  const fragment = document.createDocumentFragment();
  const all = document.createElement("option");
  all.value = "";
  all.textContent = label;
  fragment.append(all);
  for (const value of values) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    fragment.append(option);
  }
  select.replaceChildren(fragment);
  if ([...select.options].some((option) => option.value === selected)) select.value = selected;
}

function currentEventParams() {
  const params = new URLSearchParams();
  params.set("limit", String(state.limit));
  params.set("offset", String(state.offset));
  const search = elements.signalSearch.value.trim();
  if (search) params.set("q", search);
  if (elements.sourceFilter.value) params.set("source", elements.sourceFilter.value);
  if (elements.kindFilter.value) params.set("kind", elements.kindFilter.value);
  return params;
}

async function loadEvents({ keepOffset = false, quiet = false } = {}) {
  if (!keepOffset) state.offset = 0;
  if (state.loadingEvents) return;
  state.loadingEvents = true;
  if (!quiet) renderEventLoading();
  try {
    const payload = await api(`/api/events?${currentEventParams().toString()}`);
    state.events = payload.items || [];
    state.eventTotal = Number(payload.total || 0);
    state.hasMore = Boolean(payload.has_more);
    state.offset = Number(payload.offset || 0);
    renderEvents();
    updateFilterUrl();
  } catch (error) {
    elements.signalList.replaceChildren();
    elements.signalEmpty.hidden = false;
    elements.signalEmpty.querySelector("h3").textContent = "Unable to load signals";
    elements.signalEmpty.querySelector("p").textContent = error.message;
    elements.pagination.hidden = true;
  } finally {
    state.loadingEvents = false;
  }
}

function renderEventLoading() {
  const shell = createElement("div", "signal-loading");
  for (let index = 0; index < 5; index += 1) shell.append(createElement("div", "loading-row"));
  elements.signalList.replaceChildren(shell);
  elements.signalEmpty.hidden = true;
  elements.pagination.hidden = true;
}

function renderEvents() {
  elements.signalList.replaceChildren();
  elements.resultCount.textContent = `${formatCount(state.eventTotal)} result${state.eventTotal === 1 ? "" : "s"}`;
  elements.signalEmpty.hidden = state.events.length > 0;
  elements.pagination.hidden = state.eventTotal === 0;

  for (const event of state.events) {
    const row = createElement("button", "signal-row");
    row.type = "button";
    row.dataset.eventId = event.id;

    const primary = createElement("span", "signal-primary");
    const titleLine = createElement("span", "signal-title-line");
    titleLine.append(
      createElement("span", "kind-chip", event.kind),
      createElement("span", "signal-title", event.title || truncate(event.content, 100) || "Untitled signal"),
    );
    primary.append(titleLine, createElement("span", "signal-preview", truncate(event.content, 150) || event.url));

    const source = createElement("span", "source-cell");
    source.append(createElement("span", "source-avatar", sourceInitial(event.source)));
    const sourceCopy = createElement("span");
    sourceCopy.append(createElement("strong", "", event.source), createElement("small", "", event.source_instance));
    source.append(sourceCopy);

    const time = createElement("time", "signal-time", relativeTime(event.collected_at));
    time.dateTime = event.collected_at;
    time.title = exactTime(event.collected_at);
    row.append(primary, source, time, createElement("span", "row-arrow", "›"));
    row.addEventListener("click", () => openEvent(event.id));
    elements.signalList.append(row);
  }

  const page = Math.floor(state.offset / state.limit) + 1;
  const pages = Math.max(1, Math.ceil(state.eventTotal / state.limit));
  elements.pageStatus.textContent = `Page ${page} of ${pages}`;
  elements.previousPage.disabled = state.offset === 0;
  elements.nextPage.disabled = !state.hasMore;
}

function updateFilterUrl() {
  const url = new URL(window.location.href);
  const search = elements.signalSearch.value.trim();
  for (const key of ["q", "source", "kind"]) url.searchParams.delete(key);
  if (search) url.searchParams.set("q", search);
  if (elements.sourceFilter.value) url.searchParams.set("source", elements.sourceFilter.value);
  if (elements.kindFilter.value) url.searchParams.set("kind", elements.kindFilter.value);
  history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
}

function restoreFiltersFromUrl() {
  const params = new URLSearchParams(window.location.search);
  elements.signalSearch.value = params.get("q") || "";
  elements.sourceFilter.dataset.pendingValue = params.get("source") || "";
  elements.kindFilter.dataset.pendingValue = params.get("kind") || "";
}

function applyPendingFilterValues() {
  for (const select of [elements.sourceFilter, elements.kindFilter]) {
    const value = select.dataset.pendingValue;
    if (value && [...select.options].some((option) => option.value === value)) select.value = value;
    delete select.dataset.pendingValue;
  }
}

function resetFilters() {
  elements.signalSearch.value = "";
  elements.sourceFilter.value = "";
  elements.kindFilter.value = "";
  loadEvents();
}

function renderSources(sources) {
  elements.sourceGrid.replaceChildren();
  elements.sourceEmpty.hidden = sources.length > 0;
  for (const source of sources) {
    const card = createElement("article", "source-card");
    const titleRow = createElement("div", "card-title-row");
    const title = createElement("div", "card-title");
    title.append(createElement("span", "source-avatar", sourceInitial(source.source_key)));
    const titleCopy = createElement("span", "card-title-copy");
    const [sourceName, ...instanceParts] = source.source_key.split(":");
    titleCopy.append(createElement("strong", "", sourceName), createElement("span", "", instanceParts.join(":") || "default"));
    title.append(titleCopy);
    titleRow.append(title, createElement("span", `status-chip ${healthClass(source.status)}`, source.status));

    const stats = createElement("div", "card-stat-grid");
    stats.append(
      cardStat("Runs", formatCount(source.total_runs)),
      cardStat("Events", formatCount(source.total_events)),
      cardStat("Failures", formatCount(source.consecutive_failures)),
    );
    const timeline = createElement("div", "card-timeline");
    timeline.append(
      createElement("p", "", `Last success: ${relativeTime(source.last_success_at)}`),
      createElement("p", "", `Last attempt: ${relativeTime(source.last_attempt_at)}`),
    );
    card.append(titleRow, stats, timeline);
    if (source.last_error) card.append(createElement("div", "card-error", source.last_error));
    elements.sourceGrid.append(card);
  }
}

function cardStat(label, value) {
  const item = createElement("div", "card-stat");
  item.append(createElement("span", "", label), createElement("strong", "", value));
  return item;
}

function renderDelivery(sinks, actionsEnabled) {
  elements.deliveryGrid.replaceChildren();
  elements.deliveryEmpty.hidden = sinks.length > 0;
  for (const sink of sinks) {
    const card = createElement("article", "delivery-card");
    const titleRow = createElement("div", "card-title-row");
    const title = createElement("div", "card-title");
    title.append(createElement("span", "source-avatar", sourceInitial(sink.sink_key)));
    const titleCopy = createElement("span", "card-title-copy");
    titleCopy.append(createElement("strong", "", sink.sink_key), createElement("span", "", `${formatCount(sink.attempts)} delivery attempts`));
    title.append(titleCopy);
    const status = sink.enabled ? (sink.dead || sink.failed ? "warning" : "healthy") : "disabled";
    titleRow.append(title, createElement("span", `status-chip ${healthClass(status)}`, status));

    const counts = createElement("div", "delivery-counts");
    counts.append(
      deliveryCount("Pending", sink.pending),
      deliveryCount("Failed", sink.failed),
      deliveryCount("Dead", sink.dead),
      deliveryCount("Delivered", sink.delivered),
    );
    card.append(titleRow, counts);
    if (sink.last_error) card.append(createElement("div", "card-error", sink.last_error));

    const actions = createElement("div", "delivery-actions");
    actions.append(createElement("small", "", sink.last_failure_at ? `Last failure ${relativeTime(sink.last_failure_at)}` : "No recorded delivery failure"));
    const retry = createElement("button", "button button-secondary", "Retry dead");
    retry.type = "button";
    retry.disabled = !actionsEnabled || Number(sink.dead) === 0;
    retry.title = !actionsEnabled ? "Start dashboard with --allow-actions to enable retries" : Number(sink.dead) === 0 ? "No dead deliveries" : "Queue dead rows for retry";
    retry.addEventListener("click", () => openRetryDialog(sink));
    actions.append(retry);
    card.append(actions);
    elements.deliveryGrid.append(card);
  }
}

function deliveryCount(label, value) {
  const item = createElement("div", "delivery-count");
  item.append(createElement("strong", "", formatCount(value)), createElement("span", "", label));
  return item;
}

async function openEvent(eventId) {
  elements.detailDrawer.classList.add("is-open");
  elements.detailDrawer.setAttribute("aria-hidden", "false");
  document.body.style.overflow = "hidden";
  elements.drawerKind.textContent = "Loading signal";
  elements.drawerTitle.textContent = "Signal details";
  const loading = createElement("div", "drawer-loading");
  loading.append(createElement("div", "loading-row"), createElement("div", "loading-row"), createElement("div", "loading-row"));
  elements.drawerBody.replaceChildren(loading);
  elements.openOriginal.setAttribute("aria-disabled", "true");
  elements.openOriginal.removeAttribute("href");
  try {
    const event = await api(`/api/events/${encodeURIComponent(eventId)}`);
    state.selectedEvent = event;
    renderEventDetail(event);
  } catch (error) {
    elements.drawerKind.textContent = "Unable to load";
    elements.drawerTitle.textContent = "Signal unavailable";
    const section = createElement("div", "empty-state");
    section.append(createElement("div", "empty-icon", "!"), createElement("h3", "", "Could not load this signal"), createElement("p", "", error.message));
    elements.drawerBody.replaceChildren(section);
  }
}

function renderEventDetail(event) {
  elements.drawerKind.textContent = `${event.source} · ${event.kind}`;
  elements.drawerTitle.textContent = event.title || "Untitled signal";
  const body = document.createDocumentFragment();

  const contentSection = createElement("section", "detail-section");
  contentSection.append(createElement("h3", "", "Content"), createElement("p", "detail-content", event.content || "No content."));
  body.append(contentSection);

  const contextSection = createElement("section", "detail-section");
  contextSection.append(createElement("h3", "", "Context"));
  const dl = createElement("dl", "detail-grid");
  appendDetail(dl, "Source", event.source_key);
  appendDetail(dl, "Author", event.author || "Unknown");
  appendDetail(dl, "Created", exactTime(event.created_at));
  appendDetail(dl, "Updated", exactTime(event.updated_at));
  appendDetail(dl, "Collected", exactTime(event.collected_at));
  appendDetail(dl, "Event ID", event.id);
  appendDetail(dl, "URL", event.url);
  contextSection.append(dl);
  body.append(contextSection);

  const metadataSection = createElement("section", "detail-section");
  metadataSection.append(createElement("h3", "", "Metadata"));
  const table = createElement("div", "metadata-table");
  const entries = Object.entries(event.metadata || {});
  if (!entries.length) {
    table.append(metadataRow("metadata", "No source-specific metadata"));
  } else {
    for (const [key, value] of entries) {
      table.append(metadataRow(key, typeof value === "string" ? value : JSON.stringify(value, null, 2)));
    }
  }
  metadataSection.append(table);
  body.append(metadataSection);

  elements.drawerBody.replaceChildren(body);
  const originalUrl = safeExternalUrl(event.url);
  if (originalUrl) {
    elements.openOriginal.href = originalUrl;
    elements.openOriginal.hidden = false;
    elements.openOriginal.removeAttribute("aria-disabled");
  } else {
    elements.openOriginal.hidden = true;
    elements.openOriginal.removeAttribute("href");
  }
}

function appendDetail(dl, label, value) {
  dl.append(createElement("dt", "", label), createElement("dd", "", value));
}

function metadataRow(key, value) {
  const row = createElement("div", "metadata-row");
  row.append(createElement("span", "", key), createElement("span", "", value));
  return row;
}

function closeDrawer() {
  elements.detailDrawer.classList.remove("is-open");
  elements.detailDrawer.setAttribute("aria-hidden", "true");
  document.body.style.overflow = "";
  state.selectedEvent = null;
}

async function copySelectedEvent() {
  if (!state.selectedEvent) return;
  const text = JSON.stringify(state.selectedEvent, null, 2);
  try {
    await navigator.clipboard.writeText(text);
    toast("Signal copied", "The normalized event JSON is on your clipboard.");
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.append(textarea);
    textarea.select();
    document.execCommand("copy");
    textarea.remove();
    toast("Signal copied", "The normalized event JSON is on your clipboard.");
  }
}

function openRetryDialog(sink) {
  state.retrySink = sink.sink_key;
  elements.retryDialogCopy.textContent = `Queue ${sink.dead} dead deliver${Number(sink.dead) === 1 ? "y" : "ies"} for another attempt on ${sink.sink_key}?`;
  elements.retryDialog.showModal();
}

async function retryDeadDeliveries(sinkKey) {
  try {
    const result = await api(`/api/sinks/${encodeURIComponent(sinkKey)}/retry-dead`, {
      method: "POST",
      headers: { "X-SignalKit-Action": "retry-dead" },
    });
    toast("Retry queued", `${result.queued} dead deliver${result.queued === 1 ? "y was" : "ies were"} returned to pending.`);
    await refreshAll({ quiet: true });
  } catch (error) {
    toast("Retry failed", error.message, true);
  } finally {
    state.retrySink = null;
  }
}

function toast(title, message, isError = false) {
  const item = createElement("div", `toast${isError ? " is-error" : ""}`);
  const copy = createElement("div");
  copy.append(createElement("strong", "", title), createElement("p", "", message));
  item.append(copy);
  elements.toastRegion.append(item);
  window.setTimeout(() => item.remove(), 4200);
}

function configureAutoRefresh() {
  if (state.refreshTimer) window.clearInterval(state.refreshTimer);
  state.refreshTimer = null;
  const seconds = Number(elements.refreshInterval.value || 0);
  localStorage.setItem("signalkit-refresh-interval", String(seconds));
  if (seconds > 0) {
    state.refreshTimer = window.setInterval(() => {
      if (!document.hidden && !state.loadingEvents) refreshAll({ quiet: true });
    }, seconds * 1000);
  }
}

function setupNavigation() {
  const navItems = [...document.querySelectorAll("[data-nav]")];
  const sections = navItems.map((item) => document.getElementById(item.dataset.nav)).filter(Boolean);
  const titles = {
    overview: "Operational overview",
    signals: "Signal explorer",
    sources: "Source health",
    delivery: "Delivery outbox",
  };
  const observer = new IntersectionObserver((entries) => {
    const visible = entries.filter((entry) => entry.isIntersecting).sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
    if (!visible) return;
    for (const item of navItems) item.classList.toggle("is-active", item.dataset.nav === visible.target.id);
    elements.pageTitle.textContent = titles[visible.target.id] || "SignalKit Stream";
  }, { rootMargin: "-25% 0px -65% 0px", threshold: [0, 0.1, 0.5] });
  for (const section of sections) observer.observe(section);
}

function setupEvents() {
  elements.refreshButton.addEventListener("click", () => refreshAll());
  elements.noticeRetry.addEventListener("click", () => refreshAll());
  elements.refreshInterval.addEventListener("change", configureAutoRefresh);
  elements.clearFilters.addEventListener("click", resetFilters);
  elements.emptyClearFilters.addEventListener("click", resetFilters);
  elements.sourceFilter.addEventListener("change", () => loadEvents());
  elements.kindFilter.addEventListener("change", () => loadEvents());
  elements.previousPage.addEventListener("click", () => {
    state.offset = Math.max(0, state.offset - state.limit);
    loadEvents({ keepOffset: true });
    document.getElementById("signals").scrollIntoView({ behavior: "smooth" });
  });
  elements.nextPage.addEventListener("click", () => {
    if (!state.hasMore) return;
    state.offset += state.limit;
    loadEvents({ keepOffset: true });
    document.getElementById("signals").scrollIntoView({ behavior: "smooth" });
  });

  let debounceTimer = null;
  elements.signalSearch.addEventListener("input", () => {
    window.clearTimeout(debounceTimer);
    debounceTimer = window.setTimeout(() => loadEvents(), 280);
  });

  for (const close of document.querySelectorAll("[data-close-drawer]")) close.addEventListener("click", closeDrawer);
  elements.copySignalJson.addEventListener("click", copySelectedEvent);
  elements.retryDialog.addEventListener("close", () => {
    if (elements.retryDialog.returnValue === "confirm" && state.retrySink) retryDeadDeliveries(state.retrySink);
    else state.retrySink = null;
  });

  document.addEventListener("keydown", (event) => {
    const typing = ["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName);
    if (event.key === "/" && !typing) {
      event.preventDefault();
      elements.signalSearch.focus();
      document.getElementById("signals").scrollIntoView({ behavior: "smooth" });
    }
    if (event.key.toLowerCase() === "r" && !typing && !event.metaKey && !event.ctrlKey) refreshAll();
    if (event.key === "Escape" && elements.detailDrawer.classList.contains("is-open")) closeDrawer();
  });
}

async function bootstrap() {
  restoreFiltersFromUrl();
  const savedInterval = localStorage.getItem("signalkit-refresh-interval");
  if (savedInterval && [...elements.refreshInterval.options].some((option) => option.value === savedInterval)) {
    elements.refreshInterval.value = savedInterval;
  }
  setupNavigation();
  setupEvents();
  configureAutoRefresh();
  await refreshAll();
  applyPendingFilterValues();
  await loadEvents();
}

bootstrap();
