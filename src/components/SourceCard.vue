<script setup lang="ts">
import { computed } from "vue";

import { formatContentType } from "@/lib/catalog";
import type { SourceItem, SourceStatus } from "@/types";

import StatusPill from "./StatusPill.vue";

const props = defineProps<{
  source: SourceItem;
  compact?: boolean;
}>();

const FALLBACK_STATUS_TEXT: Record<SourceStatus, string> = {
  working: "Functional",
  broken: "Content not found or removed",
  blocked: "Access denied",
  unknown: "Network error",
};

const MAX_VISIBLE_STATUS_LENGTH = 180;

const featureChecks = computed(() => {
  const checks = props.source.health.checks ?? {};

  return [
    ["list", "List"],
    ["search", "Search"],
    ["details", "Manga details"],
    ["chapters", "Chapters"],
    ["images", "Images"],
  ]
    .map(([key, label]) => {
      const check = checks[key as keyof typeof checks];

      if (!check) {
        return null;
      }

      return {
        key,
        label,
        status: check.status,
        reason: normalizeCardText(check.reason),
        count: check.count,
        details: normalizeCardText(check.details),
      };
    })
    .filter(Boolean);
});

function normalizeCardText(value: string | null | undefined): string {
  return String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();
}

function truncateCardText(value: string): string {
  if (value.length <= MAX_VISIBLE_STATUS_LENGTH) {
    return value;
  }

  return `${value.slice(0, MAX_VISIBLE_STATUS_LENGTH - 1).trim()}…`;
}

function statusFallback(status: SourceStatus | undefined): string {
  return (
    FALLBACK_STATUS_TEXT[status ?? "unknown"] ?? FALLBACK_STATUS_TEXT.unknown
  );
}

const websiteUrl = computed(() => {
  const domain = props.source.domains[0];

  return domain ? `https://${domain}` : null;
});

const visibleDomains = computed(() => {
  return props.source.domains.slice(0, props.compact ? 2 : 4);
});

const hiddenDomainCount = computed(() => {
  return Math.max(props.source.domains.length - visibleDomains.value.length, 0);
});

const displayLanguage = computed(() => {
  return props.source.languageName || props.source.language.toUpperCase();
});

const contentTypeLabel = computed(() => {
  return formatContentType(props.source.contentType);
});

const appStatusText = computed(() => {
  const reason = normalizeCardText(props.source.health.reason);
  const fallback = statusFallback(props.source.health.status);

  return truncateCardText(reason || fallback);
});

const appStatusTitle = computed(() => {
  const parts = [
    normalizeCardText(props.source.health.reason),
    normalizeCardText(props.source.health.details),
  ].filter(Boolean);

  return parts.length ? parts.join(" — ") : appStatusText.value;
});

const appStatusMeta = computed(() => {
  const parts: string[] = [];

  if (props.source.health.httpStatus) {
    parts.push(`HTTP ${props.source.health.httpStatus}`);
  }

  if (typeof props.source.health.latencyMs === "number") {
    parts.push(`${props.source.health.latencyMs} ms`);
  }

  if (props.source.health.checkedAt) {
    const checkedAt = new Date(props.source.health.checkedAt);

    if (!Number.isNaN(checkedAt.getTime())) {
      parts.push(`checked ${checkedAt.toLocaleDateString()}`);
    }
  }

  return parts.join(" · ");
});

const appStatusClass = computed(() => {
  return `source-card__app-status--${props.source.health.status || "unknown"}`;
});
</script>

<template>
  <article :class="['source-card', { 'source-card--compact': compact }]">
    <div class="source-card__top">
      <div class="source-card__title-wrap">
        <p class="source-card__eyebrow">{{ contentTypeLabel }}</p>

        <h3>{{ source.title }}</h3>

        <p v-if="!compact" class="source-card__path">{{ source.path }}</p>
      </div>

      <div class="source-card__badges">
        <StatusPill :status="source.health.status" />

        <span
          v-if="source.nsfw"
          class="source-card__tag source-card__tag--nsfw"
        >
          NSFW
        </span>
      </div>
    </div>

    <div class="source-card__meta">
      <span>{{ displayLanguage }}</span>
      <span>{{ source.engine ?? "Unknown engine" }}</span>
      <span>{{ source.key }}</span>
      <span>
        {{ source.domains.length }} domain<span
          v-if="source.domains.length !== 1"
          >s</span
        >
      </span>
    </div>

    <div v-if="featureChecks.length && !compact" class="source-card__checks">
      <div
        v-for="check in featureChecks"
        :key="check.key"
        :class="['source-card__check', `source-card__check--${check.status}`]"
        :title="check.details || check.reason"
      >
        <span class="source-card__check-name">{{ check.label }}</span>
        <span class="source-card__check-status">{{ check.status }}</span>
        <span class="source-card__check-reason">
          {{ check.reason
          }}<template v-if="typeof check.count === 'number'">
            · {{ check.count }}</template
          >
        </span>
      </div>
    </div>

    <section
      v-if="appStatusText"
      :class="['source-card__app-status', appStatusClass]"
      :title="appStatusTitle"
      aria-label="App-like availability status"
    >
      <p class="source-card__app-status-label">App status</p>
      <p class="source-card__app-status-text">{{ appStatusText }}</p>
      <p v-if="appStatusMeta && !compact" class="source-card__app-status-meta">
        {{ appStatusMeta }}
      </p>
    </section>

    <div v-if="visibleDomains.length && !compact" class="domain-list">
      <span v-for="domain in visibleDomains" :key="domain" class="domain-chip">
        {{ domain }}
      </span>

      <span v-if="hiddenDomainCount > 0" class="domain-chip domain-chip--more">
        +{{ hiddenDomainCount }} more
      </span>
    </div>

    <div class="source-card__footer">
      <div class="source-card__actions">
        <a
          v-if="websiteUrl"
          class="button button--website button--small"
          :href="websiteUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          Website
        </a>

        <a
          class="button button--ghost button--small"
          :href="source.repoUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          File
        </a>

        <a
          v-if="!compact"
          class="button button--ghost button--small"
          :href="source.rawUrl"
          target="_blank"
          rel="noreferrer noopener"
        >
          Raw
        </a>
      </div>
    </div>
  </article>
</template>
