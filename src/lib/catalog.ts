import type { SourceDataset, SourceItem, SourceStatus } from "@/types";

export const LANGUAGE_NAMES: Record<string, string> = {
  en: "English",
  de: "German",
  ru: "Russian",
  fr: "French",
  es: "Spanish",
  it: "Italian",
  pt: "Portuguese",
  tr: "Turkish",
  vi: "Vietnamese",
  id: "Indonesian",
  th: "Thai",
  ar: "Arabic",
  pl: "Polish",
  uk: "Ukrainian",
  ja: "Japanese",
  ko: "Korean",
  zh: "Chinese",
  be: "Belarusian",
  cs: "Czech",
  multi: "Multiple",
};

function normalizeStatus(value: unknown): SourceStatus {
  switch (value) {
    case "working":
    case "broken":
    case "blocked":
    case "unknown":
      return value;
    default:
      return "unknown";
  }
}

export function formatContentType(value?: string | null): string {
  const normalized = (value ?? "MANGA").trim().toUpperCase();

  switch (normalized) {
    case "MANGA":
      return "Manga";
    case "MANHWA":
      return "Manhwa";
    case "MANHUA":
      return "Manhua";
    case "COMICS":
      return "Comics";
    case "NOVEL":
      return "Novel";
    default:
      return normalized.charAt(0) + normalized.slice(1).toLowerCase();
  }
}

export function getLanguageLabel(
  source: Pick<SourceItem, "language" | "languageName">,
): string {
  return (
    source.languageName ||
    LANGUAGE_NAMES[source.language] ||
    source.language.toUpperCase()
  );
}

export function buildSearchText(source: SourceItem): string {
  return [
    source.title,
    source.key,
    source.language,
    getLanguageLabel(source),
    source.engine ?? "",
    source.contentType ?? "",
    source.path,
    source.brokenReason ?? "",
    ...(source.domains ?? []),
  ]
    .join(" ")
    .trim()
    .toLowerCase();
}

export function normalizeDataset(next: SourceDataset): SourceDataset {
  const sources: SourceItem[] = next.sources.map((source) => {
    const health = {
      status: normalizeStatus(source.health?.status),
      checkedAt: source.health?.checkedAt ?? null,
      latencyMs: source.health?.latencyMs ?? null,
      httpStatus: source.health?.httpStatus ?? null,
      finalUrl: source.health?.finalUrl ?? null,
      reason: source.health?.reason ?? null,
    };

    return {
      ...source,
      domains: Array.isArray(source.domains) ? source.domains : [],
      health,
      languageName: getLanguageLabel(source),
      searchText: source.searchText || buildSearchText(source),
    };
  });

  return {
    ...next,
    generatedBy: next.generatedBy ?? "Static bundle",
    byLocale: next.byLocale ?? {},
    byType: next.byType ?? {},
    duplicatesSkipped: next.duplicatesSkipped ?? [],
    duplicatesDetected: next.duplicatesDetected ?? [],
    summary: {
      total: next.summary.total ?? sources.length,
      working:
        next.summary.working ??
        sources.filter((source) => source.health.status === "working").length,
      broken:
        next.summary.broken ??
        sources.filter((source) => source.health.status === "broken").length,
      blocked:
        next.summary.blocked ??
        sources.filter((source) => source.health.status === "blocked").length,
      unknown:
        next.summary.unknown ??
        sources.filter((source) => source.health.status === "unknown").length,
      nsfw:
        next.summary.nsfw ?? sources.filter((source) => !!source.nsfw).length,
    },
    sources,
  };
}

export function isSourceDataset(value: unknown): value is SourceDataset {
  if (!value || typeof value !== "object") return false;

  const dataset = value as Partial<SourceDataset>;

  return (
    Array.isArray(dataset.sources) &&
    typeof dataset.disclaimer === "string" &&
    !!dataset.summary &&
    typeof dataset.summary === "object" &&
    !!dataset.sourceRepo &&
    typeof dataset.sourceRepo === "object"
  );
}

export function parseDataset(value: unknown): SourceDataset | null {
  if (!isSourceDataset(value)) return null;
  return normalizeDataset(value);
}
