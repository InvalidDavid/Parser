import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import MetricCard from '@/components/MetricCard.vue'
import SourceCard from '@/components/SourceCard.vue'

import { formatDate, formatNumber } from '@/lib/format'
import { sampleData } from '@/sample-data'
import type { SourceDataset, SourceItem, SourceStatus } from '@/types'

const dataset = ref<SourceDataset>(sampleData)
const loading = ref(true)
const error = ref<string | null>(null)

const rawQuery = ref('')
const query = ref('')
const status = ref<'all' | SourceStatus>('all')
const language = ref('all')
const contentType = ref('all')
const nsfw = ref<'all' | 'safe' | 'nsfw'>('all')
const sort = ref<'title' | 'language' | 'status' | 'domains'>('status')
const view = ref<'grid' | 'list'>('grid')
const isScrolled = ref(false)

const LANGUAGE_NAMES: Record<string, string> = {
  en: 'English',
  de: 'German',
  ru: 'Russian',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  pt: 'Portuguese',
  tr: 'Turkish',
  vi: 'Vietnamese',
  id: 'Indonesian',
  th: 'Thai',
  ar: 'Arabic',
  pl: 'Polish',
  uk: 'Ukrainian',
  ja: 'Japanese',
  ko: 'Korean',
  zh: 'Chinese',
}

const statusOrder: Record<SourceStatus, number> = {
  working: 0,
  blocked: 1,
  unknown: 2,
  broken: 3,
}

let searchDebounce: number | undefined

watch(rawQuery, (value) => {
  window.clearTimeout(searchDebounce)
  searchDebounce = window.setTimeout(() => {
    query.value = value.trim().toLowerCase()
  }, 120)
})

onBeforeUnmount(() => {
  window.clearTimeout(searchDebounce)
  window.removeEventListener('scroll', onScroll)
})

function withSearchText(source: SourceItem): SourceItem {
  const languageName = source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase()

  return {
    ...source,
    languageName,
    searchText: [
      source.title,
      source.key,
      source.language,
      languageName,
      source.engine ?? '',
      source.contentType ?? '',
      source.path,
      source.brokenReason ?? '',
      ...(source.domains ?? []),
    ]
      .join(' ')
      .toLowerCase(),
  }
}

function normalizeDataset(next: SourceDataset): SourceDataset {
  return {
    ...next,
    sources: next.sources.map(withSearchText),
  }
}

const languages = computed(() => {
  const values = new Map<string, string>()

  for (const source of dataset.value.sources) {
    if (!source.language) continue
    values.set(source.language, source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase())
  }

  return [
    { value: 'all', label: 'All languages' },
    ...Array.from(values.entries())
      .sort((a, b) => a[1].localeCompare(b[1]))
      .map(([value, label]) => ({ value, label })),
  ]
})

const contentTypes = computed(() => {
  const values = new Set(dataset.value.sources.map((source) => source.contentType ?? 'MANGA'))
  return ['all', ...Array.from(values).sort((a, b) => a.localeCompare(b))]
})

const filteredSources = computed<SourceItem[]>(() => {
  const filtered = dataset.value.sources.filter((source) => {
    const matchesStatus = status.value === 'all' || source.health.status === status.value
    const matchesLanguage = language.value === 'all' || source.language === language.value
    const sourceType = source.contentType ?? 'MANGA'
    const matchesType = contentType.value === 'all' || sourceType === contentType.value
    const matchesNsfw =
      nsfw.value === 'all' ||
      (nsfw.value === 'nsfw' && !!source.nsfw) ||
      (nsfw.value === 'safe' && !source.nsfw)

    const matchesQuery = !query.value || (source.searchText?.includes(query.value) ?? false)

    return matchesStatus && matchesLanguage && matchesType && matchesNsfw && matchesQuery
  })

  return filtered.sort((left, right) => {
    switch (sort.value) {
      case 'title':
        return left.title.localeCompare(right.title)
      case 'language':
        return (left.languageName || left.language).localeCompare(right.languageName || right.language) ||
          left.title.localeCompare(right.title)
      case 'domains':
        return right.domains.length - left.domains.length || left.title.localeCompare(right.title)
      case 'status':
      default:
        return statusOrder[left.health.status] - statusOrder[right.health.status] ||
          left.title.localeCompare(right.title)
    }
  })
})

function applyStatus(next: 'all' | SourceStatus) {
  status.value = next
}

function resetFilters() {
  rawQuery.value = ''
  query.value = ''
  status.value = 'all'
  language.value = 'all'
  contentType.value = 'all'
  nsfw.value = 'all'
  sort.value = 'status'
}

function onScroll() {
  isScrolled.value = window.scrollY > 20
}

onMounted(async () => {
  window.addEventListener('scroll', onScroll, { passive: true })
  onScroll()

  try {
    const response = await fetch(`${import.meta.env.BASE_URL}data/sources.json`, { cache: 'force-cache' })
    if (!response.ok) {
      throw new Error(`Dataset request failed with ${response.status}`)
    }

    const liveData = (await response.json()) as SourceDataset
    if (liveData.sources.length > 0) {
      dataset.value = normalizeDataset(liveData)
    } else {
      dataset.value = normalizeDataset(sampleData)
    }
  } catch (reason) {
    dataset.value = normalizeDataset(sampleData)
    error.value = reason instanceof Error ? reason.message : 'Unknown data loading error'
  } finally {
    loading.value = false
  }
})
