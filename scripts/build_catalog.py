#!/usr/bin/env python3
from __future__ import annotations
import shutil
import subprocess
import tempfile
import argparse
import concurrent.futures
import json
import os
import random
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode, urlparse

ANN_RE = re.compile(
    r'@MangaSourceParser\s*\(\s*'
    r'"([^"]+)"\s*,\s*'
    r'"([^"]+)"\s*,?\s*'
    r'(?:(?:"([^"]*)")\s*,?\s*)?'
    r'(?:ContentType\.([A-Z_]+)\s*,?\s*)?'
    r'\)',
    re.DOTALL,
)

BROKEN_RE = re.compile(r'@Broken(?:\(\s*"([^"]*)"\s*\))?')

CLASS_DECL_RE = re.compile(
    r'\b(?:internal|public|private|protected)?\s*'
    r'(?:abstract|open|sealed|data)?\s*class\s+'
    r'([A-Za-z0-9_]+)'
    r'(?:\s*<[^>{}]*>)?'
    r'(?:\s*\([^)]*\))?'
    r'\s*:\s*([A-Za-z0-9_]+)'
    r'(?:\((.*?)\))?',
    re.DOTALL,
)

CLASS_NAME_RE = re.compile(
    r'\b(?:internal|public|private|protected)?\s*'
    r'(?:abstract|open|sealed|data)?\s*class\s+([A-Za-z0-9_]+)'
)

SOURCE_DOMAIN_EXPR_RE = re.compile(
    r'MangaParserSource\.[A-Z0-9_]+\s*,\s*([^,\n)]+)'
)

CFG_DOMAIN_BLOCK_RE = re.compile(r'ConfigKey\.Domain\((.*?)\)', re.DOTALL)

DOMAIN_PROPERTY_RE = re.compile(
    r'(?:override\s+)?(?:val|var)\s+[A-Za-z0-9_]*domain[A-Za-z0-9_]*\s*=\s*([^\n;]+)',
    re.IGNORECASE,
)

NAMED_DOMAIN_ARG_RE = re.compile(r'\bdomain\s*=\s*([^,\n)]+)', re.IGNORECASE)

CONST_RE = re.compile(
    r'(?:private\s+|protected\s+|internal\s+|public\s+)?'
    r'(?:const\s+)?val\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]+)"'
)

STRING_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"')

IDENTIFIER_RE = re.compile(r'\b([A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*)\b')

LANGUAGE_NAMES: dict[str, str] = {
    'en': 'English',
    'de': 'German',
    'ru': 'Russian',
    'fr': 'French',
    'es': 'Spanish',
    'it': 'Italian',
    'pt': 'Portuguese',
    'tr': 'Turkish',
    'vi': 'Vietnamese',
    'id': 'Indonesian',
    'th': 'Thai',
    'ar': 'Arabic',
    'pl': 'Polish',
    'uk': 'Ukrainian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh': 'Chinese',
    'be': 'Belarusian',
    'cs': 'Czech',
    'multi': 'Multiple',
}

VALID_HEALTH_STATUSES = {'working', 'broken', 'blocked', 'unknown'}

FETCH_ENABLED = os.environ.get('SOURCE_FETCH_ENABLED', '1') != '0'
FETCH_TIMEOUT_SECONDS = float(os.environ.get('SOURCE_FETCH_TIMEOUT', '7'))
FETCH_MAX_WORKERS = max(1, int(os.environ.get('SOURCE_FETCH_WORKERS', '12')))
FETCH_MAX_DOMAINS_PER_SOURCE = max(1, int(os.environ.get('SOURCE_FETCH_MAX_DOMAINS_PER_SOURCE', '1')))
FETCH_RETRIES = max(0, int(os.environ.get('SOURCE_FETCH_RETRIES', '2')))
FETCH_BACKOFF_SECONDS = float(os.environ.get('SOURCE_FETCH_BACKOFF', '0.8'))

FETCH_USER_AGENT = os.environ.get(
    'SOURCE_FETCH_USER_AGENT',
    'Mozilla/5.0 (Linux; Android 10; Usagi Parser Check) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36',
)

APP_CHECK_MAX_BYTES = int(os.environ.get('SOURCE_APP_CHECK_MAX_BYTES', '262144'))

RATE_LIMIT_DELAY_SECONDS = float(os.environ.get('SOURCE_RATE_LIMIT_DELAY', '0.45'))
RATE_LIMIT_JITTER_SECONDS = float(os.environ.get('SOURCE_RATE_LIMIT_JITTER', '0.25'))

RETRY_HTTP_STATUSES = {429, 500, 502, 503, 504}

MAX_REASON_LENGTH = 180
MAX_DETAILS_LENGTH = 420

PARSER_RUNTIME_ENABLED = os.environ.get('PARSER_RUNTIME_ENABLED', '1') != '0'
PARSER_RUNTIME_DIR = Path(os.environ.get('PARSER_RUNTIME_DIR', 'tools/parser-checker'))
PARSER_RUNTIME_GRADLE = os.environ.get('PARSER_RUNTIME_GRADLE', 'gradle')

_domain_last_request: dict[str, float] = {}
_domain_rate_lock = threading.Lock()


@dataclass(slots=True)
class KotlinClassInfo:
    name: str | None
    parent: str | None
    super_args: str
    constants: dict[str, str]
    text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Build public/data/sources.json from parser source files.'
    )
    parser.add_argument(
        '--repo-dir',
        default=os.environ.get('KOTATSU_REPO_DIR', '.cache/kotatsu-parsers'),
        help='Local path to a checked out parser repository.',
    )
    parser.add_argument(
        '--output',
        default=os.environ.get('OUTPUT_PATH', 'public/data/sources.json'),
        help='Where to write the generated dataset.',
    )
    parser.add_argument('--owner', default=os.environ.get('KOTATSU_OWNER', 'YakaTeam'))
    parser.add_argument('--repo', default=os.environ.get('KOTATSU_REPO', 'kotatsu-parsers'))
    parser.add_argument('--branch', default=os.environ.get('KOTATSU_BRANCH', 'master'))
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_text(value: object, max_length: int = MAX_REASON_LENGTH) -> str:
    text = str(value or '').replace('\x00', ' ')
    text = re.sub(r'\s+', ' ', text).strip()

    if not text:
        return ''

    if len(text) <= max_length:
        return text

    return text[: max_length - 1].rstrip() + '…'


def unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []

    for value in values:
        normalized = value.strip().lower().strip('.')
        if not is_probable_domain(normalized) or normalized in seen:
            continue

        seen.add(normalized)
        output.append(normalized)

    return output


def is_probable_domain(value: str) -> bool:
    if not value:
        return False

    value = value.strip().lower()

    if (
        '.' not in value
        or ' ' in value
        or value.endswith('.kt')
        or 'org.koitharu' in value
        or '/' in value
        or value.startswith('http')
        or value.startswith('.')
        or value.endswith('.')
    ):
        return False

    return bool(re.fullmatch(r'[a-z0-9.-]+\.[a-z]{2,63}', value))


def parse_quoted_domains(fragment: str) -> list[str]:
    return [
        match.group(1)
        for match in STRING_RE.finditer(fragment)
        if is_probable_domain(match.group(1))
    ]


def resolve_identifier(token: str, constants: dict[str, str]) -> str | None:
    direct = constants.get(token)

    if direct and is_probable_domain(direct):
        return direct

    tail = token.rsplit('.', 1)[-1]
    resolved = constants.get(tail)

    if resolved and is_probable_domain(resolved):
        return resolved

    return None


def parse_domain_candidates(fragment: str, constants: dict[str, str]) -> list[str]:
    domains = parse_quoted_domains(fragment)

    for match in IDENTIFIER_RE.finditer(fragment):
        resolved = resolve_identifier(match.group(1), constants)
        if resolved:
            domains.append(resolved)

    return unique(domains)


def normalize_language_name(language: str) -> str:
    return LANGUAGE_NAMES.get(language, language.upper())


def infer_nsfw(text: str, title: str, path: str, domains: list[str]) -> bool:
    haystack = ' '.join([text, title, path, *domains]).lower()
    nsfw_markers = ['nsfw', '18+', 'adult', 'hentai', 'ecchi', 'porn', 'xxx']
    return any(marker in haystack for marker in nsfw_markers)


def build_search_text(
    *,
    title: str,
    key: str,
    language: str,
    language_name: str,
    engine: str | None,
    content_type: str,
    path: str,
    broken_reason: str | None,
    domains: list[str],
) -> str:
    return ' '.join(
        [
            title,
            key,
            language,
            language_name,
            engine or '',
            content_type or '',
            path,
            broken_reason or '',
            *domains,
        ]
    ).strip().lower()


def parse_class_info(file_path: Path) -> KotlinClassInfo:
    text = file_path.read_text(encoding='utf-8', errors='replace')
    class_match = CLASS_DECL_RE.search(text)
    name_match = CLASS_NAME_RE.search(text)

    constants: dict[str, str] = {}

    for match in CONST_RE.finditer(text):
        constant_value = match.group(2).strip()
        if is_probable_domain(constant_value):
            constants[match.group(1)] = constant_value.lower()

    return KotlinClassInfo(
        name=name_match.group(1) if name_match else None,
        parent=class_match.group(2) if class_match else None,
        super_args=class_match.group(3) if class_match and class_match.group(3) else '',
        constants=constants,
        text=text,
    )


def extract_domains_from_text(
    text: str,
    constants: dict[str, str],
    super_args: str = '',
) -> list[str]:
    domains: list[str] = []

    for match in SOURCE_DOMAIN_EXPR_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in CFG_DOMAIN_BLOCK_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in DOMAIN_PROPERTY_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in NAMED_DOMAIN_ARG_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    if super_args:
        domains.extend(parse_domain_candidates(super_args, constants))

    return unique(domains)


def collect_domains(
    class_name: str | None,
    class_index: dict[str, KotlinClassInfo],
    seen: set[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    if not class_name:
        return [], {}

    if seen is None:
        seen = set()

    if class_name in seen:
        return [], {}

    seen.add(class_name)

    info = class_index.get(class_name)

    if not info:
        return [], {}

    parent_domains, parent_constants = collect_domains(info.parent, class_index, seen)
    merged_constants = {**parent_constants, **info.constants}
    own_domains = extract_domains_from_text(info.text, merged_constants, info.super_args)

    return unique([*own_domains, *parent_domains]), merged_constants


def build_health(
    status: str,
    reason: str | None,
    *,
    checked_at: str | None = None,
    latency_ms: int | None = None,
    http_status: int | None = None,
    final_url: str | None = None,
    details: str | None = None,
) -> dict[str, object | None]:
    safe_status = status if status in VALID_HEALTH_STATUSES else 'unknown'
    safe_reason = clean_text(reason or 'An error occurred', MAX_REASON_LENGTH)
    safe_details = clean_text(details or '', MAX_DETAILS_LENGTH) or None

    return {
        'status': safe_status,
        'reason': safe_reason,
        'checkedAt': checked_at,
        'latencyMs': latency_ms,
        'httpStatus': http_status,
        'finalUrl': final_url,
        'details': safe_details,
    }


def map_http_error_like_usagi(status_code: int) -> tuple[str, str]:
    if status_code in (404, 410):
        return 'broken', 'Content not found or removed'

    if status_code == 401:
        return 'blocked', 'Authorization required'

    if status_code == 403:
        return 'blocked', 'Access denied (403)'

    if status_code == 429:
        return 'blocked', 'Too many requests. Try again later'

    if status_code == 451:
        return 'blocked', 'Access denied by server or region'

    if status_code == 504:
        return 'unknown', 'Network is not available'

    if 500 <= status_code <= 599:
        return 'unknown', f'Server side error ({status_code}). Please try again later'

    return 'unknown', f'HTTP error ({status_code})'


def is_safe_final_url(value: str | None) -> str | None:
    if not value:
        return None

    try:
        parsed = urlparse(value)
    except ValueError:
        return None

    if parsed.scheme not in {'http', 'https'}:
        return None

    if not parsed.netloc:
        return None

    return value


def build_source_url(domain: str, path: str = '', scheme: str = 'https') -> str:
    clean_domain = domain.strip().lower().strip('/')
    clean_path = path.strip('/')

    if clean_path:
        return f'{scheme}://{clean_domain}/{clean_path}/'

    return f'{scheme}://{clean_domain}/'


def rate_limit_url(url: str) -> None:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if not domain:
        return

    with _domain_rate_lock:
        now = time.monotonic()
        last = _domain_last_request.get(domain, 0.0)
        delay = RATE_LIMIT_DELAY_SECONDS + random.uniform(0, RATE_LIMIT_JITTER_SECONDS)
        wait_for = delay - (now - last)

        if wait_for > 0:
            time.sleep(wait_for)

        _domain_last_request[domain] = time.monotonic()


def retry_sleep(attempt: int, retry_after: str | None = None) -> None:
    if retry_after:
        try:
            seconds = min(float(retry_after), 8.0)
            if seconds > 0:
                time.sleep(seconds)
                return
        except ValueError:
            pass

    backoff = FETCH_BACKOFF_SECONDS * (2 ** attempt)
    jitter = random.uniform(0, RATE_LIMIT_JITTER_SECONDS)
    time.sleep(min(backoff + jitter, 8.0))


def read_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        raw = exc.read(APP_CHECK_MAX_BYTES)
    except Exception:
        return ''

    try:
        return raw.decode('utf-8', errors='replace')
    except Exception:
        return ''


def html_looks_blocked_or_protected(html: str) -> str | None:
    lowered = html.lower()

    if not lowered:
        return None

    captcha_markers = (
        'captcha',
        'g-recaptcha',
        'hcaptcha',
        'cf-turnstile',
        'verify you are human',
        'human verification',
    )

    cloudflare_markers = (
        'cf-browser-verification',
        'checking your browser',
        'cf-challenge',
        'cloudflare challenge',
        'just a moment',
        'attention required',
        'ray id',
        'turnstile',
    )

    if any(marker in lowered for marker in captcha_markers):
        return 'Additional action required'

    if any(marker in lowered for marker in cloudflare_markers):
        return 'Additional action required'

    if 'enable javascript' in lowered or 'javascript is disabled' in lowered:
        return 'Additional action required'

    if 'access denied' in lowered or 'error 1020' in lowered:
        return 'Access denied (403)'

    return None


def map_http_error_with_body_like_usagi(status_code: int, body: str = '') -> tuple[str, str]:
    protected_reason = html_looks_blocked_or_protected(body)

    if protected_reason:
        return 'blocked', protected_reason

    return map_http_error_like_usagi(status_code)


def request_html_once(
    url: str,
    *,
    method: str = 'GET',
    form: dict[str, str] | None = None,
) -> tuple[dict[str, object | None], str]:
    body: bytes | None = None

    headers = {
        'User-Agent': FETCH_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.8',
        'Cache-Control': 'no-cache',
    }

    if form is not None:
        body = urlencode(form).encode('utf-8')
        headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
        headers['X-Requested-With'] = 'XMLHttpRequest'
        method = 'POST'

    last_health: dict[str, object | None] | None = None

    for attempt in range(FETCH_RETRIES + 1):
        started = time.monotonic()

        try:
            rate_limit_url(url)

            request = urllib.request.Request(
                url,
                data=body,
                headers=headers,
                method=method,
            )

            with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
                latency_ms = int((time.monotonic() - started) * 1000)
                status_code = int(response.getcode())
                final_url = is_safe_final_url(response.geturl())

                raw = response.read(APP_CHECK_MAX_BYTES)
                charset = response.headers.get_content_charset() or 'utf-8'
                html = raw.decode(charset, errors='replace')

                protected_reason = html_looks_blocked_or_protected(html)

                if protected_reason:
                    return build_health(
                        'blocked',
                        protected_reason,
                        checked_at=utc_now(),
                        latency_ms=latency_ms,
                        http_status=status_code,
                        final_url=final_url or url,
                        details=f'{method} {url} responded, but protection was detected.',
                    ), ''

                if 200 <= status_code < 400:
                    return build_health(
                        'working',
                        'Reachable',
                        checked_at=utc_now(),
                        latency_ms=latency_ms,
                        http_status=status_code,
                        final_url=final_url or url,
                        details=f'{method} {url} returned HTML for app-like parser simulation.',
                    ), html

                status, reason = map_http_error_with_body_like_usagi(status_code, html)

                last_health = build_health(
                    status,
                    reason,
                    checked_at=utc_now(),
                    latency_ms=latency_ms,
                    http_status=status_code,
                    final_url=final_url or url,
                    details=f'{method} {url} returned HTTP {status_code}',
                )

                if status_code in RETRY_HTTP_STATUSES and attempt < FETCH_RETRIES:
                    retry_sleep(attempt, response.headers.get('Retry-After'))
                    continue

                return last_health, ''

        except urllib.error.HTTPError as exc:
            latency_ms = int((time.monotonic() - started) * 1000)
            html = read_error_body(exc)
            status, reason = map_http_error_with_body_like_usagi(exc.code, html)

            last_health = build_health(
                status,
                reason,
                checked_at=utc_now(),
                latency_ms=latency_ms,
                http_status=exc.code,
                final_url=url,
                details=f'{method} {url} returned HTTP {exc.code}',
            )

            if exc.code in RETRY_HTTP_STATUSES and attempt < FETCH_RETRIES:
                retry_sleep(attempt, exc.headers.get('Retry-After'))
                continue

            return last_health, ''

        except ssl.SSLError as exc:
            last_health = build_health(
                'unknown',
                'SSL error',
                checked_at=utc_now(),
                final_url=url,
                details=f'{method} {url} failed with SSL error: {exc}',
            )

            if attempt < FETCH_RETRIES:
                retry_sleep(attempt)
                continue

            return last_health, ''

        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            last_health = build_health(
                'unknown',
                'Network error',
                checked_at=utc_now(),
                final_url=url,
                details=f'{method} {url} failed: {exc}',
            )

            if attempt < FETCH_RETRIES:
                retry_sleep(attempt)
                continue

            return last_health, ''

    return last_health or build_health(
        'unknown',
        'Network error',
        checked_at=utc_now(),
        final_url=url,
        details=f'{method} {url} failed after retries.',
    ), ''


def count_keyoapp_manga_cards(html: str) -> int:
    if not html:
        return 0

    lowered = html.lower()

    searched_match = re.search(
        r'<[^>]+id=["\']searched_series_page["\'][^>]*>([\s\S]{0,220000})',
        html,
        re.IGNORECASE,
    )

    if searched_match:
        block = searched_match.group(1)
        buttons = re.findall(r'<button\b', block, re.IGNORECASE)
        titles = re.findall(r'<h3\b', block, re.IGNORECASE)

        if buttons or titles:
            return max(len(buttons), len(titles))

    if 'id="searched_series_page"' in lowered or "id='searched_series_page'" in lowered:
        buttons = re.findall(r'<button\b', html, re.IGNORECASE)
        titles = re.findall(r'<h3\b', html, re.IGNORECASE)

        if buttons or titles:
            return max(len(buttons), len(titles))

    grid_blocks = re.findall(
        r'<div[^>]*class=["\'][^"\']*\bgrid\b[^"\']*["\'][^>]*>([\s\S]{0,160000})',
        html,
        re.IGNORECASE,
    )

    group_count = 0

    for block in grid_blocks:
        groups = re.findall(
            r'<div[^>]*class=["\'][^"\']*\bgroup\b[^"\']*["\'][^>]*>[\s\S]{0,9000}?(?:<h3\b|<a\b)',
            block,
            re.IGNORECASE,
        )
        group_count += len(groups)

    if group_count:
        return group_count

    possible_titles = re.findall(r'<h3\b', html, re.IGNORECASE)
    possible_manga_links = re.findall(
        r'<a[^>]+href=["\'][^"\']*(?:series|manga|comic|title)[^"\']*["\']',
        html,
        re.IGNORECASE,
    )

    if len(possible_titles) >= 3 and len(possible_manga_links) >= 3:
        return min(len(possible_titles), len(possible_manga_links))

    return 0


def probe_keyoapp_domain(domain: str) -> dict[str, object | None]:
    results: list[dict[str, object | None]] = []

    for scheme in ('https', 'http'):
        for path in ('latest', 'series'):
            url = build_source_url(domain, path, scheme)
            health, html = request_html_once(url)

            if health['status'] != 'working':
                results.append(health)
                continue

            manga_count = count_keyoapp_manga_cards(html)

            if manga_count > 0:
                return build_health(
                    'working',
                    'Functional',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=f'KeyoappParser simulation returned {manga_count} manga result(s) from {url}.',
                )

            results.append(
                build_health(
                    'unknown',
                    'Site reachable, but no app results found',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=f'{url} responded, but KeyoappParser selectors returned 0 manga cards.',
                )
            )

    return choose_better_failure(results)


def count_madara_manga_cards(html: str) -> int:
    if not html:
        return 0

    selectors = (
        r'<div[^>]*class=["\'][^"\']*\brow\b[^"\']*\bc-tabs-item__content\b[^"\']*["\']',
        r'<div[^>]*class=["\'][^"\']*\bpage-item-detail\b[^"\']*["\']',
        r'<div[^>]*class=["\'][^"\']*\bmanga__item\b[^"\']*["\']',
    )

    total = 0

    for pattern in selectors:
        total += len(re.findall(pattern, html, re.IGNORECASE))

    if total:
        return total

    title_links = re.findall(
        r'<a[^>]+href=["\'][^"\']*(?:manga|comic|series)[^"\']*["\'][^>]*>[\s\S]{0,180}?'
        r'(?:<h3\b|<h4\b|class=["\'][^"\']*(?:manga-title|post-title|manga-name))',
        html,
        re.IGNORECASE,
    )

    return len(title_links)


def create_madara_request_template() -> dict[str, str]:
    return {
        'action': 'madara_load_more',
        'page': '0',
        'template': 'madara-core/content/content-search',
        'vars[s]': '',
        'vars[paged]': '1',
        'vars[template]': 'search',
        'vars[meta_query][0][relation]': 'AND',
        'vars[meta_query][relation]': 'AND',
        'vars[post_type]': 'wp-manga',
        'vars[post_status]': 'publish',
        'vars[manga_archives_item_layout]': 'default',
        'vars[orderby]': 'date',
        'vars[order]': 'desc',
    }


def probe_madara_domain(domain: str) -> dict[str, object | None]:
    results: list[dict[str, object | None]] = []

    for scheme in ('https', 'http'):
        ajax_url = f'{scheme}://{domain}/wp-admin/admin-ajax.php'
        ajax_health, ajax_html = request_html_once(
            ajax_url,
            form=create_madara_request_template(),
        )

        if ajax_health['status'] == 'working':
            manga_count = count_madara_manga_cards(ajax_html)

            if manga_count > 0:
                return build_health(
                    'working',
                    'Functional',
                    checked_at=ajax_health.get('checkedAt'),
                    latency_ms=ajax_health.get('latencyMs'),
                    http_status=ajax_health.get('httpStatus'),
                    final_url=ajax_health.get('finalUrl'),
                    details=f'MadaraParser AJAX simulation returned {manga_count} manga result(s).',
                )

            results.append(
                build_health(
                    'unknown',
                    'Site reachable, but no app results found',
                    checked_at=ajax_health.get('checkedAt'),
                    latency_ms=ajax_health.get('latencyMs'),
                    http_status=ajax_health.get('httpStatus'),
                    final_url=ajax_health.get('finalUrl'),
                    details='MadaraParser AJAX response did not contain expected manga card selectors.',
                )
            )
        else:
            results.append(ajax_health)

        for path in ('manga', 'manga-list', ''):
            url = build_source_url(domain, path, scheme)
            list_health, list_html = request_html_once(url)

            if list_health['status'] != 'working':
                results.append(list_health)
                continue

            manga_count = count_madara_manga_cards(list_html)

            if manga_count > 0:
                return build_health(
                    'working',
                    'Functional',
                    checked_at=list_health.get('checkedAt'),
                    latency_ms=list_health.get('latencyMs'),
                    http_status=list_health.get('httpStatus'),
                    final_url=list_health.get('finalUrl'),
                    details=f'MadaraParser fallback simulation returned {manga_count} manga result(s).',
                )

            results.append(
                build_health(
                    'unknown',
                    'Site reachable, but no app results found',
                    checked_at=list_health.get('checkedAt'),
                    latency_ms=list_health.get('latencyMs'),
                    http_status=list_health.get('httpStatus'),
                    final_url=list_health.get('finalUrl'),
                    details=f'{url} responded, but MadaraParser selectors returned 0 manga cards.',
                )
            )

    return choose_better_failure(results)


def count_asura_manga_cards(html: str) -> int:
    if not html:
        return 0

    series_grid = re.search(
        r'<div[^>]+id=["\']series-grid["\'][^>]*>([\s\S]{0,240000})',
        html,
        re.IGNORECASE,
    )

    block = series_grid.group(1) if series_grid else html

    cards = re.findall(
        r'<div[^>]*class=["\'][^"\']*\bseries-card\b[^"\']*["\'][^>]*>[\s\S]{0,9000}?(?:<a\b|<h3\b)',
        block,
        re.IGNORECASE,
    )

    if cards:
        return len(cards)

    links = re.findall(
        r'<a[^>]+href=["\'][^"\']*/(?:series|comics|manga)/[^"\']+["\']',
        html,
        re.IGNORECASE,
    )

    titles = re.findall(r'<h3\b', html, re.IGNORECASE)

    if len(links) >= 3 and len(titles) >= 3:
        return min(len(links), len(titles))

    return 0


def probe_asura_domain(domain: str) -> dict[str, object | None]:
    results: list[dict[str, object | None]] = []

    for scheme in ('https', 'http'):
        for path in ('browse?page=1', 'series', ''):
            if '?' in path:
                url = f'{scheme}://{domain}/{path}'
            else:
                url = build_source_url(domain, path, scheme)

            health, html = request_html_once(url)

            if health['status'] != 'working':
                results.append(health)
                continue

            manga_count = count_asura_manga_cards(html)

            if manga_count > 0:
                return build_health(
                    'working',
                    'Functional',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=f'AsuraScans parser simulation returned {manga_count} manga result(s) from {url}.',
                )

            results.append(
                build_health(
                    'unknown',
                    'Site reachable, but no app results found',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=f'{url} responded, but AsuraScans selectors returned 0 cards.',
                )
            )

    return choose_better_failure(results)


def count_generic_html_manga_cards(html: str) -> int:
    if not html:
        return 0

    protected_reason = html_looks_blocked_or_protected(html)

    if protected_reason:
        return 0

    patterns = (
        r'<a[^>]+href=["\'][^"\']*(?:manga|series|comic|title|chapter)[^"\']*["\']',
        r'<div[^>]+class=["\'][^"\']*(?:manga|series|comic|card|item)[^"\']*["\']',
        r'<article[^>]+class=["\'][^"\']*(?:manga|series|comic|card|item)[^"\']*["\']',
    )

    score = 0

    for pattern in patterns:
        score += len(re.findall(pattern, html, re.IGNORECASE))

    titles = len(re.findall(r'<h[234]\b', html, re.IGNORECASE))

    if score >= 5 and titles >= 2:
        return min(score, max(titles, 1))

    return 0


def probe_generic_list_domain(domain: str, engine: str) -> dict[str, object | None]:
    results: list[dict[str, object | None]] = []

    candidate_paths = (
        '',
        'manga',
        'series',
        'browse',
        'latest',
        'comics',
    )

    for scheme in ('https', 'http'):
        for path in candidate_paths:
            url = build_source_url(domain, path, scheme)
            health, html = request_html_once(url)

            if health['status'] != 'working':
                results.append(health)
                continue

            manga_count = count_generic_html_manga_cards(html)

            if manga_count > 0:
                return build_health(
                    'unknown',
                    'Site reachable, parser not simulated',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=(
                        f'{engine or "Unknown"} is not implemented in the catalog checker. '
                        f'A generic page scan saw {manga_count} possible card(s), but this is not enough to mark Functional.'
                    ),
                )

            results.append(
                build_health(
                    'unknown',
                    'Site reachable, parser not simulated',
                    checked_at=health.get('checkedAt'),
                    latency_ms=health.get('latencyMs'),
                    http_status=health.get('httpStatus'),
                    final_url=health.get('finalUrl'),
                    details=(
                        f'{url} responded, but no app-specific parser simulation exists '
                        f'for engine {engine or "Unknown"}.'
                    ),
                )
            )

    return choose_better_failure(results)


def choose_better_failure(results: list[dict[str, object | None]]) -> dict[str, object | None]:
    if not results:
        return build_health('unknown', 'An error occurred', checked_at=utc_now())

    priority_phrases = [
        'Additional action required',
        'Access denied',
        'Too many requests',
        'Blocked by Cloudflare',
        'Site reachable, but no app results found',
        'Content not found or removed',
        'Network is not available',
        'Network error',
        'SSL error',
        'Site reachable, parser not simulated',
    ]

    for phrase in priority_phrases:
        match = next(
            (
                item
                for item in results
                if phrase.lower() in str(item.get('reason') or '').lower()
            ),
            None,
        )

        if match:
            return match

    for status in ('blocked', 'broken', 'unknown'):
        match = next((item for item in results if item.get('status') == status), None)

        if match:
            return match

    return results[0]


def source_is_asura(entry: dict[str, object], domain: str) -> bool:
    key = str(entry.get('key') or '').upper()
    title = str(entry.get('title') or '').lower()
    path = str(entry.get('path') or '').lower()
    domain_l = domain.lower()

    return (
        key in {'ASURASCANS', 'ASURA_SCANS', 'ASURA'}
        or 'asura' in title
        or 'asura' in path
        or 'asuracomic' in domain_l
        or 'asuratoon' in domain_l
        or 'asurascans' in domain_l
    )


def probe_source_entry(entry: dict[str, object]) -> dict[str, object | None]:
    domains = [
        str(domain)
        for domain in entry.get('domains', [])[:FETCH_MAX_DOMAINS_PER_SOURCE]
        if is_probable_domain(str(domain))
    ]

    if not domains:
        return build_health(
            'unknown',
            'Invalid domain',
            checked_at=utc_now(),
            details='No usable domain was extracted for this source.',
        )

    engine = str(entry.get('engine') or '')
    results: list[dict[str, object | None]] = []

    for domain in domains:
        if source_is_asura(entry, domain):
            result = probe_asura_domain(domain)
        elif engine == 'KeyoappParser':
            result = probe_keyoapp_domain(domain)
        elif engine == 'MadaraParser':
            result = probe_madara_domain(domain)
        else:
            result = probe_generic_list_domain(domain, engine)

        if result['status'] == 'working':
            return result

        results.append(result)

    return choose_better_failure(results)

def quote_gradle_arg(value: Path) -> str:
    text = str(value)
    return f'"{text}"' if ' ' in text else text


def run_parser_runtime_checker(entries: list[dict[str, object]]) -> dict[str, dict[str, object | None]]:
    if not PARSER_RUNTIME_ENABLED:
        return {}

    checker_dir = PARSER_RUNTIME_DIR.resolve()

    if not checker_dir.exists():
        print(f'Parser runtime checker not found: {checker_dir}', flush=True)
        return {}

    gradle_exe = shutil.which(PARSER_RUNTIME_GRADLE)

    if not gradle_exe:
        print(
            f'Parser runtime checker skipped: {PARSER_RUNTIME_GRADLE!r} was not found in PATH.',
            flush=True,
        )
        return {}

    with tempfile.TemporaryDirectory(prefix='parser-runtime-') as tmp_dir_raw:
        tmp_dir = Path(tmp_dir_raw)
        runtime_input = tmp_dir / 'sources.json'
        runtime_output = tmp_dir / 'health.json'

        runtime_payload = {
            'sources': [
                {
                    'key': str(entry.get('key') or ''),
                    'broken': bool(entry.get('broken')),
                }
                for entry in entries
            ],
        }

        runtime_input.write_text(
            json.dumps(runtime_payload, ensure_ascii=False),
            encoding='utf-8',
        )

        gradle_args = f'{quote_gradle_arg(runtime_input)} {quote_gradle_arg(runtime_output)}'

        print('running Parser runtime checker...', flush=True)

        completed = subprocess.run(
            [
                gradle_exe,
                '-q',
                'run',
                '--args',
                gradle_args,
            ],
            cwd=checker_dir,
            text=True,
            encoding='utf-8',
            errors='replace',
        )

        if completed.returncode != 0:
            print(
                f'Parser runtime checker failed with exit code {completed.returncode}.',
                flush=True,
            )
            return {}

        if not runtime_output.exists():
            print('Parser runtime checker did not write health output.', flush=True)
            return {}

        output = json.loads(runtime_output.read_text(encoding='utf-8'))
        health = output.get('health', {})

        if not isinstance(health, dict):
            return {}

        return health


def apply_health_checks(entries: list[dict[str, object]]) -> None:
    runtime_health = run_parser_runtime_checker(entries)

    for entry in entries:
        if entry.get('broken'):
            continue

        key = str(entry.get('key') or '')
        health = runtime_health.get(key)

        if isinstance(health, dict):
            entry['health'] = {
                'status': health.get('status') or 'unknown',
                'reason': health.get('reason') or 'Parser error',
                'checkedAt': health.get('checkedAt'),
                'latencyMs': health.get('latencyMs'),
                'httpStatus': health.get('httpStatus'),
                'finalUrl': health.get('finalUrl'),
                'resultCount': health.get('resultCount'),
                'details': health.get('details'),
                'checks': health.get('checks') if isinstance(health.get('checks'), dict) else {},
            }
            continue

        entry['health'] = build_health(
            'unknown',
            'Parser runtime unavailable',
            checked_at=utc_now(),
            details=(
                'The Parser runtime checker was not executed or did not return a result. '
                'Install Gradle or set PARSER_RUNTIME_ENABLED=0 to skip runtime checks.'
            ),
        )
        entry['health']['checks'] = {}

def extract_entry(
    repo_root: Path,
    file_path: Path,
    owner: str,
    repo: str,
    branch: str,
    class_index: dict[str, KotlinClassInfo],
):
    info = parse_class_info(file_path)
    text = info.text
    ann = ANN_RE.search(text)

    if not ann:
        return None

    key, title, locale, content_type = ann.groups()
    language = locale or 'multi'
    language_name = normalize_language_name(language)
    content_type = content_type or 'MANGA'

    engine = info.parent
    if not engine or engine == 'AbstractMangaParser':
        engine = info.name or 'Custom'

    broken_match = BROKEN_RE.search(text)
    broken_reason = (broken_match.group(1) or '').strip() if broken_match else ''
    is_broken = broken_match is not None

    domains, _ = collect_domains(info.name, class_index)
    relative_path = file_path.relative_to(repo_root).as_posix()
    is_nsfw = infer_nsfw(text, title, relative_path, domains)

    effective_broken_reason = (
        clean_text(broken_reason or 'This manga source has been marked as broken.')
        if is_broken
        else None
    )

    health = (
        build_health(
            'broken',
            effective_broken_reason,
            checked_at=utc_now(),
            details='Marked @Broken in upstream parser metadata.',
        )
        if is_broken
        else build_health(
            'unknown',
            'Pending parser simulation',
            checked_at=None,
            details='Availability will be checked after all source metadata is extracted.',
        )
    )

    return {
        'id': key,
        'key': key,
        'title': title,
        'language': language,
        'languageName': language_name,
        'engine': engine,
        'contentType': content_type,
        'broken': is_broken,
        'brokenReason': effective_broken_reason,
        'nsfw': is_nsfw,
        'path': relative_path,
        'repoUrl': f'https://github.com/{owner}/{repo}/blob/{branch}/{relative_path}',
        'rawUrl': f'https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{relative_path}',
        'domains': domains,
        'searchText': build_search_text(
            title=title,
            key=key,
            language=language,
            language_name=language_name,
            engine=engine,
            content_type=content_type,
            path=relative_path,
            broken_reason=effective_broken_reason,
            domains=domains,
        ),
        'health': health,
    }


def build_dataset(
    repo_root: Path,
    output_path: Path,
    owner: str,
    repo: str,
    branch: str,
) -> None:
    src_root = repo_root / 'src' / 'main' / 'kotlin'

    if not src_root.exists():
        raise SystemExit(f'Parser source root not found: {src_root}')

    kotlin_files = sorted(src_root.rglob('*.kt'))
    class_index: dict[str, KotlinClassInfo] = {}

    for file_path in kotlin_files:
        info = parse_class_info(file_path)

        if info.name and info.name not in class_index:
            class_index[info.name] = info

    entries: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    duplicates: list[str] = []

    for file_path in kotlin_files:
        entry = extract_entry(repo_root, file_path, owner, repo, branch, class_index)

        if not entry:
            continue

        if entry['key'] in seen_keys:
            duplicates.append(str(entry['key']))
            continue

        seen_keys.add(str(entry['key']))
        entries.append(entry)

    apply_health_checks(entries)

    entries.sort(key=lambda item: str(item['title']).lower())

    summary = {
        'total': len(entries),
        'working': sum(1 for item in entries if item['health']['status'] == 'working'),
        'broken': sum(1 for item in entries if item['health']['status'] == 'broken'),
        'blocked': sum(1 for item in entries if item['health']['status'] == 'blocked'),
        'unknown': sum(1 for item in entries if item['health']['status'] == 'unknown'),
        'nsfw': sum(1 for item in entries if item.get('nsfw')),
    }

    by_type: dict[str, int] = {}
    by_locale: dict[str, int] = {}
    domain_groups: dict[str, list[dict[str, str]]] = {}

    for item in entries:
        content_type = str(item['contentType'])
        language = str(item['language'])

        by_type[content_type] = by_type.get(content_type, 0) + 1
        by_locale[language] = by_locale.get(language, 0) + 1

        for domain in item['domains']:
            domain_groups.setdefault(str(domain), []).append(
                {
                    'key': str(item['key']),
                    'title': str(item['title']),
                    'language': language,
                }
            )

    duplicates_detected = [
        {
            'domain': domain,
            'entries': matches,
        }
        for domain, matches in sorted(domain_groups.items())
        if len(matches) > 1
    ]

    payload = {
        'generatedAt': utc_now(),
        'generatedBy': 'scripts/build_catalog.py',
        'sourceRepo': {
            'owner': owner,
            'repo': repo,
            'branch': branch,
        },
        'summary': summary,
        'byType': by_type,
        'byLocale': by_locale,
        'sources': entries,
        'disclaimer': (
            'This website is an informational catalog of Parser source metadata and generated Parser availability labels. '
            'No reader application is provided here, and no source content is hosted, cached, displayed, or proxied. '
            '“Functional” means the Parser runtime passed List, Manga details, Chapters, and Images during generation. '
            'Search is reported separately so a source can show a search-specific issue without hiding reader-path health. '
            'Images are checked by resolving the Parser image URL only; image bodies are not downloaded or stored. '
            'CAPTCHA, Cloudflare, anti-bot pages, rate limits, and access-denied responses are detected and reported, not bypassed. '
            'Errors are best-effort Parser labels and may be outdated or inaccurate.'
        ),
        'duplicatesSkipped': duplicates,
        'duplicatesDetected': duplicates_detected,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + '\n',
        encoding='utf-8',
    )

    print(
        f'wrote {output_path} :: '
        f'{summary["total"]} sources, '
        f'{summary["working"]} functional, '
        f'{summary["broken"]} broken, '
        f'{summary["blocked"]} blocked, '
        f'{summary["unknown"]} unknown, '
        f'{summary["nsfw"]} nsfw',
        flush=True,
    )


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_dir).resolve()
    output_path = Path(args.output).resolve()

    build_dataset(repo_root, output_path, args.owner, args.repo, args.branch)


if __name__ == '__main__':
    main()