# made by [InvalidDavid](https://github.com/InvalidDavid)
# Kotatsu Source Watch

A Vue/Vite dashboard for browsing Kotatsu parser sources, extracted domains, and a generated health snapshot.

## What it does

- Pulls parser files from `YakaTeam/kotatsu-parsers`
- Extracts source metadata from `@MangaSourceParser(...)`
- Extracts domains from `ConfigKey.Domain(...)` blocks when present
- Runs network-level health checks for the primary domain
- Writes a static dataset to `public/data/sources.json` during the GitHub Actions build
- Deploys the built site to GitHub Pages

## The important part

This project is designed to refresh itself on GitHub every 24 hours. You do **not** need a local server for updates after deployment.

## GitHub setup

1. Create a new GitHub repository.
2. Upload or push all project files to the `main` branch.
3. In your repository, go to **Settings → Pages**.
4. Under **Build and deployment**, set **Source** to **GitHub Actions**.
5. Commit the workflow file at `.github/workflows/pages.yml`.
6. Open the **Actions** tab and run **Build and deploy dashboard** once with **Run workflow**.
7. After the first successful run, GitHub Pages will publish your site.

## Local development

```bash
npm install
npm run generate:data
npm run dev
```

## Environment variables

Optional overrides:

- `KOTATSU_OWNER` default: `YakaTeam`
- `KOTATSU_REPO` default: `kotatsu-parsers`
- `KOTATSU_BRANCH` default: `master`
- `GITHUB_TOKEN` optional, recommended for GitHub API rate limits
- `CHECK_CONCURRENCY` default: `12`
- `CHECK_TIMEOUT_MS` default: `8000`
- `SKIP_HEALTH_CHECKS=true` to generate metadata without network probing

## Status model

- `working`: endpoint returned 2xx or 3xx
- `blocked`: endpoint returned 401, 403, or 429
- `broken`: dead endpoint, timeout, DNS failure, or hard 4xx/5xx failure
- `unknown`: no domain extracted or inconclusive result

## Blunt truth

This is still not a full parser integration lab. A website can answer HTTP 200 and still break in the app. A website can also block CI bots and still work for real users.
