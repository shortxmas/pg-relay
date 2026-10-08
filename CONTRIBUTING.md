# Contributing to pg-relay

## Development

```sh
npm install
npm test            # jest
npm run typecheck   # tsc --noEmit
```

- **Database:** the tests run against a real Postgres, set by `TEST_DB_URL`. Each test works in its own table and drops it afterwards. To set it up:
  ```sh
  createdb pg_relay_test
  cp .env.example .env    # then set TEST_DB_URL, e.g. postgres:///pg_relay_test
  ```
  `jest.config.js` loads `.env`. A `TEST_DB_URL` already set in the environment takes precedence. Without one, every test suite fails with a message saying so.
- **CI:** `.github/workflows/ci.yml` runs the typecheck, the tests and the build on every pull request and every push to `main`, against a throwaway `postgres:17` service container.
- **Test-driven:** write the test first and watch it fail, then implement.
- **Test layout:** one test file per source file. `queue.ts` has `queue.test.ts`. Shared database helpers live in `src/testing/db.ts`.
- **Docs:** one reference file per published source file, in `docs/`. `src/queue.ts` has `docs/queue.md`, and `src/index.ts` has `docs/index.md`. They ship in the npm package, so update the matching doc in the same change as the code: every export, option, default, error and behaviour. `README.md` holds install, the quick start and guides, and links to the docs instead of repeating them.

## Releasing

Releases are published to npm from CI. Put `[major]`, `[minor]` or `[patch]` in a commit's **title** (its first line) and push it to `main`. When squash-merging, the PR title becomes the commit title. Tags in a commit's body are ignored, so a description can mention them safely. After the tests pass, the `release` job:

1. picks the bump from the titles of the pushed commits (the highest one wins if several are tagged; case doesn't matter)
2. runs `npm version <bump>` on the version in `package.json`
3. builds and runs `npm publish --provenance`
4. commits the new version to `main` as `Release vX.Y.Z [skip ci]`
5. creates a GitHub release `vX.Y.Z` on that commit, with notes generated from the changes since the last release. This step only runs if the npm publish succeeded.

Pushes without a tag run the tests and publish nothing. Don't edit `version` in `package.json` by hand.

Setup:
- **`NPM_TOKEN`:** a repository secret holding an npm token allowed to publish `pg-relay`, e.g. a granular token with read and write access to the package.
- **Branch protection:** if `main` is protected, it must allow GitHub Actions to push, or step 4 fails after the package is already published.

`npm run build` compiles `src/` (minus the tests) to `dist/` with `tsconfig.build.json`. The package ships `dist/`, the README and the license only.

## Source layout

| File | Purpose |
|---|---|
| `src/queue.ts` | `PgQueue`: connection, table creation |
| `src/publisher.ts` | `Publisher.writeJob`, `DEFAULT_JOB_CONFIG` |
| `src/subscriber.ts` | `Subscriber.listen`, claiming, lock renewal, `complete`/`fail` |
| `src/job.ts` | Types: `JobConditions`, `JobConfig`, `JobPayload`, `JobRecord` |
| `src/index.ts` | Public exports |
