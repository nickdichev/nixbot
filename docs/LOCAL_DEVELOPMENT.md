# Local Development

## Running the test suite

```bash
nix develop
cd nixbot
python -m pytest nixbot/tests -q
```

The tests cover the full pipeline (webhook parsing, evaluation, scheduling,
building, status reporting, web frontend) against an ephemeral PostgreSQL
instance and real `nix`/`git` where available.

## Running the service locally

nixbot is a single process configured by a JSON file:

```bash
# Start an ephemeral PostgreSQL
initdb -D /tmp/bb-pg
pg_ctl -D /tmp/bb-pg -o "-k /tmp/bb-pg -c listen_addresses=" start
createdb -h /tmp/bb-pg nixbot

cat > /tmp/nixbot.json <<EOF
{
  "db_url": "postgresql://$(whoami)@/nixbot?host=/tmp/bb-pg",
  "build_systems": ["x86_64-linux"],
  "url": "http://localhost:8010/",
  "state_dir": "/tmp/nixbot-state",
  "pull_based": {
    "repositories": {
      "my-project": {
        "name": "my-project",
        "default_branch": "main",
        "url": "https://github.com/example/my-project"
      }
    }
  }
}
EOF

python -m nixbot.main --config /tmp/nixbot.json --log-format text
```

Access the web UI at http://localhost:8010. Pull-based repositories need no
forge credentials, which makes them convenient for local hacking; GitHub/Gitea
configuration works the same way as in the NixOS module, with secret paths
pointing at plain local files.

## VM integration test

The end-to-end NixOS test (fake GitHub + real Gitea) lives in
`checks/nixbot.nix`:

```bash
nix build .#checks.x86_64-linux.nixbot -L
```

For interactive debugging:

```bash
nix build .#checks.x86_64-linux.nixbot.driverInteractive
./result/bin/nixos-test-driver
```

Add `breakpoint()` in the test script to pause execution.

## Code quality

```bash
nix develop -c flake-fmt   # treefmt: ruff format, ruff check, mypy, nixfmt
```

## SQL queries (sqlc)

SQL statements live in `nixbot/nixbot/queries/*.sql` and are compiled to typed
asyncpg query functions in `nixbot/nixbot/db_gen/` (generated code, do not edit)
by [sqlc](https://sqlc.dev) with the
[sqlc-gen-better-python](https://github.com/rayakame/sqlc-gen-better-python)
plugin; the schema is read from `nixbot/nixbot/migrations/`. After changing a
query or migration, regenerate and commit the output:

```bash
nix develop -c sqlc generate
```

The dev shell's `sqlc` is wrapped (`nix/sqlc.nix`) to use the same nix-pinned
plugin as CI and works offline; when bumping the plugin, update the URL/checksum
in both `sqlc.yaml` and `nix/sqlc.nix`.

CI verifies freshness via `nix build .#checks.x86_64-linux.sqlc-generated`.
Dynamically assembled SQL (e.g. the build-list filters in `web/queries.py`) and
the migration runner stay on raw asyncpg.
