{ self, pkgs }:
let
  sqlc = import ../nix/sqlc.nix { inherit pkgs; };
in
# Regenerate the sqlc output offline (the wrapped sqlc uses the
# nix-pinned plugin) and fail when the committed db_gen package is
# stale.
pkgs.runCommand "sqlc-generated-fresh"
  {
    nativeBuildInputs = [ sqlc ];
  }
  ''
    cp -r ${self} src
    chmod -R u+w src
    cd src
    export HOME=$TMPDIR
    rm -r nixbot/nixbot/db_gen
    sqlc generate
    if ! diff -ru ${self}/nixbot/nixbot/db_gen nixbot/nixbot/db_gen; then
      echo "error: nixbot/nixbot/db_gen is stale; run 'sqlc generate'" >&2
      exit 1
    fi
    touch $out
  ''
