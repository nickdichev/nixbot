{
  lib,
  runCommand,
  mdbook,
}:
# mdBook site built from the repo's Markdown, published by the gh-pages effect.
let
  src = ../.;
  docNames = builtins.filter (lib.hasSuffix ".md") (
    builtins.attrNames (builtins.readDir "${src}/docs")
  );
  chapters = map (name: {
    inherit name;
    title = lib.removeSuffix ".md" name;
  }) docNames;
  summary = ''
    # Summary

    [Introduction](README.md)

  ''
  + lib.concatMapStringsSep "\n" (c: "- [${c.title}](${c.name})") chapters;
  bookToml = ''
    [book]
    title = "nixbot"
    src = "src"

    [output.html]
  '';
in
runCommand "nixbot-docs"
  {
    nativeBuildInputs = [ mdbook ];
    inherit summary bookToml;
  }
  ''
    mkdir -p book/src
    printf '%s' "$bookToml" > book/book.toml
    printf '%s\n' "$summary" > book/src/SUMMARY.md
    cp ${src}/README.md book/src/README.md
    ${lib.concatMapStringsSep "\n" (c: "cp ${src}/docs/${c.name} book/src/${c.name}") chapters}
    mdbook build book -d "$out"
  ''
