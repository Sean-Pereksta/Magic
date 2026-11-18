#!/usr/bin/env bash
set -euo pipefail

channel=catnmice-hub-channel
tag="catnmice-hub-${GITHUB_RUN_NUMBER}-${GITHUB_SHA:0:8}"
setup=Catnmice-Game-Hub-Setup.exe
manifest=github-stable.json
bundle=github-stable.sigstore.json
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
notes="$temporary/release-notes.txt"
printf 'Windows x64.\n\nMagic revision: %s\nTruth revision: %s\nSequence: %s\n' \
  "$GITHUB_SHA" "$TRUTH_SHA" "$GITHUB_RUN_NUMBER" > "$notes"

# A delayed run or approval must not publish an obsolete source pair.
is_current() {
  local magic truth
  magic=$(gh api "repos/$GH_REPO/commits/main" --jq .sha) || return 2
  truth=$(gh api repos/Sean-Pereksta/Truth/commits/main --jq .sha) || return 2
  [[ "$magic" == "$GITHUB_SHA" && "$truth" == "$TRUTH_SHA" ]]
}
check_sources() {
  local status
  if is_current; then return; else status=$?; fi
  if [[ "$status" == 1 ]]; then
    echo 'A newer main revision is available. It will be picked up by the next check.'
    exit 0
  fi
  echo 'Could not confirm source revisions.' >&2
  exit 1
}
check_sources

# Complete any draft left by a failed upload. Never replace assets of a published build.
if ! gh release view "$tag" --json isDraft > "$temporary/version.json" 2>/dev/null; then
  gh release create "$tag" --draft --latest=false --target "$GITHUB_SHA" \
    --title "Catnmice Game Hub build $GITHUB_RUN_NUMBER" --notes-file "$notes"
  draft=true
else
  draft=$(jq -r .isDraft "$temporary/version.json")
fi
if [[ "$draft" == true ]]; then
  version_files=(publish/channel/*.zip "publish/dist/$setup" "publish/channel/$manifest" "publish/channel/$bundle")
  if [[ -f publish/channel/stable.json ]]; then version_files+=(publish/channel/stable.json); fi
  gh release upload "$tag" "${version_files[@]}" --clobber
  gh release edit "$tag" --draft=false --latest=false
  stage=publish
else
  # A retry must promote exactly the files from the already-published build.
  stage="$temporary/published"
  mkdir -p "$stage/dist" "$stage/channel"
  gh release download "$tag" --pattern "$setup" --dir "$stage/dist"
  gh release download "$tag" --pattern 'github-stable.*json' --dir "$stage/channel"
  if gh release view "$tag" --json assets --jq '.assets[].name' | grep -Fx stable.json > /dev/null; then
    gh release download "$tag" --pattern stable.json --dir "$stage/channel"
  fi
  node desktop-hub/desktop/provenance/verify.cjs "$stage/channel/$manifest" "$stage/channel/$bundle"
fi
jq -e --arg magic "$GITHUB_SHA" --arg truth "$TRUTH_SHA" --argjson sequence "$GITHUB_RUN_NUMBER" \
  '.source.magic == $magic and .source.truth == $truth and .sequence == $sequence' \
  "$stage/channel/$manifest" > /dev/null
check_sources

channel_flags=(--latest=false)
if gh release view "$channel" --json body,isImmutable,isDraft,assets > "$temporary/channel.json" 2>/dev/null; then
  if [[ "$(jq -r .isImmutable "$temporary/channel.json")" == true ]]; then
    echo "The fixed download channel must allow asset updates. The build is available at $tag." >&2
    exit 1
  fi
  if [[ "$(jq -r .isDraft "$temporary/channel.json")" == true ]]; then channel_flags+=(--draft=false); fi
  previous=$(jq -r '.body' "$temporary/channel.json" | tr -d '\r' | sed -n 's/^Sequence: \([0-9][0-9]*\)$/\1/p')
  if [[ -n "$previous" && "$previous" -gt "$GITHUB_RUN_NUMBER" ]]; then
    echo 'A newer sequence is already published.'
    exit 0
  fi
  # The manifest can already be visible if the previous run stopped before editing the notes.
  if jq -e --arg file "$manifest" '.assets | any(.name == $file and .state == "uploaded")' "$temporary/channel.json" > /dev/null; then
    gh release download "$channel" --pattern "$manifest" --dir "$temporary"
    sequence=$(jq -er '.sequence | select(type == "number" and . == floor and . > 0)' "$temporary/$manifest")
    if [[ "$sequence" -gt "$GITHUB_RUN_NUMBER" ]]; then
      echo 'The channel manifest already points to a newer sequence.'
      exit 0
    fi
  fi
else
  channel_flags+=(--draft=false)
  gh release create "$channel" --draft --latest=false --target "$GITHUB_SHA" \
    --title 'Catnmice Game Hub for Windows' --notes 'Preparing the first Windows release.'
fi

# The signed manifest is the last asset to change. Readers fail verification during any mismatch.
gh release upload "$channel" "$stage/dist/$setup" "$stage/channel/$bundle" --clobber
if [[ -f "$stage/channel/stable.json" ]]; then
  gh release upload "$channel" "$stage/channel/stable.json" --clobber
fi
gh release upload "$channel" "$stage/channel/$manifest" --clobber
gh release edit "$channel" "${channel_flags[@]}" --notes-file "$notes"
