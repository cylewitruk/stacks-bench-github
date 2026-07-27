#!/usr/bin/env bash
# Build one immutable dataset generation and atomically advance an operator
# `current` pointer. Worker config must name the generation directory itself.
set -euo pipefail

if [[ $# -ne 7 ]]; then
    echo "usage: $0 SOURCE DATASET_ROOT GENERATION NETWORK FORMAT COVERED_START COVERED_END" >&2
    exit 2
fi

source_dir=$1
dataset_root=$2
generation=$3
network=$4
format_version=$5
covered_start=$6
covered_end=$7

[[ -d "$source_dir" && ! -L "$source_dir" ]] || {
    echo "source must be a real directory" >&2
    exit 2
}
[[ "$generation" =~ ^[A-Za-z0-9._-]+$ ]] || {
    echo "generation contains unsafe characters" >&2
    exit 2
}
[[ "$network" =~ ^[A-Za-z0-9._-]+$ && "$format_version" =~ ^[A-Za-z0-9._-]+$ ]] || {
    echo "network and format must contain only safe identifier characters" >&2
    exit 2
}
[[ "$covered_start" =~ ^[0-9]+$ && "$covered_end" =~ ^[0-9]+$ ]] || exit 2
(( covered_start <= covered_end )) || {
    echo "covered range is reversed" >&2
    exit 2
}

mkdir -p "$dataset_root"
final="$dataset_root/$generation"
[[ ! -e "$final" ]] || {
    echo "generation already exists: $final" >&2
    exit 1
}
staging=$(mktemp -d "$dataset_root/.staging-$generation.XXXXXX")
cleanup() {
    if [[ -d "$staging" ]]; then
        rm -rf -- "$staging"
    fi
}
trap cleanup EXIT

if find "$source_dir" -type l -print -quit | grep -q .; then
    echo "source contains symlinks; shared-write indirection is forbidden" >&2
    exit 1
fi
cp --reflink=always -a "$source_dir/." "$staging/"

manifest="$staging/.sbgh-dataset-manifest.json"
file_list="$staging/.sbgh-dataset-files.sha256"
(
    cd "$staging"
    find . -type f \
        ! -name '.sbgh-dataset-manifest.json' \
        ! -name '.sbgh-dataset-files.sha256' \
        -print0 | sort -z | xargs -0 -r sha256sum
) > "$file_list"
# Qualify the copied generation itself, not only the mutable source from which
# it was cloned. Publication is all-or-nothing if any byte differs.
(
    cd "$staging"
    sha256sum --check --strict '.sbgh-dataset-files.sha256'
)
files_digest=$(sha256sum "$file_list" | awk '{print $1}')
cat > "$manifest" <<EOF
{"generation":"$generation","network":"$network","format_version":"$format_version","covered_start":$covered_start,"covered_end":$covered_end,"files_sha256":"$files_digest"}
EOF
manifest_digest=$(sha256sum "$manifest" | awk '{print $1}')

chmod -R a-w "$staging"
mv "$staging" "$final"
staging=
ln -sfn "$generation" "$dataset_root/.current-next"
mv -Tf "$dataset_root/.current-next" "$dataset_root/current"

echo "generation=$generation"
echo "manifest_sha256=$manifest_digest"
