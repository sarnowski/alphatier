#!/bin/sh

# retrieve current version
VERSION=$(git describe --tags)
if [ -z "$VERSION" ]; then
	echo "Cannot retrieve version; aborting release!" >&2
	exit 1
fi

dirty=false
git update-index -q --ignore-submodules --refresh
git diff-files --quiet --ignore-submodules || dirty=true
if [ "$dirty" = false ]; then
	git diff-index --cached --quiet --ignore-submodules HEAD || dirty=true
fi
if [ "$dirty" = true ]; then
	VERSION="$VERSION-dirty"
fi

[ -z "$1" ] && echo $VERSION

while [ ! -z "$1" ]; do
	mkdir -p $(dirname $1)
	echo "alphatierVersion=$VERSION" > $1
	shift
done
