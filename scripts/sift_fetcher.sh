#! /bin/sh

set -eu

if [ "$#" -lt 1 ]; then
    echo "usage: $0 command [args]"
    exit 1
fi

cd $HOME

case $1 in
    init)
	if [ -d venv ]; then
	    echo "a virtual environment already exists, aborting..."
	    exit 1
	fi
	python3 -m venv venv
	. venv/bin/activate
	pip install pytrends
	echo "Done"
	;;
    fetch)
	. venv/bin/activate
	shift
	python3 sift_fetcher "$@"
	;;
    ssh)
	. venv/bin/activate
	read tf
	read kw
	read geo
	if [ -n "$geo" ]; then
	    python3 sift_fetcher "$tf" "$kw" "$geo"
	else
	    python3 sift_fetcher "$tf" "$kw"
	fi
	;;
    *)
	echo "unknown command $0"
	exit 1
	;;
esac
