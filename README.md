# SIFT

This repository contains SIFT codebase. SIFT is a scalable architecture to extract data from Google Trends. [SIFT](https://nsg.ee.ethz.ch/fileadmin/user_upload/publications/sift.pdf) has been published at [IMC'22](https://conferences.sigcomm.org/imc/2022/accepted/) in a work studying user-affecting Internet outages.

## Bibtex

```bibtex
@inproceedings{10.1145/3517745.3561428,
author = {Kirci, Ege Cem and Vahlensieck, Martin and Vanbever, Laurent},
title = {"Is My Internet down?": Sifting through User-Affecting Outages with Google Trends},
year = {2022},
isbn = {9781450392594},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3517745.3561428},
doi = {10.1145/3517745.3561428},
abstract = {What are the worst outages for Internet users? How long do they last, and how wide are they? Such questions are hard to answer via traditional outage detection and analysis techniques, as they conventionally rely on network-level signals and do not necessarily represent users' perceptions of connectivity.We present SIFT, a detection and analysis tool for capturing user-affecting Internet outages. SIFT leverages users' aggregated web search activity to detect outages. Specifically, SIFT starts by building a timeline of users' interests in outage-related search queries. It then analyzes this timeline looking for spikes of user interest. Finally, SIFT characterizes these spikes in duration, geographical extent, and simultaneously trending search terms which may help understand root causes, such as power outages or associated ISPs.We use SIFT to collect more than 49 000 Internet outages in the United States over the last two years. Among others, SIFT reveals that user-affecting outages: (i) do not happen uniformly: half of them originate from 10 states only; (ii) can affect users for a long time: 10\% of them last at least 3 hours; and (iii) can have a broad impact: 11\% of them simultaneously affect at least 10 distinct states. SIFT annotations also reveal a perhaps overlooked fact: outages are often caused by climate and/or power-related issues.},
booktitle = {Proceedings of the 22nd ACM Internet Measurement Conference},
pages = {290–297},
numpages = {8},
keywords = {Google trends, internet outages, data mining, anomaly detection},
location = {Nice, France},
series = {IMC '22}
}
```

## Contact

Please send us an email at `ekirci-at-ethz-dot-ch` and `martinva-at-student-dot-ethz-dot-ch`, if:

- You are interested in collaborating with the project.
- You are having issues trying to run SIFT.
- You happen to find a bug.
- You have any questions or concerns specific to the paper.

Thank you for your interest!

# Overview
SIFT consists of five commands:

 - `sift_queue` queues requests which will then be executed by `sift_dispatcher`.
 - `sift_dispatcher` executes the requests using one or more fetchers.
 - `sift_fetcher` is the part that interacts with Trends.  It is executed by the dispatcher.
 - `sift_tool` can be used to add new keywords and tags.
 - `sift_stitch` is used to create a SQLite3 database with a stitched time series.

Requests and the returned data are stored inside a PostgreSQL database.

## Requirements
 - PostgreSQL (tested with version 12).  Must be running locally as
   all tools use authentication via UNIX sockets.
 - Python 3

## Installation
First set up PostgreSQL.  SIFT connects to PostgreSQL using UNIX
sockets.  For that create a database and a user named after the user
name under which you want to run SIFT (in the following we use the
user `sift`):

```sql
CREATE DATABASE sift;
CREATE USER sift;
```

To test the setup, change to the `sift` user and do

```sh
psql
```

You should be presented with a prompt without error messages or having to
enter a password.

Now under the `sift` user get a copy of the source code, either by cloning
this repository or extracting a tarball.  Change to the directory and
create and activate a virtual environment using:

```sh
python3 -m venv venv
. venv/bin/activate
```

Then install the dependencies using

```sh
pip install -r requirements.txt
```

> **Note**:
> You might need to install additional packages like `python3-dev` and `libpq-dev`.

After that install SIFT using

```sh
pip install .
```

Initialize the database using
```sh
psql < sql/schema.sql
```

## Quickstart
If you want to start fetching right away, you can load the database
with the internet outage topic keyword and US states using
```sh
psql < sql/quickstart.sql
```

Then add some requests using `sift_queue` and start `sift_dispatcher`
with the `--local` option to execute the fetcher under the same user
id as the dispatcher:

```sh
sift_dispatcher --local
```

## `fetchers.json`
When not running with the `--local` flag, `sift_dispatcher` uses a file
called `fetcher.json` to get a list of all fetchers.  To get started
copy the file `fetchers.json.example` to `fetcher.json` and edit it
accordingly.

## Setting up a fetcher on a remote machine
The main feature of SIFT is the possibility to run fetchers on remote machines.
To do that, you first need to copy `scripts/sift_fetcher.sh` and the fetcher
itself (`bin/sift_fetcher`) to machine you wish to use.

Next you have to generate an SSH key for the `sift` user  and add it to the
`authorized_keys` file on the remote machine with the following options to
restrict its scope:
```
from="<ip-of-dispatcher>",restrict,command="/bin/sh <remote-user-home>/sift_fetcher.sh ssh" ssh-ed25519 AAAA...
```

You can read about the individual options in the sshd(8) man page.  Replace
`<ip-of-dispatcher>` with the IP from which the dispatcher contacts the fetcher
and `<remote-user-home>` with the directory in which you copied
`sift_fetcher.sh` and `sift_fetcher`.

To initialize the required virtual environment run
```sh
sh sift_fetcher.sh init
```

The entry in `fetchers.json` should look something like this:
```json
[
	...
	{"active": true, "type": "ssh", "user": "<remote-user>", "host": "<remote-host>"},
	...
]
```

### More complicated SSH setups
Under the hood `sift_dispatcher` executes SSH via `Popen` and joins the user
and host with an `@`.  This means that it will use for example `.ssh/config` to
add additional options, such as an different port or a jump host.

## Execute the fetcher via sudo(8)
It is also possible to execute the fetcher on the same machine but under a different user using
sudo(8).  For this copy the files `scripts/sift_fetcher.sh` and `bin/sift_fetcher` to the home
directory of the target user as for remote fetching and add a line like this to sudoers(5):
```sudoers
dispatcher ALL=(fetcher:fetcher) NOPASSWD: /bin/sh /home/fetcher/sift_fetcher.sh fetch *
```
Replace `dispatcher` and `fetcher` appropriately.

The entry in `fetchers.json` should then look like this:
```json
[
	...
	{"type": "sudo", "user": "fetcher", "group": "fetcher", "script": "/home/fetcher/sift_fetcher.sh"},
	...
]
```

## Custom fetchers
The different fetcher implementations are in `bin/sift_dispatcher` and should
be easy enough to extend.

## Easy crashing
The dispatcher crashes quite easily.  This is intentional to avoid overloading
Trends in case of a bug.

## Web Interface
SIFT has a web interface to browse the data.  It is located in `web_inteface/`.
The dependencies can be installed by switching to `web_interface/` and
executing `pip install -r requirements.txt  Then a local development server
(again in the `web_interface` directory) can be started with

```sh
    FLASK_APP=vis flask run
```

Stitched time series are not generated by the web interface but must be
precomputed using `sift_stitch`.  `sift_stitch` creates a database
with the stitched time series called `time_series.db`.  This file must
be present in the directory from which you run the web interface.
`sift_stitch` requires that the requests have so-called resolution tags.
These can be added with `sift_cli add-resolution-tags`.

By default the web interface connects via UNIX sockets to the database.
To change that modify the `DATABASE_URL` in `web_interface/config.py`.

For increased security it is possible to run the web interface under a
different user.  To do that create a new user in PostgreSQL (e.g. vis)
and grant select rights on the appropriate tables and views:

```sql
GRANT SELECT ON TABLE request_who, request_types, request_api, locations,
request_status, fetchers, requests, keyword_topics, keywords, keywords_info,
keywords_in_request, keywords_related, trends_time, trends_geo_scopes,
trends_geo, log, tags, keywords_tags, peaks_detailed, peaks_detailed_requests,
keywords_ignore, rtags, request_tags, keywords_and_topics,
raw_fetcher_output_count TO vis;
```

Don't forget to update `DATABASE_URL` in `config.py`.

## Stopping a dispatcher
You can stop a dispatcher by pressing `Ctrl-C`.  If possible press it
when the dispatcher is in its wait period.  You can also pass the
`--exit` flag which causes the dispatcher to exit as soon as there is
nothing to do (which does not mean that there are no more requests).

## Running multiple dispatchers

It is safe to run multiple dispatchers in parallel using different
fetchers as each dispatcher locks a request by setting its status to
running before executing.

## Fix requests stuck in running state
In some error cases it can happen that upon error the request is left
in a 'running' state even when it is not.  When that happens, the
request has to be manually reset to open using (replacing
`rid-of-the-request`):

```sql
UPDATE requests SET r_status = (SELECT rs_id FROM request_status WHERE rs_name = 'open')
 WHERE r_status = (SELECT rs_id FROM request_status WHERE rs_name = 'running')
   AND r_id = rid-of-the-request;
```

You should stop all dispatchers first to assure that indeed no
dispatcher is trying to fetch the request.
