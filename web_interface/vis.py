import datetime
import io
import sqlite3

from flask import Flask, render_template, abort, make_response
from flask import request
from markupsafe import escape
import matplotlib.figure
import psycopg2

from config import DATABASE_URL
from sift import restore_timelabels, stitch_timeframes

app = Flask(__name__)

con = psycopg2.connect(DATABASE_URL)
con.set_session(readonly=True, autocommit=True)


class TimeSeriesDatabaseIsEmptyError(Exception):
    pass


def open_time_series_db():
    title = request.endpoint or 'error'
    try:
        c = sqlite3.connect('file:time_series.db?mode=ro', uri=True)
    except sqlite3.OperationalError:
        abort(make_response(render_template('empty.html', title=title,
                                            msg='time_series.db does not exist.  Check the README for instructions on how to create it')))

    res = c.execute('SELECT COUNT(*) FROM ts')
    if res.fetchone()[0] == 0:
        abort(make_response(render_template('empty.html', title=title,
                                            msg='time series database is empty')))

    return c


class VisPlot:
    def __init__(self, width=17, height=8.9, backend='svg'):
        self.w = width
        self.h = height
        self.c = []
        self.backend = backend

    def annotate(self, text, position):
        self.c.append(('annotate', text, position))

    def bar(self, x, y):
        self.c.append(('bar', x, y))

    def clf(self):
        self.c = []

    def hlines(self, y, xmin, xmax, color):
        self.c.append(('hlines', y, xmin, xmax, color))

    def legend(self):
        self.c.append(('legend',))

    def plot(self, x, y, **kwargs):
        self.c.append(('plot', list(x), list(y), kwargs))

    def savefig(self):
        f = matplotlib.figure.Figure((self.w, self.h))
        a = f.add_subplot()

        for command in self.c:
            if command[0] == 'annotate':
                _, text, position = command
                a.annotate(text, position)
            elif command[0] == 'bar':
                _, x, y = command
                a.bar(x, y)
            elif command[0] == 'hlines':
                _, y, xmin, xmax, color = command
                a.hlines(y, xmin, xmax, color)
            elif command[0] == 'legend':
                a.legend()
            elif command[0] == 'plot':
                _, x, y, kwargs = command
                a.plot(x, y, **kwargs)
            elif command[0] == 'vlines':
                _, x, y_min, y_max, color = command
                a.vlines(x, y_min, y_max, color)
            elif command[0] == 'xlim':
                _, left, right = command
                a.set_xlim(left, right)
            elif command[0] == 'xticks':
                _, ticks = command
                a.set_xticks(ticks)

        b = io.BytesIO()
        f.savefig(b, format=self.backend)

        if self.backend == 'svg':
            return b.getvalue().decode()

        return b.getvalue()

    def vlines(self, x, y_min, y_max, color):
        self.c.append(('vlines', x, y_min, y_max, color))

    def xlim(self, left, right):
        self.c.append(('xlim', left, right))

    def xticks(self, ticks):
        self.c.append(('ticks', ticks))


class SvgPlot(VisPlot):
    def __init__(self, width=17, height=8.9):
        super().__init__(width, height, 'svg')


class PngPlot(VisPlot):
    def __init__(self, width=17, height=8.9):
        super().__init__(width, height, 'png')


def db_one(cur, sql, args=None):
    """
        Returns the first column of the first row from the result of
        executing sql with args args.  The statement must return at
        least one row.
    """
    if args is None:
        args = []
    cur.execute(sql, args)
    return cur.fetchone()[0]


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', e=e), 404


@app.route("/")
def index():
    """ Shows an overview of the database. """
    cur = con.cursor()

    cur.execute('''SELECT k_title IS NULL kt, COUNT(*)
                     FROM keywords
                 GROUP BY kt''')

    keywords = cur.fetchall()
    keywords = list(map(lambda x: ['queries' if x[0] else 'topics', x[1]], keywords))
    keywords.append(['total', sum(map(lambda x: x[1], keywords))])

    cur.execute('''SELECT rs_id, rs_name, COUNT(*)
                     FROM request_status
                     JOIN requests ON r_status = rs_id
                 GROUP BY rs_id, rs_name
                 ORDER BY rs_id''')

    status = cur.fetchall()
    status.append([-1, 'total', sum(map(lambda x: x[2], status))])

    links = db_one(cur, 'SELECT COUNT(*) FROM keywords_related')

    unique_links = db_one(cur, '''SELECT COUNT(*)
                                    FROM (SELECT k_id
                                            FROM keywords_related
                                        GROUP BY k_id, kr_kw) AS k''')

    cur.execute('''SELECT COUNT(*), SUM(coalesce(array_length(t_v, 1), 0))
                     FROM trends_time''')
    timeframes = cur.fetchone()

    geo = db_one(cur, 'SELECT COUNT(*) FROM trends_geo')

    geo_nonzero = db_one(cur, 'SELECT COUNT(*) FROM trends_geo WHERE g_v != 0')

    rfo = db_one(cur, 'SELECT count FROM raw_fetcher_output_count')

    return render_template('index.html', keywords=keywords,
                           links=links, unique_links=unique_links,
                           status=status, timeframes=timeframes, geo=geo,
                           geo_nonzero=geo_nonzero, rfo=rfo)


@app.route("/keywords")
def keywords():
    """ Shows a list of all keywords. """
    cur = con.cursor()

    cur.execute('''SELECT k_id, k_q, k_title || ' (' || kt_name || ')'
                     FROM keywords_and_topics
                 ORDER BY k_id''')

    res = cur.fetchall()

    return render_template('keywords.html', keywords=res)


@app.route("/keyword/<int:k_id>")
def keyword_detailed(k_id):
    """ Shows details about keyword k_id. """
    cur = con.cursor()
    cur.execute('''SELECT k_q, k_title, k_added, kt_name, ki_active, ki_added,
                          ki_note,
                          (SELECT COUNT(*) FROM keywords_in_request WHERE k_id = %s)
                     FROM keywords
                LEFT JOIN keyword_topics USING (kt_id)
                LEFT JOIN keywords_info USING (k_id)
                    WHERE k_id = %s''', (k_id, k_id))

    res = cur.fetchone()
    if res is None:
        abort(404, "No keyword with id {}".format(escape(k_id)))

    k_q, k_title, k_added, kt_name, ki_active, ki_added, ki_note, k_count = res

    cur.execute('''SELECT tg_id, tg_name
                     FROM keywords_tags
                     JOIN tags USING (tg_id)
                    WHERE k_id = %s''', (k_id,))
    tags = cur.fetchall()

    # Which keyword does k_id most often refer to?
    cur.execute('''SELECT kt.k_id, k_pretty, COUNT(*) AS c
                     FROM keywords_related AS kr
                     JOIN keywords_and_topics AS kt ON kr.kr_kw = kt.k_id
                    WHERE kr.k_id = %s
                 GROUP BY kt.k_id, k_pretty
                 ORDER BY c DESC
                    LIMIT 1 ''', (k_id,))

    refers = cur.fetchone()

    # Which keyword refers most often to k_id?
    cur.execute('''SELECT kt.k_id, k_pretty, COUNT(*) AS c
                     FROM keywords_related AS kr
                     JOIN keywords_and_topics AS kt ON kr.k_id = kt.k_id
                    WHERE kr.kr_kw = %s
                 GROUP BY kt.k_id, k_pretty
                 ORDER BY c DESC
                    LIMIT 1 ''', (k_id,))

    referred = cur.fetchone()

    # How many links to how many distinct keywords are there for k_id?
    cur.execute('''SELECT COUNT(kr_kw), COUNT(DISTINCT kr_kw)
                     FROM keywords_related
                    WHERE k_id = %s''', (k_id,))

    outgoing = cur.fetchone()

    # How many links and how many distinct keywords point to k_id?
    cur.execute('''SELECT COUNT(k_id), COUNT(DISTINCT k_id)
                     FROM keywords_related
                    WHERE kr_kw = %s''', (k_id,))

    incoming = cur.fetchone()

    return render_template('keyword_detailed.html', k_id=k_id, k_q=k_q,
                           k_title=k_title, k_added=k_added, kt_name=kt_name,
                           ki_active=ki_active, ki_added=ki_added,
                           ki_note=ki_note, k_count=k_count, tags=tags,
                           refers=refers, referred=referred,
                           outgoing=outgoing, incoming=incoming)


@app.route("/keyword/<int:k_id>/requests")
def keyword_detailed_requests(k_id):
    """ Shows all requests for keyword with id k_id. """

    cur = con.cursor()
    cur.execute('''SELECT k_pretty
                     FROM keywords_and_topics
                    WHERE k_id = %s''', (k_id,))

    res = cur.fetchone()
    if res is None:
        abort(404, "No keyword with id {}".format(escape(k_id)))

    k_pretty = res[0]

    cur.execute('''SELECT r_id, t_id, r_tf_start, r_tf_end, l_id, l_iso
                     FROM trends_time
                     JOIN requests USING (r_id)
                     JOIN locations ON r_geo = l_id
                    WHERE k_id = %s''', (k_id,))

    timeframes = cur.fetchall()

    return render_template('keyword_detailed_requests.html', k_id=k_id,
                           k_pretty=k_pretty, timeframes=timeframes)


@app.route("/timeframes")
def timeframes():
    """ Shows all collected time frames. """

    cur = con.cursor()

    cur.execute('''SELECT t_id, k_pretty,
                          r_tf_start, r_tf_end, r_ts, r_id
                     FROM trends_time
                     JOIN keywords_and_topics USING (k_id)
                     JOIN requests USING (r_id)
                 ORDER BY t_id''')

    return render_template('timeframes.html', timeframes=cur.fetchall())


@app.route("/timeframe/<int:t_id>")
def timeframe_detailed(t_id):
    """ Shows details for time frame t_id. """
    cur = con.cursor()

    cur.execute('''SELECT r_id, k_pretty, k_id, t_v,
                       /* ^  0         1     2    3 */
                          r_tf_start, r_tf_end, l_iso, l_name
                       /* ^        4         5      6       7 */
                     FROM trends_time
                     JOIN requests USING (r_id)
                LEFT JOIN locations ON r_geo = l_id
                     JOIN keywords_and_topics USING (k_id)
                    WHERE t_id = %s''', (t_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no timeframe with id {}".format(escape(t_id)))

    if len(res[3]) > 0:
        plt = SvgPlot()

        tl = restore_timelabels(res[4], res[5], res[3])

        plt.plot(tl, res[3])
        plt.xticks([tl[0], tl[int(len(tl)/2)], tl[-1]])

        max_x, max_y = 0, 0
        for x, y in zip(tl, res[3]):
            if y == 100:
                max_x = x
                max_y = y
                break
        plt.annotate(max_x, (max_x, max_y))

        plot = plt.savefig()
    else:
        plot = None

    return render_template('timeframe_detailed.html', t_id=t_id, tf=res,
                           plt=plot)


@app.route("/requests")
def requests():
    """ Shows all requests in the database. """
    cur = con.cursor()

    status = request.args.get('status', None)

    q = cur.mogrify('''SELECT r_id, loc1.l_iso, loc1.l_name, r_tf_start,
                           /* ^  0           1            2           3 */
                              r_tf_end, rs_name, k_pretty, k_id
                           /* ^      4        5         6     7*/
                         FROM requests
                    LEFT JOIN locations AS loc1 ON r_geo = loc1.l_id
                         JOIN request_status ON r_status = rs_id
                         JOIN keywords_in_request USING (r_id)
                         JOIN keywords_and_topics USING (k_id)''')
    if status:
        q += cur.mogrify(' WHERE rs_name = %s ', (status,))

    q += cur.mogrify('ORDER BY r_id''')

    cur.execute(q)
    res = cur.fetchall()

    cur.execute('SELECT rs_name FROM request_status')
    status_list = [x[0] for x in cur.fetchall()]

    return render_template('requests.html', requests=res, status=status,
                           status_list=status_list)


@app.route("/request/<int:r_id>")
def request_detailed(r_id):
    """ Shows details about request r_id. """
    cur = con.cursor()

    cur.execute('''SELECT rw_name, r_when, rt_type, api1.ra_name, r_notbefore,
                       /* ^     0       1        2             3            4 */
                          r_notafter, r_prio, loc1.l_iso, loc1.l_name,
                       /* ^        5       6           7            8 */
                          r_tf_start, r_tf_end, rs_name, r_ts, f_name, f_host,
                       /* ^        9        10       11    12      13      14 */
                          r_note, k_pretty, k_id, api2.ra_name
                       /* ^   15        16    17            18 */
                         FROM requests
                         JOIN request_who ON r_who = rw_id
                         JOIN request_types ON r_type = rt_id
                         JOIN request_api AS api1 ON r_use = api1.ra_id
                    LEFT JOIN locations AS loc1 ON r_geo = loc1.l_id
                         JOIN request_status ON r_status = rs_id
                    LEFT JOIN fetchers ON r_fetcher = f_id
                    LEFT JOIN request_api AS api2 ON fetchers.ra_id = api2.ra_id
                         JOIN keywords_in_request USING (r_id)
                         JOIN keywords_and_topics USING (k_id)
                        WHERE r_id = %s''', (r_id,))

    req = cur.fetchone()
    if req is None:
        abort(404, "No request with id {}".format(escape(r_id)))

    cur.execute('''SELECT string_agg(CAST (t_id AS TEXT), ',')
                     FROM trends_time
                    WHERE r_id = %s''', (r_id,))
    trends_time = cur.fetchone()[0]

    cur.execute('''SELECT l_name, l_iso, k_pretty, gs_name, g_v
                     FROM trends_geo
                     JOIN locations USING (l_id)
                     JOIN trends_geo_scopes USING (gs_id)
                     JOIN keywords_and_topics USING (k_id)
                    WHERE r_id = %s AND g_v != 0''', (r_id,))
    trends_geo = cur.fetchall()

    cur.execute('''SELECT b.k_pretty, a.k_pretty, a.k_id, kr_istop
                     FROM keywords_related
                     JOIN keywords_and_topics AS a ON kr_kw = a.k_id
                     JOIN keywords_and_topics AS b
                       ON keywords_related.k_id = b.k_id
                    WHERE r_id = %s''', (r_id,))

    related = cur.fetchall()

    cur.execute('''SELECT pd_peak
                     FROM peaks_detailed_requests
                     JOIN peaks_detailed USING (pd_id)
                    WHERE r_id = %s''', (r_id,))
    pd_peak = cur.fetchone()
    if pd_peak:
        pd_peak = pd_peak[0]

    return render_template('request_detailed.html', r_id=r_id, r=req,
                           trends_time=trends_time,
                           trends_geo=trends_geo, related=related,
                           pd_peak=pd_peak)


@app.route("/tags")
def tags():
    """ Show keyword tags. """
    cur = con.cursor()

    cur.execute('''SELECT tg_id, tg_name, COUNT(*)
                     FROM tags
                LEFT JOIN keywords_tags USING (tg_id)
                 GROUP BY tg_id, tg_name
                 ORDER BY tg_id''')

    res = cur.fetchall()

    return render_template('tags.html', tags=res)


@app.route("/tag/<int:tg_id>")
def tag_detailed(tg_id):
    """ Shows detailed information about the keyword tag with id tg_id. """
    cur = con.cursor()

    cur.execute('''SELECT tg_name, tg_description, tg_added
                     FROM tags
                    WHERE tg_id = %s''', (tg_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no tag with id {}".format(escape(tg_id)))

    tg_name, tg_description, tg_added = res

    cur.execute('''SELECT k_id, k_pretty
                     FROM keywords_tags
                LEFT JOIN keywords_and_topics USING(k_id)
                    WHERE tg_id = %s''', (tg_id,))

    res = cur.fetchall()

    return render_template('tag_detailed.html', tg_id=tg_id, tg_name=tg_name,
                           tg_description=tg_description, tg_added=tg_added,
                           keywords=res)


@app.route("/topics")
def topics():
    """ List of keywords topics. """
    cur = con.cursor()

    cur.execute('''SELECT kt_id, kt_name, COUNT(*)
                     FROM keyword_topics
                LEFT JOIN keywords USING (kt_id)
                 GROUP BY kt_id, kt_name
                 ORDER BY COUNT(*) DESC''')

    res = cur.fetchall()

    return render_template('topics.html', topics=res)


@app.route("/topic/<int:kt_id>")
def topic_detailed(kt_id):
    """ Shows details about the topic with id kt_id. """
    cur = con.cursor()

    cur.execute('''SELECT kt_name
                     FROM keyword_topics
                    WHERE kt_id = %s''', (kt_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no topic with id {}".format(escape(kt_id)))

    kt_name = res[0]

    cur.execute('''SELECT k_id, k_pretty
                     FROM keywords_and_topics
                    WHERE kt_id = %s''', (kt_id,))

    res = cur.fetchall()

    return render_template('topic_detailed.html', kt_id=kt_id,
                           kt_name=kt_name, keywords=res)


@app.route("/locations")
def locations():
    """ List all locations. """
    cur = con.cursor()

    cur.execute('SELECT l_id, l_iso, l_name FROM locations')

    res = cur.fetchall()

    return render_template('locations.html', locations=res)


@app.route("/location/<int:l_id>")
def location_detailed(l_id):
    """ Shows details about a location. """
    cur = con.cursor()

    cur.execute('SELECT l_iso, l_name FROM locations WHERE l_id = %s', (l_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no location with id {}".format(escape(l_id)))

    l_iso = res[0]
    l_name = res[1]

    reqs = db_one(cur, 'SELECT COUNT(*) FROM requests WHERE r_geo = %s', (l_id,))
    refs = db_one(cur, 'SELECT COUNT(*) FROM trends_geo WHERE l_id = %s', (l_id,))

    return render_template('location_detailed.html', l_id=l_id, l_iso=l_iso,
                           l_name=l_name, refs=refs, reqs=reqs)


@app.route("/location/<int:l_id>/requests")
def location_detailed_requests(l_id):
    """ List requests for location with id l_id. """
    cur = con.cursor()

    cur.execute('SELECT l_iso, l_name FROM locations WHERE l_id = %s', (l_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no location with id {}".format(escape(l_id)))

    l_iso = res[0]
    l_name = res[1]

    cur.execute('''SELECT r_id, k_id, k_pretty, r_tf_start, r_tf_end
                     FROM requests
                     JOIN keywords_in_request USING (r_id)
                     JOIN keywords_and_topics USING (k_id)
                    WHERE r_geo = %s''',
                (l_id,))

    requests = cur.fetchall()

    return render_template('location_detailed_requests.html', l_id=l_id,
                           l_iso=l_iso, l_name=l_name, requests=requests)


@app.route("/location/<int:l_id>/referenced")
def location_detailed_referenced(l_id):
    """ List requests which have values for the location with id l_id. """
    cur = con.cursor()

    cur.execute('SELECT l_iso, l_name FROM locations WHERE l_id = %s', (l_id,))

    res = cur.fetchone()

    if res is None:
        abort(404, "no location with id {}".format(escape(l_id)))

    l_iso = res[0]
    l_name = res[1]

    cur.execute('''SELECT r_id, k_id, k_pretty, r_tf_start, r_tf_end
                     FROM trends_geo
                     JOIN requests USING (r_id)
                     JOIN keywords_and_topics USING (k_id)
                    WHERE l_id = %s''',
                (l_id,))

    referenced = cur.fetchall()

    return render_template('location_detailed_referenced.html', l_id=l_id,
                           l_iso=l_iso, l_name=l_name, referenced=referenced)


@app.route("/stitch")
def stitch():
    """ Shows the stitched time series at different locations for a keyword. """

    c = open_time_series_db()

    cur = con.cursor()

    states = [x[0] for x in c.execute('SELECT DISTINCT state FROM ts ORDER BY state')]
    k_ids = [x[0] for x in c.execute('SELECT DISTINCT k_id FROM ts ORDER BY k_id')]

    k_id = request.args.get('k_id', k_ids[0])

    cur.execute('SELECT k_id, k_pretty FROM keywords_and_topics WHERE k_id IN %s',
                (tuple(k_ids),))
    keywords = {x[0]: x[1] for x in cur}

    return render_template('stitch.html', states=states, keywords=keywords,
                           k_id=k_id)


# Specked down version of /stitch code
def make_plot(plt, k_id, geo, start, end):
    """
        Plots a time series with plt for keyword k_id and region geo.
        start and end are either both None, or the region for which
        the time series should be plotted.  start and end should be
        of the form YYYY-MM-DD HH:MM.
    """

    c = open_time_series_db()

    assert (start is None and end is None) or (start is not None and end is not None)

    if start is None:
        res = c.execute('''SELECT datetime(time, 'unixepoch'), value
                             FROM ts
                            WHERE k_id = ? AND state = ?
                         ORDER BY time ASC''', (k_id, geo))
    else:
        res = c.execute('''SELECT datetime(time, 'unixepoch'), value
                             FROM ts
                            WHERE k_id = ? AND state = ?
                              AND time >= strftime('%s', ?)
                              AND time <= strftime('%s', ?)
                         ORDER BY time ASC''', (k_id, geo, start, end))

    times, values = [], []
    for t, v in res.fetchall():
        times.append(datetime.datetime.fromisoformat(t))
        values.append(v)

    if len(values) == 0:
        return plt

    m = max(values)
    series = [x / m * 100 for x in values]

    plt.plot(times, series)
    plt.xlim(start, end)

    return plt


@app.route("/ts")
def ts():
    """
        Returns a PNG image of the time series for k_id and iso (query
        parameters).
    """

    if 'k_id' not in request.args or 'iso' not in request.args:
        return render_template('empty.html', title='400 Bad Request',
                               msg='k_id and/or iso query parameters missing'), 400

    k_id = request.args['k_id']
    iso = request.args['iso']

    plt = make_plot(PngPlot(), k_id, iso, None, None)

    resp = make_response(plt.savefig(), 200)
    resp.headers['Content-Type'] = "image/png"
    return resp


def render_overlap(tl_a, ts_a, tl_b, ts_b):
    plots = []
    plt = SvgPlot()

    plt.plot(tl_a, ts_a)
    plt.plot(tl_b, ts_b)

    plots.append(plt.savefig())
    plt.clf()

    ts1 = dict(zip(tl_a, ts_a))
    ts2 = dict(zip(tl_b, ts_b))

    overlap = set(ts1.keys()) & set(ts2.keys())

    if len(overlap) == 0:
        return plots

    max_left = max([ts1[k] for k in overlap])
    max_right = max([ts2[k] for k in overlap])

    if max_left == 0 or max_right == 0:
        return plots

    scale = max_left/max_right

    plt.plot(tl_a, ts_a)
    b2p = [x * scale for x in ts_b]
    plt.plot(tl_b, b2p)
    plots.append(plt.savefig())
    plt.clf()

    ok = sorted(overlap)
    o1 = []
    o2 = []
    for o in ok:
        o1.append(ts1[o])
        o2.append(ts2[o] * scale)

    plt.plot(ok, o1)
    plt.plot(ok, o2)
    plots.append(plt.savefig())
    plt.clf()

    return plots


@app.route("/overlap")
def overlap():
    """
        View to inspect the overlap computation between individual time frames.
    """
    cur = con.cursor()

    time = datetime.datetime.fromisoformat('2019-06-12')
    geo = request.args.get('geo', 'US-CA')
    kw = 1
    r_A, r_B = None, None

    if 'time' in request.args:
        time = datetime.datetime.fromisoformat(request.args['time'])

    if 'kw' in request.args:
        kw = int(request.args['kw'])

    if 'r_A' in request.args:
        r_A = int(request.args['r_A'])

        if 'r_B' not in request.args:
            abort(404, 'r_A and r_B must be specified together')
        r_B = int(request.args['r_B'])

    cur.execute('SELECT k_pretty FROM keywords_and_topics WHERE k_id = %s',
                (kw,))
    res = cur.fetchone()

    if res is None:
        abort(404, 'invalid keyword id {}'.format(escape(kw)))

    kw_pretty = res[0]

    if geo is not None:
        cur.execute('''SELECT l_name || ' (' || l_iso || ')'
                         FROM locations
                        WHERE l_iso = %s''', (geo,))
        res = cur.fetchone()
        if res is None:
            abort(404, 'no location {}'.format(escape(geo)))

        loc = res[0]
        assert loc is not None
    else:
        loc = 'world'

    q = cur.mogrify('''SELECT DISTINCT k_id, k_pretty
                         FROM requests
                         JOIN keywords_in_request USING (r_id)
                         JOIN keywords_and_topics USING (k_id)
                        WHERE r_tf_start < %s AND %s < r_tf_end''',
                    (time, time))
    if geo is not None:
        q += cur.mogrify(' AND r_geo = (SELECT l_id FROM locations WHERE l_iso = %s)', (geo,))
    else:
        q += cur.mogrify(' AND r_geo IS NULL')

    cur.execute(q)
    kws = cur.fetchall()

    cur.execute('''SELECT DISTINCT l_iso
                     FROM requests
                     JOIN keywords_in_request USING (r_id)
                     JOIN locations ON r_geo = l_id
                    WHERE r_tf_start < %s AND %s < r_tf_end
                      AND k_id = %s ''', [time, time, kw])
    geos = cur.fetchall()

    q = cur.mogrify('''SELECT r_tf_start, r_tf_end, t_v, r_id
                         FROM requests
                         JOIN trends_time USING (r_id)
                    LEFT JOIN locations ON r_geo = l_id''')
    if geo is None:
        q += cur.mogrify(' WHERE r_geo IS NULL')
    else:
        q += cur.mogrify(' WHERE l_iso = %s', (geo,))
    q += cur.mogrify('''     AND k_id = %s''', (kw,))
    q += cur.mogrify('''     AND r_tf_end - r_tf_start = interval '7 days'
                             AND r_tf_end > timestamp %s - interval '7 days'
                             AND r_tf_start < timestamp %s + interval '7 days'
                        ORDER BY r_tf_start''', (time, time))

    cur.execute(q)

    res = cur.fetchall()

    i, j = -1, -1

    if len(res) >= 2:
        if len(res) == 2:
            i, j = 0, 1
        elif r_A is not None:
            for k, r in enumerate(res):
                if r[3] == r_A:
                    i = k
                elif r[3] == r_B:
                    j = k
        else:
            # Take the first timeframe containing time and the one
            # following it.  If the first timeframe containing time is
            # the last, take the one before.  If there is no timeframe
            # containing time, take the first two in res.
            for k, r in enumerate(res):
                if r[0] < time < r[1]:
                    i = k
                elif i != -1:
                    j = k
                    break
            if i == -1:
                i, j = 0, 1
            elif j == -1:
                j = i - 1

                assert i != j

        assert 0 <= i < len(res) and 0 <= j < len(res)

        if j < i:
            i, j = j, i

        start_a, end_a, ts_a, r_a = res[i]
        start_b, end_b, ts_b, r_b = res[j]

        r_A = r_a
        r_B = r_b

        l1 = restore_timelabels(start_a, end_a, ts_a)
        l2 = restore_timelabels(start_b, end_b, ts_b)

        plots = render_overlap(l1, ts_a, l2, ts_b)
    else:
        plots = []
        return render_template('empty.html', title='Overlap',
                               msg='no data at this point (or only a single time frame)')

    min_t = min(map(lambda r: r[0], res))
    max_t = max(map(lambda r: r[1], res))
    plt = make_plot(SvgPlot(height=5), kw, geo, None, None)
    plt.vlines(min_t, 0, 10, 'red')
    plt.vlines(max_t, 0, 10, 'red')
    plt2 = make_plot(SvgPlot(height=5), kw, geo, min_t, max_t)
    y = 102
    for start, end, _, r_id in res:
        color = 'gray'
        if r_id in (r_A, r_B):
            color = 'red'

        # plt.hlines(y, start, end, color)
        # plt.annotate(str(r_id), (start, y + 2))

        plt2.hlines(y, start, end, color)
        plt2.annotate(str(r_id), (start, y + 2))

        y += 10

    assert len(plots) in (0, 1, 3)

    return render_template('overlap.html', res=res, time=time,
                           geo=geo, kw=kw, kw_pretty=kw_pretty,
                           loc=loc, week=datetime.timedelta(weeks=1),
                           plots=plots, i=i, j=j, kws=kws, geos=geos,
                           overview=plt.savefig(),
                           overview2=plt2.savefig())


@app.route("/csv")
def csv():
    """ Returns the time series for k_id and iso (query parameters) as csv file. """

    c = open_time_series_db()

    if 'k_id' not in request.args or 'iso' not in request.args:
        return render_template('empty.html', title='400 Bad Request',
                               msg='k_id and/or iso query parameters missing'), 400

    k_id = request.args['k_id']
    iso = request.args['iso']

    res = c.execute('SELECT time, value FROM ts WHERE k_id = ? AND state = ?',
                    (k_id, iso)).fetchall()

    csv = "time,value\n"
    for t, v in res:
        csv += "{},{}\n".format(t, v)

    start, end = c.execute('''SELECT date(MIN(time), 'unixepoch'),
                                     date(MAX(time), 'unixepoch')
                                FROM ts
                               WHERE k_id = ? AND state = ?''', (k_id, iso)).fetchone()

    filename = '{}_{}_{}_{}.csv'.format(start, end, iso, k_id)
    resp = make_response(csv, 200)
    resp.headers['Content-Type'] = "text/csv"
    resp.headers['Content-Disposition'] = 'inline; filename={}'.format(filename)

    return resp


def getkw(k_id):
    """ Gets the k_pretty for k_id. """
    cur = con.cursor()
    cur.execute('SELECT k_pretty FROM keywords_and_topics WHERE k_id = %s', (k_id,))
    return cur.fetchone()[0]


def keywords_by_rid(r_id):
    """ Returns all keywords present in the request with id r_id. """
    cur = con.cursor()

    q = cur.mogrify('''SELECT k_id, k_pretty,
                              string_agg(istop, ',' ORDER BY istop)
                         FROM (SELECT DISTINCT kt.k_id AS k_id, k_pretty,
                                      CASE WHEN kr_istop THEN 'top'
                                           ELSE 'rising' END AS istop
                                FROM requests
                                JOIN keywords_related AS kr USING (r_id)
                                JOIN keywords_and_topics AS kt
                                  ON kr_kw = kt.k_id
                               WHERE r_id = %s) AS k
                      GROUP BY k_id, k_pretty''', [r_id])

    cur.execute(q)

    return cur.fetchall()


def diff_keywords(set_a, set_b):
    """
    Computes the differences between two set of keywords.

    format of set_a and set_b:
        k_id, k_pretty, ("top"|"rising"|"top,rising")
    """
    kw_a = {k[0]: (k[1], k[2]) for k in set_a}
    kw_b = {k[0]: (k[1], k[2]) for k in set_b}

    # k_id k_pretty in_a in_b c_a c_b
    keywords = []
    for k in set(kw_a.keys()) | set(kw_b.keys()):
        in_a = k in kw_a
        in_b = k in kw_b
        c_a = None
        c_b = None
        k_pretty = None
        if in_a:
            k_pretty = kw_a[k][0]
            c_a = kw_a[k][1]
        if in_b:
            k_pretty = kw_b[k][0]
            c_b = kw_b[k][1]

        assert k_pretty is not None
        assert (in_a and c_a) or (not in_a and not c_a)
        assert (in_b and c_b) or (not in_b and not c_b)
        assert in_a or in_b

        keywords.append((k, k_pretty, in_a, in_b, c_a, c_b))

    def kw_sort(k):
        z = 0
        if k[2] and not k[3]:
            z = -1
        elif not k[2] and k[3]:
            z = 1
        return (z, k[1])

    return sorted(keywords, key=kw_sort)


def get_rid_info(cur, r_id):
    try:
        r_id = int(r_id)
    except ValueError:
        return r_id, None, 'r_id must be integer'

    cur.execute('''SELECT k_id, k_pretty, l_iso, r_tf_start, r_tf_end
                     FROM requests
                     JOIN keywords_in_request USING (r_id)
                     JOIN keywords_and_topics USING (k_id)
                LEFT JOIN locations ON r_geo = locations.l_id
                    WHERE r_id = %s''', (r_id,))

    res = cur.fetchone()

    if res is None:
        return r_id, None, 'no request with that id'

    time = '{} - {}'.format(res[3].strftime('%Y-%m-%d %H:%M:%S'),
                            res[4].strftime('%Y-%m-%d %H:%M:%S'))

    d = {
        'k_id': res[0],
        'k_pretty': res[1],
        'geo': res[2],
        'time': time,
    }

    return r_id, d, None


@app.route("/keyword_diff")
def keyword_diff():
    """ Shows the difference in recommended keywords between two requests """
    cur = con.cursor()

    a_id, a, a_error = get_rid_info(cur, request.args.get('a_id', ''))
    b_id, b, b_error = get_rid_info(cur, request.args.get('b_id', ''))

    if a_error or b_error:
        return render_template('keyword_diff_form.html', a_id=a_id, b_id=b_id,
                               a_error=a_error, b_error=b_error)

    kw_a = keywords_by_rid(a_id)
    kw_b = keywords_by_rid(b_id)

    return render_template('keyword_diff.html', a_id=a_id, a=a, b_id=b_id, b=b,
                           kws=diff_keywords(kw_a, kw_b))


@app.route("/keyword_statistics")
def keyword_statistics():
    """ Displays statistics about keywords. """
    cur = con.cursor()

    rising = 'rising' in request.args

    q = cur.mogrify('''SELECT kt.k_id, k_pretty,
                              COUNT(CASE WHEN kr_istop THEN 1 END) AS c,
                              COUNT(CASE WHEN NOT kr_istop THEN 1 END) AS c2
                         FROM keywords_related
                         JOIN keywords_and_topics kt ON kt.k_id = kr_kw
                     GROUP BY kt.k_id, k_pretty''')

    if rising:
        q += cur.mogrify(' ORDER BY c2 DESC, k_pretty''')
    else:
        q += cur.mogrify(' ORDER BY c DESC, k_pretty''')

    q += cur.mogrify(' LIMIT 1000')
    cur.execute(q)
    kws = cur.fetchall()

    cur.execute('''SELECT c, COUNT(*)
                     FROM (
                           SELECT k_pretty, COUNT(*) AS c
                             FROM keywords_related
                             JOIN keywords_and_topics
                               ON keywords_and_topics.k_id = kr_kw
                         GROUP BY k_pretty
                         ORDER BY c DESC, k_pretty
                     ) AS k
                 GROUP BY c
                 ORDER BY c''')

    buckets = {}
    for c, count in cur.fetchall():
        k = None
        if c < 10:
            k = c
        elif 10 <= c < 100:
            d = c // 10
            lo = d * 10
            hi = ((d + 1) * 10) - 1
            k = '{} - {}'.format(lo, hi)
        elif 100 <= c < 1000:
            d = c // 100
            lo = d * 100
            hi = ((d + 1) * 100) - 1
            k = '{} - {}'.format(lo, hi)
        elif c >= 1000:
            k = '1000+'

        if k not in buckets:
            buckets[k] = 0

        buckets[k] += count

    kwstat = buckets

    cur.execute('''SELECT k_pretty, array_agg(k_id), array_agg(k_q)
                     FROM keywords_and_topics
                 GROUP BY k_pretty
                   HAVING COUNT(*) > 1
                 ORDER BY COUNT(*) DESC''')

    kwdouble = []
    for k_pretty, k_ids, k_qs in cur.fetchall():
        kwdouble.append((k_pretty, zip(k_ids, k_qs)))

    return render_template('keyword_statistics.html', rising=rising, kws=kws,
                           kwstat=kwstat, kwdouble=kwdouble)


@app.route("/help")
def help_page():
    """ Returns documentation for the different web pages. """
    rules = []
    for rule in app.url_map.iter_rules():
        func = app.view_functions[rule.endpoint]
        rules.append((rule.rule, rule.endpoint, func.__doc__))
    return render_template('help.html', rules=rules)


@app.route("/ignored_keywords")
def ignored_keywords():
    """ Shows all the keywords which are ignored for labelling. """
    cur = con.cursor()

    cur.execute('''SELECT k_id, k_pretty
                     FROM keywords_ignore
                     JOIN keywords_and_topics USING (k_id)''')
    return render_template('ignored_keywords.html', ignored=cur.fetchall())


@app.route("/timeframe_search")
def timeframe_search():
    """ Search timeframes around a certain point in time. """
    cur = con.cursor()

    time = request.args.get('time', None)
    if time:
        time = datetime.datetime.fromisoformat(time)
    else:
        time = ""
    k_id = request.args.get('k_id', None)
    if k_id:
        k_id = int(k_id)
    geo = request.args.get('geo', None)
    duration = request.args.get('duration', None)
    results = None

    if time or k_id or geo:
        q = cur.mogrify('''SELECT r_tf_start, r_tf_end, t_id,
                                  array_length(t_v, 1) IS NOT NULL, k_pretty,
                                  coalesce(l_iso, 'world') l
                             FROM requests
                             JOIN trends_time USING (r_id)
                        LEFT JOIN locations ON r_geo = l_id
                             JOIN keywords_and_topics USING (k_id)
                            WHERE true ''')
        if time:
            q += cur.mogrify(' AND r_tf_start <= %s AND r_tf_end >= %s ',
                             (time, time))

        if k_id:
            q += cur.mogrify(' AND k_id = %s', (k_id,))

        if geo == 'world':
            q += cur.mogrify(' AND r_geo IS NULL')
        elif geo:
            q += cur.mogrify(' AND l_iso = %s', (geo,))

        if duration:
            duration = int(duration)
            q += cur.mogrify(''' AND r_tf_end - r_tf_start >
                                     (%s ::text || ' seconds') :: interval''',
                             (duration,))

        q += cur.mogrify(' ORDER BY r_tf_start ASC')
        cur.execute(q)

        results = cur.fetchall()

    cur.execute('''SELECT l_iso, l_name
                     FROM locations
                    WHERE l_iso LIKE 'US%' ''')
    locations = cur.fetchall()

    cur.execute('''SELECT k_id, k_pretty
                     FROM keywords_and_topics
                    WHERE k_id IN
                          (SELECT DISTINCT k_id FROM keywords_in_request)''')

    keywords = cur.fetchall()

    return render_template('timeframe_search.html', time=time, k_id=k_id,
                           geo=geo, duration=duration,
                           results=results, locations=locations,
                           keywords=keywords)


@app.route("/duplicates")
def duplicates():
    """ Shows all duplicate time frames for a location and keywords. """
    cur = con.cursor()

    k_id = request.args.get('k_id', None, int)
    geo = request.args.get('geo', None)

    q = cur.mogrify('''SELECT r_tf_start, r_tf_end, l_iso, k_id,
                              MIN(k_pretty), array_agg(r_id)
                         FROM requests
                    LEFT JOIN locations ON r_geo = l_id
                         JOIN keywords_in_request USING (r_id)
                         JOIN keywords_and_topics USING (k_id)
                        WHERE true''')

    if k_id:
        q += cur.mogrify('''
                          AND k_id = %s''', (k_id,))

    if geo:
        q += cur.mogrify('''
                          AND l_iso = %s''', (geo,))

    q += cur.mogrify('''
                     GROUP BY r_tf_start, r_tf_end, l_iso, k_id
                       HAVING COUNT(r_id) > 1
                     ORDER BY l_iso, r_tf_start, r_tf_end, k_id''')
    cur.execute(q)
    res = cur.fetchall()

    return render_template('duplicates.html', res=res)


@app.route("/duplicate_compare")
def duplicate_compare():
    """ Allows closer inspection of duplicates. """
    cur = con.cursor()

    start = datetime.datetime.fromisoformat(request.args['start'])
    end = datetime.datetime.fromisoformat(request.args['end'])
    k_id = request.args['k_id']
    iso = request.args['iso']

    cur.execute('''SELECT r_id, r_ts, t_v
                     FROM requests
                LEFT JOIN locations ON r_geo = l_id
                     JOIN trends_time USING (r_id)
                    WHERE r_tf_start = %s AND r_tf_end = %s
                      AND k_id = %s AND l_iso = %s''',
                [start, end, k_id, iso])

    res = cur.fetchall()

    plt = SvgPlot()

    for r_id, _, t_v in res:
        if len(t_v) == 0:
            continue

        tl = restore_timelabels(start, end, t_v)
        plt.plot(tl, t_v, label=str(r_id))

    cur.execute('''SELECT r_tf_start, r_tf_end, t_v
                     FROM requests
                     JOIN trends_time USING (r_id)
                LEFT JOIN locations ON r_geo = l_id
                    WHERE k_id = %s AND l_iso = %s
                      AND r_tf_end - r_tf_start > '7 days' ''',
                [k_id, iso])

    tfs = []
    for tf_start, tf_end, t_v in cur:
        tl = restore_timelabels(tf_start, tf_end, t_v)
        tfs.append((tl, t_v))

    labels, values = stitch_timeframes(tfs)
    ts = dict(zip(labels, values))

    for key in list(ts):
        if key < start or key > end:
            del ts[key]

    labels = sorted(ts)
    values = [ts[k] for k in labels]

    plt.plot(labels, values, label="daily")
    plt.legend()
    daily = plt.savefig()

    return render_template('duplicate_compare.html', start=start, end=end,
                           k_id=k_id, k_pretty=getkw(k_id), iso=iso,
                           res=res, daily=daily)


@app.route("/rcomment")
def rcomment():
    """
        Lists all distinct r_note values.  If a query parameter q is
        given, list all request ids where r_note is q as well.
    """
    cur = con.cursor()

    res = None
    q = None
    if 'q' in request.args:
        q = request.args['q']
        cur.execute('SELECT r_id FROM requests WHERE r_note = %s', (q,))
        res = [x[0] for x in cur]

    cur.execute('''SELECT r_note, COUNT(*)
                     FROM requests
                    WHERE r_note IS NOT NULL
                 GROUP BY r_note
                 ORDER BY r_note''')

    notes = cur.fetchall()

    return render_template('rcomment.html', res=res, notes=notes, q=q)
