CREATE TABLE request_who (
    rw_id SERIAL PRIMARY KEY,
    rw_name TEXT NOT NULL UNIQUE
);
CREATE TABLE request_types (
    rt_id SERIAL PRIMARY KEY,
    rt_type TEXT NOT NULL UNIQUE
);
INSERT INTO request_types (rt_type) VALUES ('all'), ('geo'), ('time'), ('related');
CREATE TABLE request_api (
    ra_id SERIAL PRIMARY KEY,
    ra_name TEXT NOT NULL UNIQUE
);
INSERT INTO request_api (ra_name) VALUES ('any'), ('pytrends'), ('google');
CREATE TABLE locations (
    l_id SERIAL PRIMARY KEY,
    l_iso TEXT NOT NULL UNIQUE,
    l_name TEXT NOT NULL
);
CREATE TABLE request_status (
    rs_id SERIAL PRIMARY KEY,
    rs_name TEXT NOT NULL UNIQUE
);
INSERT INTO request_status (rs_name) VALUES ('open'), ('running'), ('done'), ('error');
CREATE TABLE fetchers (
    f_id SERIAL PRIMARY KEY,
    f_name TEXT NOT NULL UNIQUE,
    f_host TEXT NOT NULL,
    ra_id INTEGER REFERENCES request_api (ra_id) NOT NULL
);
CREATE TABLE requests (
    r_id SERIAL PRIMARY KEY,
    r_who INTEGER REFERENCES request_who (rw_id) NOT NULL, /* Who submitted this request */
    r_when TIMESTAMP NOT NULL, /* When was this request submitted */
    r_type INTEGER REFERENCES request_types (rt_id) NOT NULL, /* geo, time, related */
    r_use INTEGER REFERENCES request_api (ra_id) NOT NULL, /* API to use */
    r_notbefore TIMESTAMP NOT NULL, /* Wait until that point before considering the request */
    r_notafter TIMESTAMP NOT NULL, /* After that point, don't bother */
    r_prio INTEGER NOT NULL, /* Priority, higher more important */
    r_geo INTEGER REFERENCES locations (l_id), /* NULL means no restriction */
    r_tf_start TIMESTAMP NOT NULL, /* Timeframe start */
    r_tf_end TIMESTAMP NOT NULL, /* Timeframe end */
    r_status INTEGER REFERENCES request_status (rs_id) NOT NULL,
    r_ts TIMESTAMP, /* Time the request was completed/done */
    r_fetcher INTEGER REFERENCES fetchers (f_id),
    /* r_api INTEGER REFERENCES request_api (ra_id), tracked by fetchers table */
    r_note TEXT,
    CHECK(r_tf_start < r_tf_end)
);
CREATE TABLE keyword_topics (
    kt_id SERIAL PRIMARY KEY,
    kt_name TEXT NOT NULL UNIQUE
);
CREATE TABLE keywords (
    k_id SERIAL PRIMARY KEY,
    k_q TEXT NOT NULL UNIQUE,
    k_title TEXT,
    kt_id INTEGER REFERENCES keyword_topics,
    k_added TIMESTAMP NOT NULL,
    /* UNIQUE(k_title, kt_id), removed 2021-11-18 */
    CHECK((k_title IS NULL AND kt_id IS NULL) OR
	  (k_title IS NOT NULL AND kt_id IS NOT NULL))
);
CREATE TABLE keywords_info (
    ki_id SERIAL PRIMARY KEY,
    k_id INTEGER REFERENCES keywords(k_id) NOT NULL,
    ki_added TIMESTAMP NOT NULL,
    ki_active BOOLEAN NOT NULL,
    ki_note TEXT
    , UNIQUE (k_id) /* Added 2021-11-25 */
);
CREATE TABLE keywords_in_request (
    k_id INTEGER REFERENCES keywords(k_id) NOT NULL,
    r_id INTEGER REFERENCES requests(r_id) NOT NULL,
    UNIQUE(k_id, r_id)
);
CREATE TABLE keywords_related (
    kr_id SERIAL PRIMARY KEY,
    kr_istop BOOLEAN NOT NULL, /* !istop == rising */
    r_id INTEGER REFERENCES requests(r_id) NOT NULL,
    k_id INTEGER REFERENCES keywords(k_id) NOT NULL, /* This keyword was recommended in response of */
    kr_kw INTEGER REFERENCES keywords(k_id) NOT NULL, /* The keyword that was recommended */
    kr_v INTEGER NOT NULL
);
CREATE TABLE trends_time (
    t_id SERIAL PRIMARY KEY,
    r_id INTEGER REFERENCES requests(r_id) NOT NULL,
    k_id INTEGER REFERENCES keywords(k_id) NOT NULL,
    t_v INTEGER[] NOT NULL,
    t_ts TEXT[],
    UNIQUE(r_id, k_id), /* A keyword can only appear once per request */
    CHECK(t_ts IS NULL OR coalesce(array_length(t_v, 0), 0) = coalesce(array_length(t_ts, 0), 0))
);
CREATE TABLE trends_geo_scopes (
    gs_id SERIAL PRIMARY KEY,
    gs_name TEXT NOT NULL UNIQUE
);
INSERT INTO trends_geo_scopes (gs_name) VALUES ('country'), ('states'), ('region'), ('dma');
CREATE TABLE trends_geo (
    g_id SERIAL PRIMARY KEY,
    r_id INTEGER REFERENCES requests(r_id) NOT NULL,
    l_id INTEGER REFERENCES locations(l_id) NOT NULL,
    k_id INTEGER REFERENCES keywords(k_id) NOT NULL,
    gs_id INTEGER REFERENCES trends_geo_scopes(gs_id) NOT NULL,
    g_v INTEGER NOT NULL,
    UNIQUE(r_id, l_id, k_id) /* A location can only appear once per keyword in a request */
);
CREATE TABLE log (
    log_id SERIAL PRIMARY KEY,
    log_ts TIMESTAMP NOT NULL,
    log_msg TEXT NOT NULL
);
CREATE TABLE raw_fetcher_output (
    rfo_id SERIAL PRIMARY KEY,
    rfo_data TEXT NOT NULL,
    f_id INTEGER REFERENCES fetchers (f_id) NOT NULL,
    r_id INTEGER REFERENCES requests (r_id) NOT NULL,
    k_id INTEGER REFERENCES keywords (k_id) NOT NULL,
    rfo_ts TIMESTAMP NOT NULL
);

/* Added on December 16th, 2021 */
CREATE TABLE tags (
       tg_id SERIAL PRIMARY KEY,
       tg_name TEXT NOT NULL UNIQUE,
       tg_description TEXT NOT NULL,
       tg_added TIMESTAMP NOT NULL
);
CREATE TABLE keywords_tags (
       k_id INTEGER REFERENCES keywords(k_id) NOT NULL,
       tg_id INTEGER REFERENCES tags(tg_id) NOT NULL,
       UNIQUE(k_id, tg_id)
);

/* Added on March 22nd, 2022 */

CREATE TABLE peaks_detailed (
       pd_id SERIAL PRIMARY KEY,
       pd_peak TIMESTAMP NOT NULL,
       l_id INTEGER REFERENCES locations(l_id) NOT NULL,
       k_id INTEGER REFERENCES keywords(k_id) NOT NULL
);

CREATE TABLE peaks_detailed_requests (
       pdr_id SERIAL PRIMARY KEY,
       pd_id INTEGER REFERENCES peaks_detailed(pd_id) NOT NULL,
       r_id INTEGER REFERENCES requests(r_id) NOT NULL UNIQUE
);

/* Added on March 24th, 2022 */

CREATE TABLE keywords_ignore (
       k_id INTEGER REFERENCES keywords(k_id) UNIQUE NOT NULL
);

/* Added on May 6th, 2022 */

CREATE TABLE rtags (
       rtag_id SERIAL PRIMARY KEY,
       rtag_name TEXT NOT NULL UNIQUE
);

CREATE TABLE request_tags (
       rtag_id INTEGER REFERENCES rtags (rtag_id) NOT NULL,
       r_id INTEGER REFERENCES requests (r_id) NOT NULL,
       UNIQUE(rtag_id, r_id)
);

/* Views */
CREATE VIEW keywords_and_topics AS
    SELECT k_id, k_q, k_title, kt_id, kt_name, coalesce(k_title || ' (' || kt_name || ')', k_q) AS k_pretty
      FROM keywords
 LEFT JOIN keyword_topics USING (kt_id);

CREATE VIEW raw_fetcher_output_count AS
     SELECT COUNT(*) FROM raw_fetcher_output
