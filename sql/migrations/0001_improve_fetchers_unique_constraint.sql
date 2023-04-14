ALTER TABLE fetchers ADD CONSTRAINT fetchers_f_name_f_host_ra_id_key UNIQUE(f_name, f_host, ra_id);
ALTER TABLE fetchers DROP CONSTRAINT fetchers_f_name_key;
