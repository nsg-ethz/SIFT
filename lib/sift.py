import datetime
import sqlite3


def stitch_timeframes(tfs, ignoreNoOverlap=False):
    """
        Stitches timeframes together.  tfs is a list of tuples
        (labels, values).  Prior to stitching, the tuples are sorted
        by the first element in labels.  Once sorted each timeframe
        must overlap with the consecutive timeframe, in adddition both
        timeframes must have one non-zero value in the overlap area.
        Returns the labels and values of the stitched time series.

        The method to stitch two consecutive time frames is the one from
        https://github.com/qztseng/google-trends-daily
    """

    # Adapted from layers.py
    labels = []
    values = []

    assert len(tfs) > 0

    for tf in sorted(tfs, key=lambda x: x[0][0]):
        tl = tf[0]
        tv = tf[1]

        assert len(tl) == len(tv)

        ts = dict(zip(tl, tv))

        if len(labels) == 0:
            labels = list(tl)
            values = list(tv)
            continue

        overlap_keys = set(tl) & set(labels)

        assert ignoreNoOverlap or len(overlap_keys) > 0

        s = dict(zip(labels, values))

        if ignoreNoOverlap and len(overlap_keys) == 0:
            scale = 1
        elif max(ts[key] for key in overlap_keys) == 0:
            if not ignoreNoOverlap:
                return None, None
            else:
                scale = 1
        else:
            scale = max(s[key] for key in overlap_keys) / max(ts[key] for key in overlap_keys)

        assert scale != 0

        for l, v in zip(tl, tv):
            if l in overlap_keys:
                continue
            labels.append(l)
            values.append(v * scale)

    assert len(labels) > 0
    assert len(labels) == len(values)

    m = max(values)
    values = [100 * x / m for x in values]
    return labels, values


class RestoreTimelabelsError(Exception):
    def __init__(self):
        pass


def restore_timelabels(start, end, tf):
    """
        Restores the timelabels of the values in tf.
    """

    # When no values are returned
    if len(tf) == 0:
        return []

    tl = []
    if end - start == datetime.timedelta(hours=4) and len(tf) == 241:
        for i in range(241):
            tl.append(start + datetime.timedelta(minutes=i))

    elif end-start == datetime.timedelta(days=4) and len(tf) == 97:
        for i in range(97):
            tl.append(start + datetime.timedelta(hours=i))

    elif end-start == datetime.timedelta(days=7) and len(tf) == 169:
        for i in range(169):
            tl.append(start + datetime.timedelta(hours=i))

    elif end-start == datetime.timedelta(hours=8) and len(tf) == 60:
        for i in range(60):
            tl.append(start + datetime.timedelta(minutes=4+i*8))

    elif end-start == datetime.timedelta(hours=8) and len(tf) == 61:
        for i in range(61):
            tl.append(start + datetime.timedelta(minutes=i*8))

    elif end-start == datetime.timedelta(hours=12) and len(tf) == 90:
        for i in range(90):
            tl.append(start + datetime.timedelta(minutes=4+i*8))

    elif end-start == datetime.timedelta(hours=12) and len(tf) == 91:
        for i in range(91):
            tl.append(start + datetime.timedelta(minutes=i*8))

    elif end - start > datetime.timedelta(days=7):
        while start <= end:
            tl.append(start)
            start += datetime.timedelta(days=1)

    else:
        raise RestoreTimelabelsError

    return tl


def rescale_hourly_to_daily(daily_frames, hourly_frames):
    """
        Scales hourly datapoints to daily datapoints.  daily_frames is a list of
        three element tuples r_tf_start, r_tf_end, t_v.  hourly_frames is a list
        of (label, value) pairs.

        The rescaling is an extension of what can be found in this
        https://github.com/trendecon/trendecon repository.
    """

    labels, values = daily_frames

    if labels is None:
        return None

    c = sqlite3.connect(':memory:')
    c.execute('CREATE TABLE ts(t, v)')

    for t, v in zip(labels, values):
        c.execute('INSERT INTO ts VALUES(?, ?)', [t, v])

    layer_ts = {}
    for layer_labels, layer_values in hourly_frames:
        layer_mean = sum(layer_values) / len(layer_values)
        monthly_mean = c.execute('SELECT AVG(v) FROM ts WHERE ? <= t AND t <= ?',
                                 [layer_labels[0], layer_labels[-1]]).fetchone()[0]
        if layer_mean == 0:
            return None

        scale = monthly_mean / layer_mean

        layer_values = [v * scale for v in layer_values]

        for t, v in zip(layer_labels, layer_values):
            layer_ts[t] = v

    m = max(layer_ts.values())
    normalized_tl = sorted(layer_ts)
    normalized_tv = [100 * layer_ts[k] / m for k in sorted(layer_ts)]

    return normalized_tl, normalized_tv
