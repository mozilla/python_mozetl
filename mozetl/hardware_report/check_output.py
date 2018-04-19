import ujson
import re

from mozetl.utils import send_ses


def check_output():
    data = _get_data()
    changes = _check_most_recent_change(data, min_change=.3)

    if len(changes) > 0:
        _report_changes(changes)


def _get_data():
    """
    Make the date values the keys for each json blob.

    For example:
    Transform [ {date: 20170101, data1: some1, ...}, {date:20170702, data1:some2}, ... ]
    into { 20170101: {data1: some1, ...}, 20170102: {data1:some2, ...} }
    """
    with open("hwsurvey-weekly.json", "r") as hwsurvey:
        data = ujson.loads(''.join(hwsurvey.readlines()))

    return {int(re.sub('-', '', e['date'])): {k: e[k] for k in e if k != 'date'} for e in data}


def _check_most_recent_change(values, min_change=.05, min_value=0.01, missing_val=.01):
    assert missing_val > 0

    recent_week = max(values.keys())
    second_recent_week = max(values.viewkeys() - {recent_week})

    base, compare = values[second_recent_week], values[recent_week]
    changes = [
        (k, (compare.get(k, missing_val) / base.get(k, missing_val)) - 1)
        for k in base.viewkeys() | compare.viewkeys()
    ]

    return {
        k: {'change': c,
            'old_value': base.get(k, missing_val),
            'new_value': compare.get(k, missing_val)}
        for k, c in changes
        if abs(c) > min_change
        and base.get(k, missing_val) >= min_value
    }


def _make_report(changes):
    def mk_line(k, v1, v2):
        return "{}: Last week = {:.2f}%, This week = {:.2f}%".format(k, v1, v2)

    change_strings = [(v['change'], mk_line(k, v['old_value']*100, v['new_value']*100))
                      for k, v in changes.iteritems()]
    return '\n'.join([x[1] for x in sorted(change_strings, key=lambda x: x[0])])


def _report_changes(changes):
    send_ses(
        'telemetry-alerts@mozilla.com',
        'Hardware Report Validation Checks',
        _make_report(changes),
        'firefox-hardware-report-feedback@mozilla.com'
    )
