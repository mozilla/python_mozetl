# Restrict to a whitelist of search_source's to avoid double counting while
# we test our new search_count telemetry developed in:
# https://bugzilla.mozilla.org/show_bug.cgi?id=1367554
# https://bugzilla.mozilla.org/show_bug.cgi?id=1368089
SEARCH_SOURCE_WHITELIST = [
    'searchbar', 'urlbar', 'abouthome', 'newtab', 'contextmenu', 'system',
    'activitystream', 'webextension', 'alias'
]
