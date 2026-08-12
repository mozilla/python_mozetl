# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import requests


__versions = {}


def get_url(product, typ):
    if product == 'Firefox':
        file_base = 'firefox'
    elif product == 'FennecAndroid':
        file_base = 'mobile'
    else:
        raise Exception('Unexpected product: {}'.format(product))

    return 'https://product-details.mozilla.org/1.0/{}_{}.json'.format(file_base, typ)


def __get_major(v):
    return int(v.split('.')[0])


def __getVersions(product):
    """Get the versions number for each channel

    Returns:
        dict: versions for each channel
    """
    r = requests.get(get_url(product, 'versions'))
    data = r.json()

    if product == 'Firefox':
        esr = data['FIREFOX_ESR_NEXT']
        if not esr:
            esr = data['FIREFOX_ESR']
        if esr.endswith('esr'):
            esr = esr[:-3]

        return {
            'release': data['LATEST_FIREFOX_VERSION'],
            'beta': data['LATEST_FIREFOX_RELEASED_DEVEL_VERSION'],
            'nightly': data['FIREFOX_NIGHTLY'],
            'esr': esr,
        }
    elif product == 'FennecAndroid':
        return {
            'release': data['version'],
            'beta': data['beta_version'],
            'nightly': data['nightly_version'],
        }


def get(product, base=False):
    """Get current version number by channel

    Returns:
        dict: containing version by channel
    """
    global __versions
    if product not in __versions:
        __versions[product] = __getVersions(product)

    if base:
        res = {}
        for k, v in __versions[product].items():
            res[k] = __get_major(v) if k != "nightly" else v
        return res

    return __versions[product]


def getStabilityReleases(product, base):
    r = requests.get(get_url(product, 'history_stability_releases'))
    data = r.json()

    return [v for v in data.keys() if v.startswith(base + '.')]


def getDevelopmentReleases(product, base):
    r = requests.get(get_url(product, 'history_development_releases'))
    data = r.json()

    return [v for v in data.keys() if v.startswith(base + '.')]
