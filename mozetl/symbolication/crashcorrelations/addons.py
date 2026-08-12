# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import requests
import xml.etree.ElementTree as ET


guids_to_names = {
    '{972ce4c6-7e08-4474-a285-3208198ce6fd}': 'Default Firefox Theme',
    '{4ED1F68A-5463-4931-9384-8FFF5ED91D92}': 'McAfee SiteAdvisor',
    'wrc@avast.com': 'Avast Online Security',
    'sp@avast.com': 'Avast SafePrice',
    'e10srollout@mozilla.org': 'Multi-process staged rollout',
    'firefox@getpocket.com': 'Pocket',
    'firefox@mega.co.nz': 'MEGA',
    'cpmanager@mozillaonline.com': 'China Edition Addons Manager',
    'tabtweak@mozillaonline.com': 'China Edition Tab Tweak',
    'cehomepage@mozillaonline.com': 'China Edition Firefox Home Page',
    'commonfix@mozillaonline.com': '火狐修复工具',
    'wx-assistant@mozillaonline.com': '微信网页版助手',
    'coba@mozilla.com.cn': 'China Online Banking Assistant',
    'light_plugin_D772DC8D6FAF43A29B25C4EBAA5AD1DE@kaspersky.com': 'Kaspersky Internet Security',
    'light_plugin_ACF0E80077C511E59DED005056C00008@kaspersky.com': 'Kaspersky Protection',
    'jid1-YcMV6ngYmQRA2w@jetpack': 'Pinterest - Pin it button',
    '{C1A2A613-35F1-4FCF-B27F-2840527B6556}': 'Norton Identity Safe',
    'mozilla_cc2@internetdownloadmanager.com': 'Internet Download Manager integration',
    'adbhelper@mozilla.org': 'ADB Helper',
    'helper-sig@savefrom.net': 'SaveFrom.net - helper',
    'fxdevtools-adapters@mozilla.org': 'Valence',
    'abs@avira.com': 'Avira Browser Safety',
    '{a38384b3-2d1d-4f36-bc22-0f7ae402bcd7}': 'Adware.WebAlta',
    'ubufox@ubuntu.com': 'Ubuntu Modifications',
    'avg@toolbar': 'AVG Security Toolbar',
    'jid1-4P0kohSJxU1qGg@jetpack': 'Hola Unblocker',
    'jetpack-extension@dashlane.com': 'Dashlane',
    'searchme@mybrowserbar.com': 'SearchMe Toolbar',
    '{22119944-ED35-4ab1-910B-E619EA06A115}': 'RoboForm Toolbar for Firefox',
    '{22181a4d-af90-4ca3-a569-faed9118d6bc}': 'Trend Micro Toolbar',
    '{cd617375-6743-4ee8-bac4-fbf10f35729e}': 'RightToClick',
}


def get_addon_name(guid):
    try:
        if guid in guids_to_names:
            return guids_to_names[guid]

        r = requests.get('https://services.addons.mozilla.org/en-US/firefox/api/1.5/search/guid:' + guid)

        el = ET.fromstring(r.content).find('./addon/name')
        if el is None or el.text is None:
            return None

        return el.text.encode('utf-8')
    except:
        return guid
