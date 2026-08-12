# -*- coding: utf-8 -*-
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import re
from functools import lru_cache

from . import utils


def get_critical_errors():
    results = utils.query_searchfox('gfxCriticalError(') + utils.query_searchfox('gfxCriticalNote <<') + utils.query_searchfox('gfxCriticalErrorOnce(')

    matches = [re.search(r'"(.*?)"', line['line']) for result in results for line in result['lines']]

    errors = [match.group(1) for match in matches if match is not None]

    return set(error for error in errors if error != ', ')
