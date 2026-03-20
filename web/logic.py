#!/usr/bin/env python3
"""Compatibility facade for Media Tracks backend logic.

The route-facing service layer now lives in :mod:`web.services`, while the
remaining business logic is split by domain boundary into :mod:`web.ledger`,
:mod:`web.themes`, and :mod:`web.tasks`.
"""

from __future__ import annotations

from web.ledger import *  # noqa: F401,F403
from web.services import *  # noqa: F401,F403
from web.tasks import *  # noqa: F401,F403
from web.themes import *  # noqa: F401,F403
