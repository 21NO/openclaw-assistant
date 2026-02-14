#!/usr/bin/env python3
"""Simple unit test / smoke test for RegimeClassifier.

Run: python3 tests/test_regime_classifier.py

This file is intentionally self-contained and inserts the project root into
sys.path so it can be executed from CI or directly on the machine.
"""
from __future__ import annotations
import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.regime_classifier import RegimeClassifier


def make_point(adx, ema9=None, ema50=None):
    d = {'adx14': adx}
    if ema9 is not None:
        d['ema9'] = ema9
    if ema50 is not None:
        d['ema50'] = ema50
    return d


def main():
    clf = RegimeClassifier(adx_trend_threshold=25.0, adx_range_threshold=20.0, hysteresis_period=2)

    # construct a small sequence to verify hysteresis behaviour
    data = []
    # initial: clear trend_up for 3 points
    data += [make_point(30, 101, 100) for _ in range(3)]
    # a transient 'transition' sample
    data += [make_point(22, 101, 100)]
    # then a candidate 'range' for two consecutive points -> should flip to 'range'
    data += [make_point(15, 100, 100) for _ in range(2)]
    # then two consecutive trend_down samples -> should flip to 'trend_down'
    data += [make_point(30, 90, 100) for _ in range(2)]

    labels = clf.classify_series(data)
    print('DATA LEN:', len(data))
    for i, (pt, lab) in enumerate(zip(data, labels)):
        print(i, pt, '->', lab)

    expected = [
        'trend_up', 'trend_up', 'trend_up',  # first 3 trend_up
        'trend_up',                          # transient transition doesn't flip yet
        'trend_up',                          # first range candidate (needs two in a row)
        'range',                             # second range -> switch
        'range',                             # first trend_down candidate
        'trend_down'                         # second trend_down -> switch
    ]

    assert len(labels) == len(expected), 'length mismatch'
    assert labels == expected, f'labels did not match expected\n{labels}\n{expected}'
    print('OK - regime classifier behaviour as expected')


if __name__ == '__main__':
    main()
