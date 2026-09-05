"""Regression for Paris: two DEC winners accompanied by submission attempts."""
import pytest

from tapology_scraper.espn_etl import normalize_result_method, transform_result
from tapology_scraper.card_observation_sources import _espn_result_values


@pytest.mark.parametrize('marker,method,family', [
    ('Unofficial Winner Decision', 'DEC', 'decision'),
    ('Unofficial Winner Submission', 'SUB', 'submission'),
    ('Unofficial Winner Kotko', 'KO/TKO', 'ko_tko'),
    ('No Contest', 'NC', 'other'),
    ('Majority Draw', 'DEC', 'decision'),
    ('Disqualification', 'DQ', 'dq'),
    ('NC', 'NC', 'other'),
    ('Official Winner SUB', 'SUB', 'submission'),
    ('Unofficial Winner DEC', 'DEC', 'decision'),
])
@pytest.mark.parametrize('reverse', [False, True])
def test_result_ignores_attempts_knockdowns_and_generic_results(marker, method, family, reverse):
    texts = [marker, 'Results', 'Fight Over', 'Round End', 'Submission Attempt',
             'Knockdown', 'Takedown', 'Submission Attempt']
    if reverse:
        texts.reverse()
    competition = {
        'id': 'synthetic-result',
        'status': {'type': {'completed': True}, 'period': 3, 'displayClock': '5:00'},
        'details': [{'type': {'text': text}} for text in texts],
    }
    decisive = method != 'NC' and 'Draw' not in marker
    winner = {'winner': decisive, 'athlete': {'displayName': 'Test Winner'}}
    canonical = _espn_result_values(competition, [
        {'corner': 'red', 'fighter_id': 'test-winner', '_competitor': winner},
    ])
    legacy = transform_result(competition, {'red': winner})
    assert normalize_result_method(texts)[0] == method
    assert legacy['method'] == method
    assert canonical['method_family'] == family
    assert canonical['method_detail'] == marker
    if method == 'NC':
        assert canonical['outcome'] == 'no_contest'
        assert legacy['outcome'] == 'nc'


@pytest.mark.parametrize('texts', [
    ['Submission Attempt', 'Results', 'Knockdown'],
    ['Unofficial Winner Decision', 'Unofficial Winner Submission'],
    [],
])
def test_missing_or_conflicting_declaration_cannot_publish_a_result(texts):
    competition = {'status': {'type': {'completed': True}},
                   'details': [{'type': {'text': text}} for text in texts]}
    assert normalize_result_method(texts) == ('OTHER', None)
    assert transform_result(competition, {}) is None
    assert _espn_result_values(competition, []) is None
