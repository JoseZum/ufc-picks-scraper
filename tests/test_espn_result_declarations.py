"""ESPN result declarations and core status fallback."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tapology_scraper.card_observation_sources import _espn_result_values
from tapology_scraper.espn_etl import normalize_result_method, transform_result
from tapology_scraper.spiders.espn import EspnSpider


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


@pytest.mark.parametrize('label,family,round_number,clock', [
    ('Decision - Unanimous', 'decision', 3, '5:00'),
    ('DQ', 'dq', 1, '2:19'),
])
def test_core_status_supplies_missing_scoreboard_method(label, family, round_number, clock):
    competition = {
        'status': {'type': {'completed': True}, 'period': round_number,
                   'displayClock': clock},
        'details': [{'type': {'text': 'Results'}}, {'type': {'text': 'Fight Over'}}],
    }
    fighters = [
        {'corner': 'red', 'fighter_id': 'winner', '_competitor': {'winner': True}},
        {'corner': 'blue', 'fighter_id': 'loser', '_competitor': {'winner': False}},
    ]
    assert _espn_result_values(competition, fighters) is None
    result = _espn_result_values(competition, fighters, label)
    assert result['outcome'] == 'red_win'
    assert result['winner_fighter_id'] == 'winner'
    assert result['method_family'] == family
    assert result['method_detail'] == label


def test_core_status_cannot_override_conflicting_scoreboard_declarations():
    competition = {'status': {'type': {'completed': True}}, 'details': [
        {'type': {'text': 'Unofficial Winner Decision'}},
        {'type': {'text': 'Unofficial Winner Submission'}},
    ]}
    assert _espn_result_values(competition, [], 'Decision - Unanimous') is None


def test_competition_status_method_survives_metadata_response_order():
    spider = EspnSpider.__new__(EspnSpider)
    bouts = Mock()
    bouts.find_one.return_value = {'event_id': 1, 'espn_competition_id': '2'}
    spider.db = SimpleNamespace(bouts=bouts)
    spider._espn_cards = {1: {'details': {}}}
    spider._submit_card_observations = Mock()

    spider.parse_competition_result(
        SimpleNamespace(json=lambda: {'result': {'displayName': 'DQ'}}), 2
    )
    spider.parse_competition_metadata(SimpleNamespace(json=lambda: {}), 2)

    assert spider._espn_cards[1]['details']['2']['result_method'] == 'DQ'
    assert spider._submit_card_observations.call_count == 2


def test_results_pass_fetches_status_when_scoreboard_has_no_method():
    spider = EspnSpider.__new__(EspnSpider)
    bouts = Mock()
    bouts.find.return_value = [{'id': 2, 'event_id': 1, 'espn_competition_id': '2'}]
    spider.db = SimpleNamespace(bouts=bouts)
    spider.mode = 'results'
    spider._espn_cards = {}
    spider.processed_bouts = 0
    spider._attach_espn_aliases = Mock()
    spider._submit_card_observations = Mock()
    spider._link_competitors = Mock()
    spider._competition_request = Mock(return_value='metadata request')
    competition = {
        'id': '2', 'status': {'type': {'completed': True}},
        'details': [{'type': {'text': 'Results'}}],
        'competitors': [{'id': '11'}, {'id': '12'}],
    }

    requests = list(spider._process_card(
        {'id': '123', 'competitions': [competition]}, {'id': 1}
    ))

    assert len(requests) == 2
    assert requests[1].url.endswith('/events/123/competitions/2/status?lang=en&region=us')
    assert requests[1].callback == spider.parse_competition_result
