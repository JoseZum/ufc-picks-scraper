"""ESPN result declarations and core status fallback."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tapology_scraper.canonical_card_writer import (
    InMemoryCanonicalCardStore,
    submit_card_observations,
)
from tapology_scraper.card_observation_sources import (
    _espn_result_values,
    build_espn_card_observations,
)
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


@pytest.mark.parametrize('label', [
    'Draw', 'Majority Draw', 'Split Draw', 'Unanimous Draw', 'Technical Draw',
])
def test_core_status_supplies_missing_draw_result(label):
    competition = {
        'status': {'type': {'completed': True}, 'period': 3, 'displayClock': '5:00'},
        'details': [{'type': {'text': 'Results'}}, {'type': {'text': 'Fight Over'}}],
    }
    fighters = [
        {'corner': 'red', 'fighter_id': 'red', '_competitor': {'winner': False}},
        {'corner': 'blue', 'fighter_id': 'blue', '_competitor': {'winner': False}},
    ]

    result = _espn_result_values(competition, fighters, label)

    assert result == {
        'outcome': 'draw',
        'winner_fighter_id': None,
        'method_family': 'decision',
        'method_detail': label,
        'ending_round': 3,
        'ending_time_seconds': 300,
    }


@pytest.mark.parametrize('label', ['Decision - Unanimous', 'KO/TKO', 'Submission'])
@pytest.mark.parametrize('declared', [False, True])
def test_decisive_method_without_a_winner_flag_is_not_a_draw(label, declared):
    # ESPN a veces declara el metodo antes de marcar al ganador: eso es un
    # resultado a medio publicar, no un empate (puntuaria todos los picks a 0).
    details = [{'type': {'text': 'Results'}}]
    if declared:
        details.append({'type': {'text': 'Unofficial Winner Decision'}})
    competition = {
        'status': {'type': {'completed': True}, 'period': 3, 'displayClock': '5:00'},
        'details': details,
    }
    fighters = [
        {'corner': 'red', 'fighter_id': 'red', '_competitor': {'winner': False}},
        {'corner': 'blue', 'fighter_id': 'blue', '_competitor': {'winner': False}},
    ]

    assert _espn_result_values(competition, fighters, label) is None


def test_scoreboard_draw_declaration_still_publishes_a_draw():
    competition = {
        'status': {'type': {'completed': True}, 'period': 3, 'displayClock': '5:00'},
        'details': [{'type': {'text': 'Majority Draw'}}],
    }
    fighters = [
        {'corner': 'red', 'fighter_id': 'red', '_competitor': {'winner': False}},
        {'corner': 'blue', 'fighter_id': 'blue', '_competitor': {'winner': False}},
    ]

    assert _espn_result_values(competition, fighters)['outcome'] == 'draw'


def test_core_status_draw_allows_completed_card_to_converge():
    fixture = Path(__file__).parent / 'fixtures/espn/card_data_v1/result_outcomes.json'
    event = json.loads(fixture.read_text(encoding='utf-8'))['events'][0]
    draw = next(c for c in event['competitions'] if any(
        'Draw' in d['type']['text'] for d in c.get('details', [])
    ))
    draw['details'] = [{'type': {'text': 'Results'}}]
    event_id = int(event['id'])
    store = InMemoryCanonicalCardStore(events=[{'id': event_id, 'espn_event_id': event['id']}])
    batch = build_espn_card_observations(
        event,
        store.load_card(event_id),
        observed_at='2026-10-04T12:00:00Z',
        competition_metadata={draw['id']: {'result_method': 'Draw'}},
    )

    plan, receipt = submit_card_observations(
        store, event_id, batch.observations, dry_run=False,
    )

    assert not plan.blocked, [item.message for item in plan.findings]
    assert plan.snapshot['event']['status'] == 'completed'
    result = store.bouts[int(draw['id'])]['result']
    assert result['outcome'] == 'draw'
    assert result['winner'] is None
    assert receipt.verified_converged
    replay, _ = submit_card_observations(store, event_id, batch.observations)
    assert replay.converged
    assert replay.operation_count == 0


def test_core_status_unknown_method_cannot_publish_a_result():
    competition = {'status': {'type': {'completed': True}}}
    assert _espn_result_values(competition, [], 'Unknown') is None


@pytest.mark.parametrize('label', ['Decision - Unanimous', 'Draw'])
def test_core_status_cannot_override_conflicting_scoreboard_declarations(label):
    competition = {'status': {'type': {'completed': True}}, 'details': [
        {'type': {'text': 'Unofficial Winner Decision'}},
        {'type': {'text': 'Unofficial Winner Submission'}},
    ]}
    assert _espn_result_values(competition, [], label) is None


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
