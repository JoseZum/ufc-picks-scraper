# ESPN CardData V1 golden fixtures

These fixtures preserve only the ESPN payload fields needed to design and test `CardDataContractV1`. They are intentionally small, deterministic and safe to keep locally.

## Provenance and sanitization

- Payload topology was observed from ESPN's public [UFC scoreboard endpoint](https://site.api.espn.com/apis/site/v2/sports/mma/ufc/scoreboard) and public `sports.core.api.espn.com` competition resources on 2026-07-31.
- All event, competition and athlete IDs are synthetic reserved fixture IDs.
- All fighter, event and venue names are fictional.
- URLs, GUIDs, images, broadcast metadata, odds, play-by-play and unrelated stats were removed.
- Timestamps and result shapes remain representative because section inference, lifecycle and result normalization depend on them.
- The title-heavy fixture deliberately preserves an observed ESPN limitation: champion belt accolades may exist while the competition has no explicit `titleFight` field.

These files contain no UFC Picks users, picks, database documents, credentials or production MongoDB data.

## Files

| Scenario | Files | Purpose |
|---|---|---|
| Two-section Fight Night | `fight_night_two_sections.json` | Completed card with two timestamp groups and mixed result methods. |
| Three-section numbered card | `numbered_three_sections.json` | Scheduled card with early prelim, prelim and main timestamp groups. |
| Title-heavy card | `title_heavy_missing_explicit_flag.json` | Two belt-accolade matchups, explicit section/order metadata and no `titleFight` signal. |
| Cancellation + reorder | `card_change_before.json`, `card_change_after.json` | One competition disappears and surviving fights change array order. |
| Fighter replacement | `replacement_before.json`, `replacement_after.json` | Same ESPN competition ID, changed canonical fighter set. |
| Result outcomes | `result_outcomes.json` | Decision, KO/TKO, submission, draw and no contest. |
| Result correction | `result_correction_before.json`, `result_correction_after.json` | Same competition changes from a decision result to a round-two submission. |

`manifest.json` is the machine-readable index and records the exact expectations used by the integrity tests.

## Scope boundary

SCR-003 adds evidence only. It does not decide how the future normalizer resolves a disappearance, title inference or result correction. Those rules remain in `CardDataContractV1`, Q-015 and later SCR tasks.
