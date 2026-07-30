import unittest
from datetime import date, datetime

from scrapy.http import HtmlResponse, Request

from tapology_scraper.spiders.event_images import (
    clean_event_name,
    extract_credit_url,
    extract_source_image_url,
    extract_ufc_event_date,
    extract_ufc_hero_url,
    event_is_in_image_window,
    event_is_in_season,
    image_response_is_valid,
    instagram_embed_url,
    is_supported_source_page,
    is_x_media_url,
    poster_is_displayable,
    poster_needs_wikipedia_refresh,
    select_wikipedia_article,
    select_wikipedia_candidate,
)


def html_response(url: str, body: str) -> HtmlResponse:
    return HtmlResponse(
        url=url,
        body=body.encode("utf-8"),
        encoding="utf-8",
        request=Request(url=url),
    )


class EventImageResolverTests(unittest.TestCase):
    def test_clean_event_name_removes_duplicate_heading(self):
        self.assertEqual(
            clean_event_name(
                "UFC Fight Night UFC Fight Night: Ankalaev vs. Guskov"
            ),
            "UFC Fight Night: Ankalaev vs. Guskov",
        )

    def test_selects_matching_wikipedia_file_and_credit(self):
        payload = {
            "query": {
                "pages": {
                    "1": {
                        "title": "File:UFC Fight Night - Ankalaev vs. Guskov.jpg",
                        "imageinfo": [
                            {
                                "url": "https://upload.wikimedia.org/poster.jpg",
                                "descriptionurl": "https://en.wikipedia.org/wiki/File:poster",
                                "extmetadata": {
                                    "Categories": {
                                        "value": "Ultimate Fighting Championship event posters"
                                    },
                                    "Credit": {
                                        "value": (
                                            '<a rel="nofollow" href="'
                                            "https://x.com/BigMarcel24/status/1/photo/1"
                                            '">source</a>'
                                        )
                                    },
                                },
                            }
                        ],
                    },
                    "2": {
                        "title": "File:UFC Fight Night - Other vs. Event.jpg",
                        "imageinfo": [
                            {
                                "url": "https://upload.wikimedia.org/other.jpg",
                                "extmetadata": {},
                            }
                        ],
                    },
                }
            }
        }

        result = select_wikipedia_candidate(
            payload,
            "UFC Fight Night: Ankalaev vs. Guskov",
        )

        self.assertIsNotNone(result)
        self.assertEqual(
            result["source_page_url"],
            "https://x.com/BigMarcel24/status/1/photo/1",
        )

    def test_article_resolves_numbered_poster_filename(self):
        payload = {
            "query": {
                "pages": {
                    "83176195": {
                        "title": "UFC Fight Night: Medić vs. Rodriguez",
                        "fullurl": (
                            "https://en.wikipedia.org/wiki/"
                            "UFC_Fight_Night:_Medi%C4%87_vs._Rodriguez"
                        ),
                        "images": [
                            {"title": "File:Commons-logo.svg"},
                            {"title": "File:UFC Fight Night 283.jpg"},
                            {"title": "File:Štark Arena.jpg"},
                        ],
                    }
                }
            }
        }

        result = select_wikipedia_article(
            payload,
            "UFC Fight Night UFC Fight Night: Medic vs Rodriguez",
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["file_title"], "File:UFC Fight Night 283.jpg")

    def test_extract_credit_url_ignores_wikipedia_links(self):
        value = (
            '<a href="https://en.wikipedia.org/wiki/Test">wiki</a>'
            '<a href="https://x.com/UFC/status/123/photo/1">source</a>'
        )
        self.assertEqual(
            extract_credit_url(value),
            "https://x.com/UFC/status/123/photo/1",
        )

    def test_accepts_non_wikipedia_source_pages(self):
        self.assertTrue(is_supported_source_page("https://ufc.com/news/poster"))
        self.assertTrue(is_supported_source_page("https://x.com/UFC/status/123"))
        self.assertFalse(
            is_supported_source_page("https://en.wikipedia.org/wiki/File:poster")
        )

    def test_extracts_source_og_image(self):
        response = html_response(
            "https://x.com/UFC/status/123",
            """
            <html><head>
              <meta property="og:image"
                    content="https://pbs.twimg.com/media/poster.jpg:large">
            </head></html>
            """,
        )
        self.assertEqual(
            extract_source_image_url(response),
            "https://pbs.twimg.com/media/poster.jpg:large",
        )

    def test_x_source_rejects_generic_preview_art(self):
        self.assertTrue(
            is_x_media_url("https://pbs.twimg.com/media/poster.jpg:large")
        )
        self.assertFalse(
            is_x_media_url(
                "https://upload.wikimedia.org/wikipedia/commons/icon.png"
            )
        )
        self.assertFalse(
            is_x_media_url("https://abs.twimg.com/responsive-web/client-web/icon.png")
        )

    def test_instagram_source_uses_public_embed_page(self):
        self.assertEqual(
            instagram_embed_url("https://www.instagram.com/p/Da4rrB4mssd/"),
            "https://www.instagram.com/p/Da4rrB4mssd/embed/",
        )
        self.assertEqual(
            instagram_embed_url(
                "https://www.instagram.com/ufc/reel/ABC123/?utm_source=test"
            ),
            "https://www.instagram.com/reel/ABC123/embed/",
        )

    def test_prefers_uncropped_instagram_embed_image(self):
        response = html_response(
            "https://www.instagram.com/p/Da4rrB4mssd/embed/",
            """
            <html><head>
              <meta property="og:image"
                    content="https://cdninstagram.com/square.jpg">
            </head><body>
              <img class="EmbeddedMediaImage"
                   src="https://fbcdn.net/original-poster.jpg">
            </body></html>
            """,
        )
        self.assertEqual(
            extract_source_image_url(response),
            "https://fbcdn.net/original-poster.jpg",
        )

    def test_prefers_official_xl_2x_hero(self):
        response = html_response(
            "https://www.ufcespanol.com/event/test",
            """
            <div class="c-hero"><picture>
              <source srcset="
                https://ufc.com/images/styles/background_image_xl/s3/art.jpg 1x,
                https://ufc.com/images/styles/background_image_xl_2x/s3/art.jpg 2x">
              <img src="https://ufc.com/images/styles/background_image_sm/s3/art.jpg">
            </picture></div>
            """,
        )
        self.assertEqual(
            extract_ufc_hero_url(response),
            "https://ufc.com/images/styles/background_image_xl_2x/s3/art.jpg",
        )

    def test_extracts_new_ufc_event_date_from_slug(self):
        response = html_response(
            "https://www.ufcespanol.com/event/"
            "ufc-fight-night-august-22-2026",
            "<html></html>",
        )
        self.assertEqual(extract_ufc_event_date(response), "2026-08-22")

    def test_extracts_numbered_ufc_event_date_from_description(self):
        response = html_response(
            "https://www.ufcespanol.com/event/ufc-330",
            """
            <meta name="description"
                  content="Live in Philadelphia on August 15, 2026">
            """,
        )
        self.assertEqual(extract_ufc_event_date(response), "2026-08-15")

    def test_signed_instagram_source_is_refreshed_before_it_expires(self):
        event = {
            "poster_image_source": "wikipedia_source",
            "poster_image_url": (
                "https://scontent-sjc6-1.cdninstagram.com/poster.jpg?oe=123"
            ),
        }
        self.assertTrue(poster_is_displayable(event))
        self.assertTrue(poster_needs_wikipedia_refresh(event))

    def test_stable_wikipedia_source_does_not_need_daily_refresh(self):
        event = {
            "poster_image_source": "wikipedia_source",
            "poster_image_url": "https://pbs.twimg.com/media/poster.jpg:large",
        }
        self.assertTrue(poster_is_displayable(event))
        self.assertFalse(poster_needs_wikipedia_refresh(event))

    def test_legacy_poster_source_is_not_counted_as_coverage(self):
        event = {
            "poster_image_source": "tapology",
            "poster_image_url": "https://images.tapology.com/old.jpg",
        }
        self.assertFalse(poster_is_displayable(event))

    def test_image_validator_requires_successful_image_content(self):
        self.assertTrue(image_response_is_valid(200, "image/jpeg"))
        self.assertTrue(image_response_is_valid(206, "image/png"))
        self.assertFalse(image_response_is_valid(404, "image/jpeg"))
        self.assertFalse(image_response_is_valid(200, "text/html"))

    def test_image_refresh_window_includes_current_upcoming_cards(self):
        self.assertTrue(
            event_is_in_image_window(
                {"date": datetime(2026, 8, 22)},
                today=date(2026, 7, 28),
            )
        )
        self.assertFalse(
            event_is_in_image_window(
                {"date": datetime(2025, 8, 22)},
                today=date(2026, 7, 28),
            )
        )

    def test_image_season_can_be_checked_without_expanding_daily_window(self):
        event = {"date": datetime(2026, 1, 24)}
        self.assertTrue(event_is_in_season(event, 2026))
        self.assertFalse(event_is_in_season(event, 2025))
        self.assertFalse(
            event_is_in_image_window(
                event,
                today=date(2026, 7, 28),
            )
        )


if __name__ == "__main__":
    unittest.main()
