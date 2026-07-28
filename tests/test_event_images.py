import unittest

from scrapy.http import HtmlResponse, Request

from tapology_scraper.spiders.event_images import (
    clean_event_name,
    extract_credit_url,
    extract_source_image_url,
    extract_ufc_hero_url,
    is_supported_source_page,
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


if __name__ == "__main__":
    unittest.main()
