import time
import urllib.parse

import feedparser
from bs4 import BeautifulSoup

from trending_topics.data.newsletter import Newsletter


class BaseParser:
    def parse(self, url: str):
        newsletters: list[Newsletter] = list()
        for entry in feedparser.parse(url).entries:
            newsletter: Newsletter = self._parse_entry(entry, url)
            if newsletter is not None:
                newsletters.append(newsletter)
        return newsletters

    def _parse_entry(self, entry, source: str):
        source: str = self._get_author(entry, source)
        title: str = self._preprocess_text(entry.title)
        summary: str = self._parse_summary(entry)
        content: str = self._get_content(entry)
        published: int = self._get_publishing_timestamp(entry)
        if published is None:
            return None
        return Newsletter(
            source=source,
            title=title,
            summary=summary,
            content=content,
            published=published
        )

    def _get_author(self, entry, source: str):
        if not hasattr(entry, 'author'):
            return urllib.parse.urlsplit(source).hostname
        return entry.author

    def _parse_summary(self, entry):
        if not hasattr(entry, 'summary'):
            return None
        return self._preprocess_text(entry.summary)

    def _get_publishing_timestamp(self, entry):
        if not hasattr(entry, 'published_parsed'):
            return None
        return int(time.mktime(entry.published_parsed))

    def _get_content(self, entry):
        if not hasattr(entry, 'content'):
            return None
        contents: list[str] = list()
        for content in entry.content:
            if hasattr(content, 'value'):
                contents.append(content.value)
        return self._preprocess_text(' '.join(contents))

    def _preprocess_text(self, text: str):
        text_without_html = BeautifulSoup(text, 'html.parser').get_text()
        processed_text = (
            text_without_html
            .replace('\u200b', '')
            .replace('\xa0', ' ')
            .strip()
        )
        return processed_text
