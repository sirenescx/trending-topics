from trending_topics.data.newsletter import Newsletter
from trending_topics.parsers.base import BaseParser


class RbcParser(BaseParser):
    def _parse_entry(self, entry, source: str):
        newsletter: Newsletter = super()._parse_entry(entry, source)
        newsletter.tag = entry.rbc_news_tag
        return newsletter
