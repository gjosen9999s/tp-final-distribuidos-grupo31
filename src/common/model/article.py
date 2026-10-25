"""Modelo de artículo normalizado."""

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Article:
    docId: int
    name: str | None = None
    url: str | None = None
    dateModified: int | None = None
    inLanguage: int | None = None
    sectionsFlags: int | None = None
    abstract: str | None = None
    imageUrl: str | None = None

    @property
    def year(self):
        if self.dateModified is None:
            return None
        return datetime.fromtimestamp(self.dateModified, tz=timezone.utc).year
