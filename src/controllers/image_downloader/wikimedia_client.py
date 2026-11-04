"""Cliente HTTP para descargar imágenes de Wikimedia."""

import logging
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

logger = logging.getLogger("WikimediaClient")

DEFAULT_USER_AGENT = (
    "tp-final-distribuidos-grupo31/1.0 (trabajo practico FIUBA; "
    "https://github.com/gjosen9999s/tp-final-distribuidos-grupo31) python-urllib"
)
DEFAULT_TIMEOUT = 10.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 1.0
DEFAULT_MAX_BYTES = 16 * 1024 * 1024
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}
MAX_RETRY_AFTER = 30.0


class WikimediaClient:
    def __init__(
        self,
        user_agent=None,
        timeout=None,
        max_retries=None,
        backoff=None,
        max_bytes=None,
        sleep=time.sleep,
        opener=None,
    ):
        self.user_agent = user_agent or os.getenv(
            "WIKIMEDIA_USER_AGENT", DEFAULT_USER_AGENT
        )
        self.timeout = timeout if timeout is not None else float(
            os.getenv("WIKIMEDIA_TIMEOUT", DEFAULT_TIMEOUT)
        )
        self.max_retries = max_retries if max_retries is not None else int(
            os.getenv("WIKIMEDIA_MAX_RETRIES", DEFAULT_MAX_RETRIES)
        )
        self.backoff = backoff if backoff is not None else float(
            os.getenv("WIKIMEDIA_BACKOFF", DEFAULT_BACKOFF)
        )
        self.max_bytes = max_bytes if max_bytes is not None else int(
            os.getenv("WIKIMEDIA_MAX_BYTES", DEFAULT_MAX_BYTES)
        )
        self._sleep = sleep
        self._open = opener or urllib.request.urlopen

    def download(self, url):
        if not self._is_valid_url(url):
            return b""

        attempts = self.max_retries + 1
        for attempt in range(attempts):
            retry_after = None
            try:
                return self._fetch(url)
            except urllib.error.HTTPError as e:
                if e.code not in RETRYABLE_STATUS:
                    logger.warning(f"HTTP {e.code} descargando {url}; se descarta")
                    return b""
                retry_after = self._parse_retry_after(e.headers)
                logger.warning(f"HTTP {e.code} descargando {url} (intento {attempt + 1}/{attempts})")
            except _TooLarge:
                logger.warning(f"Imagen demasiado grande (> {self.max_bytes} bytes): {url}")
                return b""
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                logger.warning(f"Error de red descargando {url}: {e} (intento {attempt + 1}/{attempts})")

            if attempt < attempts - 1:
                delay = retry_after if retry_after is not None else self.backoff * (2 ** attempt)
                self._sleep(min(delay, MAX_RETRY_AFTER))

        logger.error(f"No se pudo descargar {url} tras {attempts} intentos")
        return b""

    def _fetch(self, url):
        request = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        with self._open(request, timeout=self.timeout) as response:
            length = response.headers.get("Content-Length")
            if length and length.isdigit() and int(length) > self.max_bytes:
                raise _TooLarge()
            data = response.read(self.max_bytes + 1)
            if len(data) > self.max_bytes:
                raise _TooLarge()
            return data

    @staticmethod
    def _is_valid_url(url):
        if not isinstance(url, str) or not url.strip():
            return False
        parsed = urlparse(url.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)

    @staticmethod
    def _parse_retry_after(headers):
        value = headers.get("Retry-After") if headers else None
        if value is not None and value.strip().isdigit():
            return float(value.strip())
        return None


class _TooLarge(Exception):
    pass
