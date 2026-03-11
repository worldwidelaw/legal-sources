"""
Browser-based scraper for World Wide Law.

Uses Playwright for sites that require JavaScript execution:
- Cloudflare-protected pages
- JavaScript SPAs (React, Vue, Angular, Blazor)
- Sites with anti-bot measures

Provides stealth mode with fingerprint evasion.
"""

import time
import logging
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Generator
from contextlib import contextmanager

try:
    from playwright.sync_api import sync_playwright, Browser, Page, BrowserContext
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger("legal-data-hunter")


class BrowserScraper:
    """
    Playwright-based scraper with stealth settings for bypassing
    Cloudflare, anti-bot systems, and JavaScript-rendered content.

    Usage:
        with BrowserScraper() as scraper:
            page = scraper.new_page()
            page.goto("https://example.com")
            content = page.content()
            # ... parse content ...
    """

    # Stealth JavaScript to evade bot detection
    STEALTH_SCRIPT = """
    // Overwrite navigator.webdriver
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });

    // Overwrite plugins to look like a real browser
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
            { name: 'Native Client', filename: 'internal-nacl-plugin' }
        ],
        configurable: true
    });

    // Overwrite languages
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en', 'de'],
        configurable: true
    });

    // Fix Chrome runtime
    window.chrome = {
        runtime: {},
    };

    // Overwrite permissions query
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    """

    def __init__(
        self,
        headless: bool = True,
        slow_mo: int = 0,
        timeout: int = 30000,
        user_agent: str = None,
        viewport: Dict[str, int] = None,
        proxy: Dict[str, str] = None,
        stealth: bool = True,
        browser_type: str = "chromium",
    ):
        """
        Initialize the browser scraper.

        Args:
            headless: Run browser in headless mode (default True)
            slow_mo: Slow down operations by this many ms (useful for debugging)
            timeout: Default timeout for operations in ms
            user_agent: Custom user agent string
            viewport: Browser viewport dimensions, e.g., {"width": 1920, "height": 1080}
            proxy: Proxy configuration, e.g., {"server": "http://proxy:8080"}
            stealth: Enable stealth mode to evade bot detection
            browser_type: "chromium", "firefox", or "webkit"
        """
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "Playwright is required for BrowserScraper. "
                "Install with: pip install playwright && playwright install chromium"
            )

        self.headless = headless
        self.slow_mo = slow_mo
        self.timeout = timeout
        self.stealth = stealth
        self.browser_type = browser_type
        self.proxy = proxy

        self.user_agent = user_agent or (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        self.viewport = viewport or {"width": 1920, "height": 1080}

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._pages: List[Page] = []

    def __enter__(self):
        """Context manager entry - launches browser."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - closes browser."""
        self.stop()

    def start(self):
        """Launch the browser."""
        self._playwright = sync_playwright().start()

        # Select browser engine
        if self.browser_type == "firefox":
            browser_engine = self._playwright.firefox
        elif self.browser_type == "webkit":
            browser_engine = self._playwright.webkit
        else:
            browser_engine = self._playwright.chromium

        # Browser launch options
        launch_options = {
            "headless": self.headless,
            "slow_mo": self.slow_mo,
        }

        if self.proxy:
            launch_options["proxy"] = self.proxy

        # Add extra args for stealth
        if self.stealth and self.browser_type == "chromium":
            launch_options["args"] = [
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]

        self._browser = browser_engine.launch(**launch_options)

        # Create context with stealth settings
        context_options = {
            "viewport": self.viewport,
            "user_agent": self.user_agent,
            "locale": "en-US",
            "timezone_id": "Europe/Berlin",
        }

        if self.proxy:
            context_options["proxy"] = self.proxy

        self._context = self._browser.new_context(**context_options)
        self._context.set_default_timeout(self.timeout)

        # Inject stealth script on every page
        if self.stealth:
            self._context.add_init_script(self.STEALTH_SCRIPT)

        logger.info(f"Browser started: {self.browser_type}, headless={self.headless}")

    def stop(self):
        """Close browser and cleanup."""
        for page in self._pages:
            try:
                page.close()
            except Exception:
                pass

        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

        self._pages = []
        self._context = None
        self._browser = None
        self._playwright = None

        logger.info("Browser stopped")

    def new_page(self) -> Page:
        """Create a new browser tab/page."""
        if not self._context:
            raise RuntimeError("Browser not started. Call start() first or use context manager.")

        page = self._context.new_page()
        self._pages.append(page)
        return page

    def goto(self, page: Page, url: str, wait_until: str = "domcontentloaded") -> None:
        """
        Navigate to a URL and wait for the page to load.

        Args:
            page: Playwright Page object
            url: URL to navigate to
            wait_until: When to consider navigation finished.
                       Options: "domcontentloaded", "load", "networkidle"
        """
        page.goto(url, wait_until=wait_until)

    def wait_for_cloudflare(self, page: Page, max_wait: int = 15000) -> bool:
        """
        Wait for Cloudflare challenge to complete.

        Returns True if challenge was passed, False if timed out.
        """
        start = time.time()
        max_seconds = max_wait / 1000

        while (time.time() - start) < max_seconds:
            # Check for common Cloudflare challenge indicators
            content = page.content()

            # Challenge page indicators
            cloudflare_indicators = [
                "Checking your browser",
                "Just a moment...",
                "cf-browser-verification",
                "challenge-form",
            ]

            is_challenge = any(ind in content for ind in cloudflare_indicators)

            if not is_challenge:
                logger.debug("Cloudflare challenge passed")
                return True

            # Wait and check again
            time.sleep(0.5)

        logger.warning("Cloudflare challenge timeout")
        return False

    def scroll_to_bottom(self, page: Page, pause: float = 0.5) -> None:
        """Scroll to bottom of page to trigger lazy loading."""
        page.evaluate("""
            async () => {
                await new Promise((resolve) => {
                    let totalHeight = 0;
                    const distance = 500;
                    const timer = setInterval(() => {
                        const scrollHeight = document.body.scrollHeight;
                        window.scrollBy(0, distance);
                        totalHeight += distance;
                        if(totalHeight >= scrollHeight){
                            clearInterval(timer);
                            resolve();
                        }
                    }, 100);
                });
            }
        """)
        time.sleep(pause)

    def wait_for_selector(self, page: Page, selector: str, timeout: int = None) -> bool:
        """
        Wait for a CSS selector to appear.

        Returns True if found, False if timeout.
        """
        try:
            page.wait_for_selector(selector, timeout=timeout or self.timeout)
            return True
        except Exception:
            return False

    def get_text(self, page: Page, selector: str) -> Optional[str]:
        """Get text content of an element."""
        try:
            return page.text_content(selector)
        except Exception:
            return None

    def get_all_texts(self, page: Page, selector: str) -> List[str]:
        """Get text content of all matching elements."""
        try:
            elements = page.query_selector_all(selector)
            return [el.text_content() or "" for el in elements]
        except Exception:
            return []

    def get_attribute(self, page: Page, selector: str, attr: str) -> Optional[str]:
        """Get an attribute value from an element."""
        try:
            return page.get_attribute(selector, attr)
        except Exception:
            return None

    def click_and_wait(self, page: Page, selector: str, wait_ms: int = 1000) -> None:
        """Click an element and wait for navigation/content load."""
        page.click(selector)
        time.sleep(wait_ms / 1000)

    def fill_form(self, page: Page, selector: str, value: str) -> None:
        """Fill a form input."""
        page.fill(selector, value)

    def select_option(self, page: Page, selector: str, value: str) -> None:
        """Select an option from a dropdown."""
        page.select_option(selector, value)

    def get_page_html(self, page: Page) -> str:
        """Get the full HTML content of the page."""
        return page.content()

    def evaluate_js(self, page: Page, script: str) -> Any:
        """Execute JavaScript and return the result."""
        return page.evaluate(script)

    def screenshot(self, page: Page, path: str) -> None:
        """Take a screenshot for debugging."""
        page.screenshot(path=path)

    def pdf(self, page: Page, path: str) -> None:
        """Save page as PDF (Chromium only, non-headless may have issues)."""
        page.pdf(path=path)

    def download_file(self, page: Page, click_selector: str, download_path: str) -> Optional[str]:
        """
        Click a download link and save the file.

        Returns the path to the downloaded file, or None on failure.
        """
        try:
            with page.expect_download() as download_info:
                page.click(click_selector)
            download = download_info.value
            save_path = Path(download_path) / download.suggested_filename
            download.save_as(save_path)
            return str(save_path)
        except Exception as e:
            logger.warning(f"Download failed: {e}")
            return None


class BrowserScraperPool:
    """
    Pool of browser scrapers for concurrent operations.

    Usage:
        pool = BrowserScraperPool(size=3)
        pool.start()

        def process_url(scraper, url):
            page = scraper.new_page()
            page.goto(url)
            return page.content()

        results = pool.map(process_url, urls)
        pool.stop()
    """

    def __init__(self, size: int = 3, **scraper_kwargs):
        self.size = size
        self.scraper_kwargs = scraper_kwargs
        self.scrapers: List[BrowserScraper] = []
        self._available: List[BrowserScraper] = []

    def start(self):
        """Start all browser instances."""
        for _ in range(self.size):
            scraper = BrowserScraper(**self.scraper_kwargs)
            scraper.start()
            self.scrapers.append(scraper)
            self._available.append(scraper)

    def stop(self):
        """Stop all browser instances."""
        for scraper in self.scrapers:
            scraper.stop()
        self.scrapers = []
        self._available = []

    @contextmanager
    def get_scraper(self):
        """Get an available scraper from the pool."""
        while not self._available:
            time.sleep(0.1)

        scraper = self._available.pop()
        try:
            yield scraper
        finally:
            self._available.append(scraper)

    def map(self, func: Callable, items: List[Any]) -> List[Any]:
        """
        Apply a function to items using available scrapers.

        func should accept (scraper, item) and return a result.
        """
        import concurrent.futures

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.size) as executor:
            futures = []
            for item in items:
                with self.get_scraper() as scraper:
                    future = executor.submit(func, scraper, item)
                    futures.append(future)

            for future in concurrent.futures.as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logger.error(f"Pool task error: {e}")
                    results.append(None)

        return results
