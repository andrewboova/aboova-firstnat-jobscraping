import time
import json
import random
import re
import os
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, parse_qs

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException,
)

import logging


# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SCRAPER
# -----------------------------------------------------------------------------
class MultiCompanyScraper:
    """
    LinkedIn Multi-Company Jobs Scraper (JSON ONLY, custom links per company)

    Key Features:
    - ALWAYS waits for user to press ENTER before starting scrape
      (even if already logged in / cookies restored)
    - Login once (cookies persisted to disk: linkedin_cookies.json)
    - Auto-recovery if ChromeDriver wedges OR session dies:
        * restarts driver
        * reloads cookies
        * resumes the last URL without manual login
        * only prompts if LinkedIn forces checkpoint/login
    - Robust pagination end detection via page signature (ignores promoted)
    - Saves job URLs as canonical permalinks:
        https://www.linkedin.com/jobs/view/<job_id>/
      (removes all filters automatically)
    - No deduping
    """

    def __init__(self):
        self.jobs = []
        self.company_counts = {}
        self.PAGE_SIZE = 25

        self.cookies_path = "linkedin_cookies.json"
        self.driver = None
        self.wait = None

        os.makedirs("json_output", exist_ok=True)

        self._create_driver()

    # -------------------------------------------------------------------------
    # DRIVER / COOKIE MANAGEMENT
    # -------------------------------------------------------------------------
    def _chrome_options(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-first-run")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        options.page_load_strategy = "eager"

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        return options

    def _create_driver(self):
        """Create a fresh driver instance."""
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
        finally:
            self.driver = webdriver.Chrome(options=self._chrome_options())
            self.driver.set_page_load_timeout(60)
            self.wait = WebDriverWait(self.driver, 30)

    def _save_cookies(self):
        try:
            cookies = self.driver.get_cookies()
            with open(self.cookies_path, "w", encoding="utf-8") as f:
                json.dump(cookies, f, indent=2)
            logger.info(f"✓ Saved cookies to {self.cookies_path}")
        except Exception as e:
            logger.warning(f"Could not save cookies: {e}")

    def _load_cookies(self):
        if not os.path.exists(self.cookies_path):
            return False
        try:
            with open(self.cookies_path, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            self.driver.get("https://www.linkedin.com/")
            time.sleep(2)

            for c in cookies:
                c.pop("sameSite", None)
                try:
                    self.driver.add_cookie(c)
                except:
                    pass

            logger.info("✓ Loaded cookies")
            return True
        except Exception as e:
            logger.warning(f"Could not load cookies: {e}")
            return False

    def _looks_logged_out(self):
        """
        Detect if we're on sign-in or checkpoint page.
        """
        try:
            url = (self.driver.current_url or "").lower()
            if "login" in url or "checkpoint" in url:
                return True

            body = (self.driver.find_element(By.TAG_NAME, "body").text or "").lower()
            if "sign in" in body and "join linkedin" in body:
                return True
            return False
        except:
            return False

    def _ensure_logged_in_once(self, first_url):
        """
        Load cookies if possible, open first URL, then ALWAYS pause for user confirmation.

        If logged out/checkpoint, user logs in then presses ENTER.
        Cookies saved after successful login.

        Even if already logged in, user must press ENTER to begin scraping.
        """
        loaded = self._load_cookies()

        self.safe_get(first_url, retries=2, wait_after=6.0)

        if self._looks_logged_out():
            logger.info("Not logged in or cookies invalid. Manual sign-in required.")
            input("Log into LinkedIn in the opened browser window, then press ENTER to begin scraping... ")
            self._save_cookies()
        else:
            if loaded:
                logger.info("✓ Session restored from cookies")
            else:
                logger.info("✓ Already logged in")

            # ALWAYS require user confirmation before starting
            input("LinkedIn appears logged in. Press ENTER to begin scraping... ")

    def human_delay(self, a, b):
        time.sleep(random.uniform(a, b))

    # -------------------------------------------------------------------------
    # HARD FAILURE DETECTION + RECOVERY
    # -------------------------------------------------------------------------
    def _is_driver_dead(self, e: Exception) -> bool:
        msg = str(e).lower()
        return (
            "invalid session id" in msg
            or "session deleted" in msg
            or "not connected to devtools" in msg
            or "disconnected" in msg
            or "chrome not reachable" in msg
            or "httpconnectionpool" in msg
            or "read timed out" in msg
        )

    def recover_driver(self, url_to_resume: str):
        """
        Hard reset driver, restore cookies, resume on the given URL.
        Only prompts if LinkedIn checkpoints.
        """
        logger.warning("Recovering driver (restart + reload cookies + resume page)...")
        self._create_driver()
        self._load_cookies()

        ok = self.safe_get(url_to_resume, retries=3, wait_after=5.5)
        if not ok:
            logger.error("Failed to reload after driver recovery.")
            return False

        if self._looks_logged_out():
            logger.error("LinkedIn checkpoint/login detected after recovery.")
            input("Resolve LinkedIn checkpoint/login in the browser, then press ENTER to continue... ")
            self._save_cookies()
            ok = self.safe_get(url_to_resume, retries=2, wait_after=5.5)

        return ok

    def safe_get(self, url, retries=3, wait_after=5.5):
        """
        Robust driver.get wrapper.
        If driver is wedged or dead, restart + reload cookies, retry.
        """
        for attempt in range(1, retries + 1):
            try:
                self.driver.get(url)
                time.sleep(wait_after)
                return True
            except Exception as e:
                logger.warning(f"GET failed (attempt {attempt}/{retries}): {e}")
                if self._is_driver_dead(e):
                    logger.warning("Driver appears dead/wedged. Restarting and restoring cookies...")
                    self._create_driver()
                    self._load_cookies()
                    continue
                return False
        return False

    # -------------------------------------------------------------------------
    # URL HELPERS
    # -------------------------------------------------------------------------
    def _set_query_param(self, url, key, value):
        parts = urlsplit(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q[key] = str(value)
        new_query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    def _normalize_search_url(self, url):
        """
        Strip params that anchor results and break pagination.
        Keep true filters. Force start=0.
        """
        parts = urlsplit(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))

        drop_keys = {
            "currentJobId",
            "originToLandingJobPostings",
            "origin",
            "trackingId",
            "refId",
            "lipi",
        }
        for k in list(q.keys()):
            if k in drop_keys:
                q.pop(k, None)

        q["start"] = "0"
        new_query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    def _job_permalink(self, job_id):
        if job_id and job_id != "N/A":
            return f"https://www.linkedin.com/jobs/view/{job_id}/"
        return "N/A"

    # -------------------------------------------------------------------------
    # JOB ID PARSING
    # -------------------------------------------------------------------------
    def _extract_job_id_from_url(self, url):
        if not url:
            return "N/A"

        try:
            parts = urlsplit(url)
            qs = parse_qs(parts.query)
            if "currentJobId" in qs and qs["currentJobId"]:
                return qs["currentJobId"][0].strip()
        except:
            pass

        if "/jobs/view/" in url:
            try:
                return url.split("/jobs/view/")[1].split("/")[0].split("?")[0]
            except:
                pass

        return "N/A"

    def _wait_for_url_job_change(self, previous_job_id, timeout=7.0):
        end = time.time() + timeout
        while time.time() < end:
            try:
                cur_url = self.driver.current_url
            except Exception as e:
                if self._is_driver_dead(e):
                    raise
                cur_url = self.driver.current_url

            cur_job_id = self._extract_job_id_from_url(cur_url)
            if cur_job_id != "N/A" and cur_job_id != previous_job_id:
                return cur_url, cur_job_id

            time.sleep(0.15)

        cur_url = self.driver.current_url
        return cur_url, self._extract_job_id_from_url(cur_url)

    # -------------------------------------------------------------------------
    # JOB CARD FINDING
    # -------------------------------------------------------------------------
    def _find_job_cards(self):
        selectors = [
            "li[data-occludable-job-id]",
            "li[data-job-id]",
            "li.jobs-search-results__list-item",
            "li[class*='job-card']",
        ]
        for sel in selectors:
            cards = self.driver.find_elements(By.CSS_SELECTOR, sel)
            if cards:
                return cards
        return []

    def _find_list_container(self):
        list_container_selectors = [
            "div.scaffold-layout__list",
            "div.jobs-search-results-list",
            "div.jobs-search-results-list__container",
            "div.scaffold-layout__list-container",
        ]
        for sel in list_container_selectors:
            try:
                return self.driver.find_element(By.CSS_SELECTOR, sel)
            except:
                continue
        return None

    def _wait_for_results_or_end(self, timeout=25):
        end = time.time() + timeout
        while time.time() < end:
            if self._find_job_cards():
                return True

            no_res_selectors = [
                "div.jobs-search-no-results",
                "div.jobs-search-two-pane__no-results",
                "div.artdeco-empty-state",
            ]
            for sel in no_res_selectors:
                try:
                    el = self.driver.find_element(By.CSS_SELECTOR, sel)
                    txt = (el.text or "").lower()
                    if "no" in txt and "result" in txt:
                        return False
                except:
                    pass

            time.sleep(0.25)

        return False

    def _ensure_cards_loaded(self, target=25, max_scrolls=12):
        container = self._find_list_container()
        prev_count = -1
        stable = 0

        for _ in range(max_scrolls):
            cur_count = len(self._find_job_cards())

            if cur_count >= target:
                return True

            if cur_count == prev_count:
                stable += 1
            else:
                stable = 0

            if stable >= 2:
                return cur_count > 0

            prev_count = cur_count

            try:
                if container:
                    self.driver.execute_script(
                        "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;",
                        container
                    )
                else:
                    self.driver.execute_script("window.scrollBy(0, 800);")
            except:
                pass

            self.human_delay(0.35, 0.8)

        return len(self._find_job_cards()) > 0

    def _get_listing_anchor_id(self, card):
        for attr in ("data-occludable-job-id", "data-job-id"):
            try:
                v = card.get_attribute(attr)
                if v and v.strip().isdigit():
                    return v.strip()
            except:
                pass
        return None

    def _is_promoted_card(self, card):
        try:
            t = (card.text or "").lower()
            return ("promoted" in t) or ("sponsored" in t)
        except:
            return False

    def _page_signature(self, n=10):
        cards = self._find_job_cards()
        ids = []

        for c in cards:
            if self._is_promoted_card(c):
                continue
            jid = self._get_listing_anchor_id(c)
            if jid:
                ids.append(jid)
            if len(ids) >= n:
                break

        if not ids:
            for c in cards:
                jid = self._get_listing_anchor_id(c)
                if jid:
                    ids.append(jid)
                if len(ids) >= n:
                    break

        return tuple(ids)

    # -------------------------------------------------------------------------
    # DETAILS EXTRACTION
    # -------------------------------------------------------------------------
    def _wait_for_job_details_loaded(self):
        selectors = [
            "div.job-details-jobs-unified-top-card__job-title h1",
            "div.jobs-unified-top-card__job-title h1",
            "h2[data-test-job-title]",
        ]
        for sel in selectors:
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
                return True
            except:
                continue
        return False

    def _extract_title(self):
        selectors = [
            "div.job-details-jobs-unified-top-card__job-title h1",
            "div.jobs-unified-top-card__job-title h1",
            "h2[data-test-job-title]",
        ]
        for sel in selectors:
            try:
                t = (self.driver.find_element(By.CSS_SELECTOR, sel).text or "").strip()
                if t and 3 < len(t) < 200 and t.lower() != "jobs":
                    return t
            except:
                continue
        return "N/A"

    def _extract_location_posted(self):
        location = "N/A"
        posted = "N/A"

        top_card_selectors = [
            "div.job-details-jobs-unified-top-card__primary-description",
            "div.jobs-unified-top-card__primary-description",
            "div.job-details-jobs-unified-top-card__primary-description-container",
        ]

        header_text = None
        for sel in top_card_selectors:
            try:
                el = self.driver.find_element(By.CSS_SELECTOR, sel)
                txt = (el.text or "").strip()
                if txt and len(txt) < 350:
                    header_text = txt
                    break
            except:
                continue

        if header_text:
            parts = [p.strip() for p in header_text.split("·") if p.strip()]
            if parts:
                location = parts[0]
            for p in parts[1:]:
                if "ago" in p.lower() or "reposted" in p.lower():
                    posted = p
                    break

        if posted == "N/A":
            try:
                time_el = self.driver.find_element(By.CSS_SELECTOR, "time")
                t = (time_el.text or "").strip()
                if t and "ago" in t.lower() and len(t) < 80:
                    posted = t
            except:
                pass

        if location != "N/A":
            low = location.lower()
            if len(location) > 120 or "search by title" in low or "try premium" in low or "notifications" in low:
                location = "N/A"

        if posted != "N/A":
            low = posted.lower()
            if len(posted) > 120 or "search by title" in low or "try premium" in low:
                posted = "N/A"

        return location, posted

    def _extract_description(self):
        selectors = [
            "div.jobs-box__html-content",
            "div.jobs-description-content__text",
            "article.jobs-description__container",
            "div[class*='jobs-description']",
        ]
        for sel in selectors:
            try:
                elem = self.driver.find_element(By.CSS_SELECTOR, sel)
                txt = (elem.text or "").strip()
                if txt and len(txt) > 50:
                    return txt
            except:
                continue
        return "N/A"

    def _extract_salary_from_description(self, description):
        if not description or description == "N/A":
            return "N/A"

        t = description.replace(",", "")
        patterns = [
            r"\$\s?\d{2,3}\s?(?:k|K)\s?(?:-|–|to)\s?\$\s?\d{2,3}\s?(?:k|K)",
            r"\$\s?\d{4,6}\s?(?:-|–|to)\s?\$\s?\d{4,6}",
            r"\$\s?\d{4,6}",
        ]

        hits = []
        for p in patterns:
            for m in re.finditer(p, t):
                s = m.group(0).strip()
                if "$0" in s:
                    continue
                hits.append(s)

        if not hits:
            return "N/A"

        hits.sort(key=lambda x: (("-" in x or "to" in x.lower() or "–" in x), len(x)), reverse=True)
        return hits[0]

    # -------------------------------------------------------------------------
    # CLICKING
    # -------------------------------------------------------------------------
    def _click_card(self, card):
        try:
            click_targets = []
            try:
                click_targets += card.find_elements(By.CSS_SELECTOR, "a.job-card-container__link")
            except:
                pass
            try:
                click_targets += card.find_elements(By.CSS_SELECTOR, "a.job-card-list__title")
            except:
                pass

            target = click_targets[0] if click_targets else card

            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target)
            except:
                pass

            self.human_delay(0.12, 0.30)

            try:
                ActionChains(self.driver).move_to_element(target).pause(0.05).click(target).perform()
                return True
            except:
                pass

            try:
                target.click()
                return True
            except:
                pass

            try:
                self.driver.execute_script("arguments[0].click();", target)
                return True
            except:
                return False
        except:
            return False

    # -------------------------------------------------------------------------
    # JSON SAVE
    # -------------------------------------------------------------------------
    def save_results(self):
        json_path = "json_output/linkedin_all_large_companies_jobs.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Saved JSON (jobs={len(self.jobs)})")

    # -------------------------------------------------------------------------
    # MAIN SCRAPE
    # -------------------------------------------------------------------------
    def scrape_all_companies(self, companies, max_pages_per_url=None):
        try:
            logger.info(f"Starting to scrape {len(companies)} companies")
            logger.info("You will sign in ONCE at the beginning (cookies persisted)")
            self._ensure_logged_in_once(companies[0]["urls"][0])

            for idx, c in enumerate(companies, 1):
                company_name = c["name"]
                url_list = c["urls"]
                self.company_counts.setdefault(company_name, 0)

                logger.info("\n" + "=" * 70)
                logger.info(f"COMPANY {idx}/{len(companies)}: {company_name}")
                logger.info(f"Custom URLs: ( {len(url_list)} )")
                logger.info("=" * 70)

                for uidx, url in enumerate(url_list, 1):
                    logger.info("-" * 70)
                    logger.info(f"URL {uidx}/{len(url_list)} for {company_name}")
                    logger.info(url[:220] + ("..." if len(url) > 220 else ""))
                    logger.info("-" * 70)

                    self.scrape_single_url(company_name, url, max_pages=max_pages_per_url)
                    self.human_delay(1.5, 3.0)

                self.human_delay(2.0, 4.0)

        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass

    def scrape_single_url(self, company_name, url, max_pages=None):
        base_url = self._normalize_search_url(url)
        base_url = self._set_query_param(base_url, "start", 0)

        logger.info(f"Opening: {base_url[:160]}...")

        if not self.safe_get(base_url, retries=3, wait_after=5.5):
            logger.error("Failed to open URL after retries")
            return

        if self._looks_logged_out():
            logger.error("Looks logged out after navigation. Aborting this URL.")
            return

        if not self._wait_for_results_or_end(timeout=25):
            logger.warning("No results/end detected on first page for this URL")
            return

        page_count = 0
        prev_sig = None

        while True:
            page_count += 1
            start = (page_count - 1) * self.PAGE_SIZE
            logger.info(f"  Page {page_count} (start={start})")

            if max_pages and page_count > max_pages:
                break

            if page_count > 1:
                next_url = self._set_query_param(base_url, "start", start)

                if not self.safe_get(next_url, retries=3, wait_after=4.5):
                    logger.warning("Failed to load next page; attempting recovery and retry...")
                    if not self.recover_driver(next_url):
                        break

                if self._looks_logged_out():
                    logger.error("Looks logged out mid-run (checkpoint/captcha). Stopping this URL.")
                    break

                if not self._wait_for_results_or_end(timeout=25):
                    logger.info("  End detected")
                    break

            try:
                if not self._ensure_cards_loaded(target=self.PAGE_SIZE, max_scrolls=12):
                    logger.info("  No cards after load attempts")
                    break
            except Exception as e:
                if self._is_driver_dead(e):
                    logger.warning("Driver died while loading cards. Recovering and retrying page...")
                    if not self.recover_driver(self.driver.current_url):
                        break
                    continue
                raise

            cards = self._find_job_cards()
            if not cards:
                logger.info("  No job cards found")
                break

            sig = self._page_signature(n=10)
            if sig and sig == prev_sig:
                logger.info("  Page signature repeated (end reached or cap)")
                break
            prev_sig = sig

            try:
                found = self._extract_jobs_on_page(company_name)
            except Exception as e:
                if self._is_driver_dead(e):
                    logger.warning("Driver died mid-extraction. Recovering and retrying page...")
                    if not self.recover_driver(self.driver.current_url):
                        break
                    continue
                raise

            logger.info(f"  Found {found} jobs on this page")
            self.save_results()

    def _extract_jobs_on_page(self, company_name):
        count = 0
        cards = self._find_job_cards()
        if not cards:
            return 0

        prev_job_id = self._extract_job_id_from_url(self.driver.current_url)
        limit = min(len(cards), self.PAGE_SIZE)

        for i in range(limit):
            try:
                cards = self._find_job_cards()
                if i >= len(cards):
                    break

                card = cards[i]
                if not self._click_card(card):
                    continue

                job_url, job_id = self._wait_for_url_job_change(prev_job_id, timeout=7.0)
                prev_job_id = job_id

                if not self._wait_for_job_details_loaded():
                    continue
                self.human_delay(0.25, 0.65)

                title = self._extract_title()
                if title == "N/A":
                    continue

                location, posted = self._extract_location_posted()
                description = self._extract_description()
                salary = self._extract_salary_from_description(description)

                self.company_counts[company_name] += 1
                company_job_count = self.company_counts[company_name]

                url_for_json = self._job_permalink(job_id)

                job_data = {
                    "title": title,
                    "company": company_name,
                    "location": location,
                    "salary_range": salary,
                    "posted_date": posted,
                    "description": description,
                    "url": url_for_json if url_for_json else "N/A",
                    "job_id": job_id,
                    "company_job_count": company_job_count,
                    "scraped_at": datetime.now().isoformat(),
                }

                self.jobs.append(job_data)
                count += 1

            except (StaleElementReferenceException, TimeoutException):
                continue
            except Exception as e:
                if self._is_driver_dead(e):
                    raise
                continue

        return count


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    companies = [
        # TD
        {
            "name": "TD",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4359370027&f_C=2775&f_E=2&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R",
                "https://www.linkedin.com/jobs/search/?currentJobId=4354588573&f_C=2775&f_E=1%2C3%2C5%2C6&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R",
                "https://www.linkedin.com/jobs/search/?currentJobId=4354667828&f_C=2775&f_E=4&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R&spellCorrectionEnabled=true",
            ],
        },

        # Scotiabank
        {
            "name": "Scotiabank",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4354685711&f_C=3139%2C339802%2C2143489%2C80676%2C11755816%2C52194332%2C40826861%2C10494059%2C51686%2C31712%2C35466629%2C433549%2C65006386&f_E=1%2C2%2C3%2C6&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER",
                "https://www.linkedin.com/jobs/search/?currentJobId=4344892959&f_C=3139%2C339802%2C2143489%2C80676%2C11755816%2C52194332%2C40826861%2C10494059%2C51686%2C31712%2C35466629%2C433549%2C65006386&f_E=4%2C5&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER",
            ],
        },

        # Desjardins
        {
            "name": "Desjardins",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4351637376&f_C=6331&f_E=1%2C2%2C3&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER",
                "https://www.linkedin.com/jobs/search/?currentJobId=4330262836&f_C=6331&f_E=4%2C5%2C6&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER",
            ],
        },

        # BMO
        {
            "name": "BMO",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4346723960&f_C=2587%2C2589%2C2590%2C2591%2C3634186%2C688460%2C708682&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R",
                "https://www.linkedin.com/jobs/search/?currentJobId=4354527050&f_C=164126&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R",
            ],
        },

        # CIBC
        {
            "name": "CIBC",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4354565228&f_C=1826%2C11243664%2C25040356&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4354565228%2C4358273818%2C4329052416%2C4354681752%2C4346912340%2C4346883106%2C4346603171%2C4328903579%2C4328823860",
            ],
        },

        # National Bank of Canada
        {
            "name": "National Bank of Canada",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4351763019&f_C=165099%2C9256959&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4351763019%2C4350732628%2C4350477771%2C4328862096%2C4328217368%2C4351963636%2C4350597200%2C4328346465%2C4351571536",
            ],
        },

        # RBC
        {
            "name": "RBC",
            "urls": [
                "https://www.linkedin.com/jobs/search/?currentJobId=4344566252&f_C=1808&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=DD",
                "https://www.linkedin.com/jobs/search/?currentJobId=4358155782&f_C=1805%2C1806%2C74348441%2C2421846%2C11324622%2C1800%2C2421842%2C2546409%2C5578%2C7232%2C862933&geoId=92000000&origin=JOB_SEARCH_PAGE_JOB_FILTER&sortBy=R",
            ],
        },
    ]

    logger.info("Starting Multi-Company LinkedIn Scraper")
    logger.info(f"Total companies to scrape: {len(companies)}")

    scraper = MultiCompanyScraper()
    scraper.scrape_all_companies(companies, max_pages_per_url=None)
    scraper.save_results()

    logger.info("=" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total jobs scraped: {len(scraper.jobs)}")
    logger.info("Saved to:")
    logger.info("  - json_output/linkedin_all_large_companies_jobs.json")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
