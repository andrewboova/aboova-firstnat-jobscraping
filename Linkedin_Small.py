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
    LinkedIn Multi-Company Jobs Scraper (JSON ONLY, NO DEDUPE, CORRECT URL + JOB_ID)

    Behaviors:
    - NO deduping
    - JSON output only
    - URL saved as the ACTUAL browser URL after clicking a card
      (LinkedIn search page updates currentJobId=... in URL)
    - job_id extracted from:
        1) currentJobId query param
        2) /jobs/view/<id> if present
    - Adds computed field:
        company_job_count = running count for that company so far
    - Saves JSON after every page
    - URL pagination start=0,25,50...
    """

    def __init__(self):
        self.jobs = []
        self.company_counts = {}
        self._signin_prompted = False

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

        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(60)
        self.wait = WebDriverWait(self.driver, 30)

        os.makedirs("json_output", exist_ok=True)

    def human_delay(self, a, b):
        time.sleep(random.uniform(a, b))

    def _is_session_valid(self):
        try:
            _ = self.driver.current_url
            return True
        except Exception as e:
            if "invalid session id" in str(e).lower():
                return False
            return True

    # -------------------------------------------------------------------------
    # URL HELPERS (FIX #1: normalize polluted URLs)
    # -------------------------------------------------------------------------
    def _set_query_param(self, url, key, value):
        parts = urlsplit(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q[key] = str(value)
        new_query = urlencode(q, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    def _normalize_search_url(self, url):
        """
        Strip params that can constrain/anchor results and break pagination.
        Keep real filters (f_C, geoId, keywords, etc). Force start=0.
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

    # -------------------------------------------------------------------------
    # JOB ID PARSING
    # -------------------------------------------------------------------------
    def _extract_job_id_from_url(self, url):
        """
        Extract job_id from:
          - currentJobId=<id>
          - /jobs/view/<id>
        """
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
        """
        After clicking a card, LinkedIn updates the URL with currentJobId=...
        Wait until currentJobId differs from previous_job_id.
        """
        end = time.time() + timeout
        while time.time() < end:
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

    # FIX #2: don't early-break if results haven't loaded yet
    def _wait_for_results_or_end(self, timeout=25):
        """
        Wait until either job cards exist OR a no-results/end marker is visible.
        Prevents false 'No more pages available' on slow loads.
        """
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

    # FIX #3: do not over-scroll into next pages
    def _ensure_cards_loaded(self, target=25, max_scrolls=12):
        """
        Load enough cards for this start= page without over-scrolling.
        """
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

        # Garbage protection
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
    # CLICKING (more reliable than clicking li)
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

            self.human_delay(0.15, 0.35)

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
        json_path = "json_output/linkedin_all_small_companies_jobs.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Saved JSON (jobs={len(self.jobs)})")

    # -------------------------------------------------------------------------
    # MAIN SCRAPE
    # -------------------------------------------------------------------------
    def scrape_all_companies(self, companies, max_pages_per_company=None):
        try:
            logger.info(f"Starting to scrape {len(companies)} company job pages")
            logger.info("You will sign in ONCE at the beginning")

            # ---- LOGIN LOGIC UNCHANGED (exact same behavior as your script) ----
            if companies and not self._signin_prompted:
                self.driver.get(companies[0]["url"])
                time.sleep(6)

                self.driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => false});
                """)

                input("Log into LinkedIn (Google sign-in fine), then press ENTER when job list is visible... ")
                self._signin_prompted = True
            # ------------------------------------------------------------------

            for idx, c in enumerate(companies, 1):
                if not self._is_session_valid():
                    logger.error(f"Session lost at company {idx}. Stopping.")
                    break

                company_name = c["name"]
                url = c["url"]
                self.company_counts.setdefault(company_name, 0)

                logger.info("\n" + "=" * 70)
                logger.info(f"COMPANY {idx}/{len(companies)}: {company_name}")
                logger.info("=" * 70)

                self.scrape_single_company(company_name, url, max_pages_per_company)
                self.human_delay(2, 4)

        finally:
            try:
                self.driver.quit()
            except:
                pass

    def scrape_single_company(self, company_name, url, max_pages=None):
        try:
            # FIX #1: normalize polluted URLs
            base_url = self._normalize_search_url(url)
            base_url = self._set_query_param(base_url, "start", 0)

            logger.info(f"Opening: {base_url[:160]}...")
            self.driver.get(base_url)
            time.sleep(6)

            # FIX #2: wait for results or explicit end
            if not self._wait_for_results_or_end(timeout=25):
                logger.warning("No results/end detected on first page")
                return

            page_count = 0
            seen_first_listing_ids = set()

            while True:
                page_count += 1
                logger.info(f"  Page {page_count}")

                if max_pages and page_count > max_pages:
                    break

                # FIX #3: do not over-scroll beyond page capacity
                if not self._ensure_cards_loaded(target=25, max_scrolls=12):
                    logger.info("  No cards after load attempts")
                    break

                cards = self._find_job_cards()
                if not cards:
                    logger.info("  No more pages available")
                    break

                # End-detection: if first listing id repeats, pagination loop / end reached
                first_listing_id = self._get_listing_anchor_id(cards[0])
                if first_listing_id and first_listing_id in seen_first_listing_ids:
                    logger.info("  Pagination repeated (end reached)")
                    break
                if first_listing_id:
                    seen_first_listing_ids.add(first_listing_id)

                found = self._extract_jobs_on_page(company_name)
                logger.info(f"  Found {found} jobs on this page")

                self.save_results()

                next_start = page_count * 25
                next_url = self._set_query_param(base_url, "start", next_start)

                self.driver.get(next_url)
                self.human_delay(2.2, 4.0)

                # FIX #2: wait for results before deciding end
                if not self._wait_for_results_or_end(timeout=25):
                    logger.info("  No more pages available (end detected)")
                    break

        except InvalidSessionIdException:
            raise
        except Exception as e:
            logger.error(f"Error scraping company {company_name}: {str(e)}")

    def _extract_jobs_on_page(self, company_name):
        count = 0
        cards = self._find_job_cards()
        if not cards:
            return 0

        prev_job_id = self._extract_job_id_from_url(self.driver.current_url)

        for i in range(len(cards)):
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

                job_data = {
                    "title": title,
                    "company": company_name,
                    "location": location,
                    "salary_range": salary,
                    "posted_date": posted,
                    "description": description,
                    "url": job_url if job_id != "N/A" else "N/A",
                    "job_id": job_id,
                    "company_job_count": company_job_count,
                    "scraped_at": datetime.now().isoformat(),
                }

                self.jobs.append(job_data)
                count += 1

            except (StaleElementReferenceException, TimeoutException):
                continue
            except InvalidSessionIdException:
                raise
            except Exception:
                continue

        return count


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    company_names = [
        "Fairstone Bank",
        "Questrade Financial Group",
        "Canada Mortgage and Housing Corporation",
        "EQ Bank | Equitable Bank",
        "Vancity",
        "ATB Financial",
        "nesto",
        "Tangerine",
        "Meridian Credit Union",
        "Alterna Savings",
        "First National Financial LP",
        "Servus Credit Union",
        "Coast Capital Savings",
        "CMLS Financial",
        "HomeEquity Bank",
        "DUCA Financial Services Credit Union Ltd.",
        "MCAP",
        "Sagen",
        "Ratehub.ca",
        "Haventree Bank",
        "Pine",
        "Simplii Financial",
        "Canada Guaranty",
        "M3 Financial Group",
    ]

    urls = [
        # (your existing list; unchanged)
        "https://www.linkedin.com/jobs/search/?currentJobId=4330597803&f_C=16230872&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4330597803%2C4330587814%2C4330597804%2C4330367862%2C4330587816%2C4324331767%2C4330587488%2C4330507453%2C4330377404",
        "https://www.linkedin.com/jobs/search/?currentJobId=4351710352&f_C=94666%2C1617725&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4351710352%2C4347945704%2C4351630320%2C4340453609%2C4286314261%2C4351085043%2C4328167866%2C4348000049%2C4312220274",
        "https://www.linkedin.com/jobs/search/?currentJobId=4344418250&f_C=1559&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4344418250%2C4340622752%2C4346429683%2C4358650201%2C4358434376%2C4358463827%2C4351776442%2C4351972287%2C4358571594",
        "https://www.linkedin.com/jobs/search/?currentJobId=4343261067&f_C=515301&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4343261067%2C4343977469%2C4358561396%2C4358333030%2C4331155416%2C4345264051%2C4344459377%2C4338290250%2C4354377430",
        "https://www.linkedin.com/jobs/search/?currentJobId=4358334501&f_C=10612&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4358334501%2C4358117218%2C4328956505%2C4346879517%2C4240349174%2C4343476886%2C4346621927%2C4343474743%2C4330398340",
        "https://www.linkedin.com/jobs/search/?currentJobId=4342737318&f_C=12526&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4342737318%2C4354519848%2C4358270827%2C4354579393%2C4358344076%2C4358314451%2C4358205853%2C4358136059%2C4343341499",
        "https://www.linkedin.com/jobs/search/?currentJobId=4302591481&f_C=11542350&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4302591481%2C4303207029%2C4311733259%2C4303201098%2C4302565060%2C4311731339%2C4341754381%2C4311741082%2C4302550748",
        "https://www.linkedin.com/jobs/search/?currentJobId=4330397630&f_C=433549&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4330397630%2C4255279152%2C4344892959%2C4329827524%2C4317969184%2C4316989409%2C4330497631%2C4323556123%2C4322430549",
        "https://www.linkedin.com/jobs/search/?currentJobId=4343759210&f_C=20695&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4343759210%2C4344009190%2C4344416482%2C4344826002%2C4358285954%2C4343689839%2C4343249754%2C4342694067%2C4286210481",
        "https://www.linkedin.com/jobs/search/?currentJobId=4346889752&f_C=43621&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4346889752%2C4322480390%2C4336125241%2C4344539863%2C4338444950%2C4354320298%2C4310627649%2C4349306638%2C4304149470",
        "https://www.linkedin.com/jobs/search/?currentJobId=4330527635&f_C=54994&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4330527635%2C4330447648%2C4330377435%2C4330547523%2C4330397448%2C4330549696%2C4330547524%2C4330537513%2C4330407711",
        "https://www.linkedin.com/jobs/search/?currentJobId=4330587166&f_C=572573&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4330587166%2C4321890340%2C4327164445%2C4329896774%2C4319883991%2C4330577125%2C4330492201%2C4330864598%2C4330610288",
        "https://www.linkedin.com/jobs/search/?currentJobId=4350847221&f_C=14773%2C19073687%2C18868450&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4350847221%2C4349670181%2C4351337401%2C4349731082%2C4347675923%2C4301019377%2C4349451048%2C4345976989%2C4350354614",
        "https://www.linkedin.com/jobs/search/?currentJobId=4339397426&f_C=384908&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4339397426%2C4345600999%2C4262247868%2C4342123920%2C4315886703%2C4317378870%2C4246121506%2C4288329495%2C4250766588",
        "https://www.linkedin.com/jobs/search/?currentJobId=4351627595&f_C=1180768&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4351627595%2C4347804501%2C4350995720%2C4337959697%2C4350330013%2C4347626414%2C4351750915%2C4351236504%2C4349843475",
        "https://www.linkedin.com/jobs/search/?currentJobId=4326872488&f_C=1052567&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4326872488%2C4339350469%2C4354399103%2C4344932113%2C4345901356%2C4335667026%2C4340307156%2C4354651752%2C4344892791",
        "https://www.linkedin.com/jobs/search/?currentJobId=4327283558&f_C=17613&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4327283558%2C4327716224%2C4328220228%2C4326806760%2C4326652650%2C4330915977%2C4330899146",
        "https://www.linkedin.com/jobs/search/?currentJobId=4329931267&f_C=69196583&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4329931267%2C4329777723%2C4329841704%2C4329795088%2C4329804442%2C4329777728%2C4329804441%2C4329881694%2C4329921323",
        "https://www.linkedin.com/jobs/search/?currentJobId=4312329766&f_C=2349594&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4312329766%2C4340358558%2C4339212424%2C4340308740%2C4342394951%2C4157861988%2C4317578486%2C4042381522%2C4328914593",
        "https://www.linkedin.com/jobs/search/?currentJobId=4344505587&f_C=1311339&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4344505587%2C4344336450%2C4343375041%2C4346209558%2C4278962986%2C4329090844%2C4344396450%2C4329611932%2C4345000693",
        "https://www.linkedin.com/jobs/search/?currentJobId=4247803578&f_C=74353206&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4247803578%2C4346631355%2C4339193213",
        "https://www.linkedin.com/jobs/search/?currentJobId=4358117016&f_C=11243664&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4358117016%2C4329282791%2C4345194319",
        "https://www.linkedin.com/jobs/search/?currentJobId=4342352788&f_C=1930280&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4342352788%2C4342482299%2C4342400368",
        "https://www.linkedin.com/jobs/search/?currentJobId=4354534369&f_C=28134981&geoId=92000000&origin=COMPANY_PAGE_JOBS_CLUSTER_EXPANSION&originToLandingJobPostings=4354534369",
    ]

    if len(company_names) != len(urls):
        logger.error(f"Company names count ({len(company_names)}) does not match URLs count ({len(urls)}).")
        return

    companies = [{"name": company_names[i], "url": urls[i]} for i in range(len(urls))]

    logger.info("Starting Multi-Company LinkedIn Scraper")
    logger.info(f"Total companies to scrape: {len(companies)}")

    scraper = MultiCompanyScraper()
    scraper.scrape_all_companies(companies, max_pages_per_company=None)
    scraper.save_results()

    logger.info("=" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total jobs scraped: {len(scraper.jobs)}")
    logger.info("Saved to:")
    logger.info("  - json_output/linkedin_all_small_companies_jobs.json")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
