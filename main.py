import asyncio
import csv
import time
import random
import os
from urllib.parse import urlparse
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page, BrowserContext

# --- SETTINGS ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXTENSION_PATH = os.path.join(SCRIPT_DIR, "uBlock0.chromium")
USER_DATA_DIR = "./user_data"
PROFILE_URL = "https://www.meetup.com/ru-RU/members/2883433/"
# Number of parallel workers to scrape organizers
MAX_WORKERS = 3
# Random delay between actions to mimic human behavior
MIN_DELAY_S = 0.5
MAX_DELAY_S = 1

# --- QUEUES ---
groups_queue = asyncio.Queue()
results_queue = asyncio.Queue()

# --- PROGRESS BARS ---
producer_bar = None
worker_bar = None

async def group_producer(page: Page):
    global producer_bar
    producer_bar = tqdm(desc="Groups Found", unit=" groups", position=0, leave=False)
    try:
        await page.wait_for_selector('div[data-testid="groups-container"]', timeout=30000)
        processed_urls = set()

        # --- FIX 1: Process initially loaded groups ---
        initial_group_divs = await page.query_selector_all('div[data-testid="groups-container"] > div')
        for group_div in initial_group_divs:
            link_el = await group_div.query_selector('a.ds-font-body-medium')
            if not link_el:
                continue
            group_url = await link_el.get_attribute('href')
            if group_url and not group_url.startswith('http'):
                group_url = "https://www.meetup.com" + group_url
            if group_url and group_url not in processed_urls:
                group_name = (await link_el.inner_text()).strip()
                await groups_queue.put({"group_name": group_name, "group_url": group_url})
                processed_urls.add(group_url)
                if producer_bar is not None:
                    producer_bar.update(1)
        
        max_retries = 3
        while True:
            group_divs = await page.query_selector_all('div[data-testid="groups-container"] > div')
            initial_count = len(group_divs)

            show_more_buttons = await page.query_selector_all('[data-testid="show-more-groups"]')
            if not show_more_buttons:
                break # No more groups to load

            show_more_btn = show_more_buttons[-1]
            if await show_more_btn.is_disabled():
                break # Button is disabled, no more groups

            is_in_iframe = await show_more_btn.evaluate("node => !!node.closest('iframe')")
            if is_in_iframe:
                await asyncio.sleep(5)
                continue

            try:
                await show_more_btn.wait_for_element_state("visible", timeout=5000)
                await show_more_btn.wait_for_element_state("enabled", timeout=5000)
            except Exception:
                await asyncio.sleep(2)
                continue

            for attempt in range(max_retries):
                try:
                    await show_more_btn.click()
                    new_groups_loaded = False
                    max_wait_sec = 10
                    interval = 0.3
                    elapsed = 0
                    while elapsed < max_wait_sec:
                        await asyncio.sleep(interval)
                        elapsed += interval
                        current_divs = await page.query_selector_all('div[data-testid="groups-container"] > div')
                        new_count = len(current_divs)
                        if new_count > initial_count:
                            new_groups_loaded = True
                            new_divs = current_divs[initial_count:]
                            for group_div in new_divs:
                                link_el = await group_div.query_selector('a.ds-font-body-medium')
                                if not link_el:
                                    continue
                                group_url = await link_el.get_attribute('href')
                                if group_url and not group_url.startswith('http'):
                                    group_url = "https://www.meetup.com" + group_url
                                if group_url and group_url not in processed_urls:
                                    group_name = (await link_el.inner_text()).strip()
                                    await groups_queue.put({"group_name": group_name, "group_url": group_url})
                                    processed_urls.add(group_url)
                                    if producer_bar is not None:
                                        producer_bar.update(1)
                            break # Exit inner while loop
                    if new_groups_loaded:
                        break # Exit attempt loop
                    else:
                        if attempt >= max_retries - 1:
                            # Could not load more groups, assume we are done.
                            # To be safe, we just return here, finally will handle sentinels.
                            return
                except Exception as e:
                    print(f"Producer: Exception during click attempt: {e}")
                    if attempt == max_retries - 1:
                        # Exit if max retries are reached due to exceptions
                        return
                    await asyncio.sleep(2)
    except Exception as e:
        print(f"Producer error: {e}")
    finally:
        # --- FIX 2: Always send sentinels ---
        for _ in range(MAX_WORKERS):
            await groups_queue.put(None)
        if producer_bar is not None:
            producer_bar.close()


async def organizer_worker(context: BrowserContext, worker_id: int):
    page = await context.new_page()
    while True:
        group_data = await groups_queue.get()
        if group_data is None:
            groups_queue.task_done()
            break

        group_name = group_data["group_name"]
        group_url = group_data["group_url"]
        try:
            await page.goto(group_url, timeout=60000)
            if "human" in (await page.title()).lower() or "robot" in (await page.title()).lower():
                print(f"[Worker {worker_id}] CAPTCHA detected on {group_url}. Skipping for now.")
                groups_queue.task_done()
                continue

            organizers = await scrape_organizers_from_page(page, group_url)
            if not organizers:
                await results_queue.put({
                    "group_name": group_name,
                    "group_url": group_url,
                    "organizer_name": "",
                    "organizer_profile_url": ""
                })
            else:
                for org in organizers:
                    await results_queue.put({
                        "group_name": group_name,
                        "group_url": group_url,
                        "organizer_name": org["name"],
                        "organizer_profile_url": org["profile_url"]
                    })
            await asyncio.sleep(random.uniform(MIN_DELAY_S, MAX_DELAY_S))
        except PlaywrightTimeoutError:
            print(f"⚠️ [Worker {worker_id}] Timeout on {group_url}. Skipping.")
        except Exception as e:
            print(f"An error occurred in worker {worker_id} on {group_url}: {e}")
        finally:
            if worker_bar is not None:
                worker_bar.update(1)
            groups_queue.task_done()
    await page.close()

async def scrape_organizers_from_page(page: Page, group_url: str):
    try:
        await page.wait_for_selector('a#organizer-photo-photo', timeout=10000)
    except PlaywrightTimeoutError:
        return []
    await page.click('a#organizer-photo-photo')
    try:
        await page.wait_for_selector('ul.flex.w-full.flex-col.space-y-5', timeout=10000)
    except PlaywrightTimeoutError:
        print(f"⚠️ Organizers list not found on {group_url} after click.")
        return []
    organizers_li = await page.query_selector_all('ul.flex.w-full.flex-col.space-y-5 > li')
    organizers = []
    for li in organizers_li:
        a = await li.query_selector('a.select-none.font-medium')
        if not a:
            continue
        name = (await a.inner_text()).strip()
        profile_url = await a.get_attribute('href')
        if profile_url and not profile_url.startswith('http'):
            profile_url = "https://www.meetup.com" + profile_url
        organizers.append({"name": name, "profile_url": profile_url})
    return organizers

async def results_writer(csv_filename: str):
    processed_count = 0
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["group_name", "group_url", "organizer_name", "organizer_profile_url"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        while True:
            try:
                result = await asyncio.wait_for(results_queue.get(), timeout=30.0)
                writer.writerow(result)
                f.flush()
                processed_count += 1
                results_queue.task_done()
            except asyncio.TimeoutError:
                if groups_queue.empty() and all(w.done() for w in worker_tasks):
                    break
                else:
                    continue

async def main():
    global worker_bar, worker_tasks
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
                args=[
                    f"--disable-extensions-except={EXTENSION_PATH}",
                    f"--load-extension={EXTENSION_PATH}"
                    ],
        )
        async def handle_new_page(page):
            await asyncio.sleep(1)
            try:
                await page.close()
            except Exception as e:
                print(f"[Tab Guard] Failed to close page: {e}")
        context.on("page", handle_new_page)
        page = context.pages[0] if context.pages else await context.new_page()
        context.remove_listener("page", handle_new_page)
        await page.goto(PROFILE_URL)
        if "/login" in page.url:
            print("Please log in manually in the opened browser window.")
            context.on("page", handle_new_page)
            await page.wait_for_url(PROFILE_URL, timeout=300000)
            print("Login successful.")
        user_id = urlparse(PROFILE_URL).path.strip("/").split("/")[-1]
        csv_filename = f"{user_id}_groups_and_organizers.csv"

        # --- Start all tasks ---
        producer_task = asyncio.create_task(group_producer(page))
        worker_tasks = [
            asyncio.create_task(organizer_worker(context, i + 1))
            for i in range(MAX_WORKERS)
        ]
        writer_task = asyncio.create_task(results_writer(csv_filename))

        # --- Wait for producer to find some groups ---
        await asyncio.sleep(5) # Give producer a head start

        # --- Initialize worker_bar once producer has started ---
        q_size = groups_queue.qsize()
        if q_size > 0:
            worker_bar = tqdm(total=q_size, desc="Groups Processed", unit=" groups", position=1, leave=True)

        # --- Main loop to update worker_bar total ---
        while producer_task.done() is False:
            await asyncio.sleep(1)
            if worker_bar is not None:
                worker_bar.total = groups_queue.qsize() + (worker_bar.n) # total should be found + processed
            elif groups_queue.qsize() > 0: # If bar not initialized yet
                worker_bar = tqdm(total=groups_queue.qsize(), desc="Groups Processed", unit=" groups", position=1, leave=True)


        await producer_task # ensure producer is finished
        if worker_bar is not None:
            worker_bar.total = groups_queue.qsize() + worker_bar.n

        await groups_queue.join()

        if worker_bar is not None:
            worker_bar.close()

        await writer_task
        print(f"Finished processing. All data saved to {csv_filename}")
        print("Scraping complete. The browser will remain open for inspection.")
        print("Press Ctrl+C in this terminal to close the browser and exit.")
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("Exiting... Closing browser.")
        finally:
            await context.close()

if __name__ == "__main__":
    worker_tasks = []
    asyncio.run(main())
