import asyncio
import csv
import random
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page, BrowserContext

# --- SETTINGS ---
USER_DATA_DIR = "./user_data"
PROFILE_URL = "https://www.meetup.com/ru-RU/members/398792140/"
# Number of parallel workers to scrape organizers
MAX_WORKERS = 3
# Random delay between actions to mimic human behavior
MIN_DELAY_S = 1
MAX_DELAY_S = 2

# --- QUEUES ---
groups_queue = asyncio.Queue()
results_queue = asyncio.Queue()


async def group_producer(page: Page):
    """
    Finds all group URLs on the main profile page and puts them into the groups_queue.
    This acts as the "Producer".
    """
    print("Starting group producer...")
    try:
        await page.wait_for_selector('div[data-testid="groups-container"]', timeout=30000)
        
        processed_urls = set()

        while True:
            group_divs = await page.query_selector_all('div[data-testid="groups-container"] > div')

            for group_div in group_divs:
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

            # --- Advanced Click & Verification Logic ---
            initial_group_count = len(await page.query_selector_all('div[data-testid="groups-container"] > div'))

            show_more_buttons = await page.query_selector_all('[data-testid="show-more-groups"]')
            if not show_more_buttons:
                print("Producer: No 'Show more' button found. All groups loaded.")
                break

            show_more_btn = show_more_buttons[-1]
            if await show_more_btn.is_disabled():
                print("Producer: 'Show more' button is disabled. All groups loaded.")
                break

            # 1. Pre-click check: Is the button inside an iframe (a common ad technique)?
            is_in_iframe = await show_more_btn.evaluate("node => !!node.closest('iframe')")
            if is_in_iframe:
                print("Producer: 'Show more' button is inside an iframe, likely an ad. Skipping click and waiting.")
                await asyncio.sleep(5)  # Wait for the ad to hopefully disappear
                continue

            # 2. Execute a "quick click" and then verify the outcome.
            try:
                current_url = page.url
                # Click without waiting for navigation, to avoid getting stuck on ad pages.
                await show_more_btn.click(timeout=5000, no_wait_after=True)

                # 3. Post-click check: Did the URL change?
                await asyncio.sleep(0.5)  # Give a brief moment for a potential redirect to register.
                if page.url != current_url:
                    print(f"Producer: Page URL changed unexpectedly to {page.url}. Stopping producer.")
                    break

                # 4. Wait for the expected result: more groups.
                await page.wait_for_function(
                    f"document.querySelectorAll('div[data-testid=\"groups-container\"] > div').length > {initial_group_count}",
                    timeout=15000
                )
                print("Producer: Clicked 'Show more', new groups loaded.")

            except PlaywrightTimeoutError:
                print("Producer: Timed out waiting for new groups to load after click. Assuming it's the end.")
                break
            except Exception as e:
                print(f"Producer: An error occurred while clicking 'Show more': {e}. Assuming it's the end.")
                break
        
        print(f"Producer finished. Found {len(processed_urls)} groups.")

    except Exception as e:
        print(f"An error occurred in the group producer: {e}")
    finally:
        # Signal that the producer is done by putting None for each worker
        for _ in range(MAX_WORKERS):
            await groups_queue.put(None)


async def organizer_worker(context: BrowserContext, worker_id: int):
    """
    Takes a group from the queue, scrapes its organizers, and puts the result into the results_queue.
    This acts as the "Consumer/Worker".
    """
    print(f"[Worker {worker_id}] Starting...")
    page = await context.new_page()

    while True:
        group_data = await groups_queue.get()
        if group_data is None:
            # End of queue
            groups_queue.task_done()
            break

        group_name = group_data["group_name"]
        group_url = group_data["group_url"]
        print(f"[Worker {worker_id}] Processing group: {group_name}")

        try:
            await page.goto(group_url, timeout=60000)
            
            # Check for CAPTCHA or access issues
            if "human" in (await page.title()).lower() or "robot" in (await page.title()).lower():
                print(f"[Worker {worker_id}] CAPTCHA detected on {group_url}. Skipping for now.")
                # Optionally, put the item back in the queue for a later retry
                # await groups_queue.put(group_data) 
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
            
            # Random delay to be less aggressive
            await asyncio.sleep(random.uniform(MIN_DELAY_S, MAX_DELAY_S))

        except PlaywrightTimeoutError:
            print(f"⚠️ [Worker {worker_id}] Timeout on {group_url}. Skipping.")
        except Exception as e:
            print(f"An error occurred in worker {worker_id} on {group_url}: {e}")
        finally:
            groups_queue.task_done()

    await page.close()
    print(f"[Worker {worker_id}] Finished.")


async def scrape_organizers_from_page(page: Page, group_url: str):
    """
    Helper function to scrape organizers from an already loaded group page.
    """
    try:
        await page.wait_for_selector('a#organizer-photo-photo', timeout=10000)
    except PlaywrightTimeoutError:
        # This is a valid case for groups with no visible organizer section
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
    """
    Takes results from the results_queue and writes them to a CSV file.
    """
    print("Starting results writer...")
    processed_count = 0
    
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["group_name", "group_url", "organizer_name", "organizer_profile_url"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            try:
                # Wait for a result with a timeout
                result = await asyncio.wait_for(results_queue.get(), timeout=30.0)
                writer.writerow(result)
                f.flush()  # Force write to disk
                processed_count += 1
                results_queue.task_done()
            except asyncio.TimeoutError:
                # If no results for 30s, check if work is done
                if groups_queue.empty() and all(w.done() for w in worker_tasks):
                    print("Writer: Timed out and all workers are done. Exiting.")
                    break
                else:
                    continue # Continue waiting if workers are still active

    print(f"Writer finished. Wrote {processed_count} rows to {csv_filename}.")


async def main():
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
        )

        # --- Tab Guard: Automatically close unexpected new pages ---
        async def handle_new_page(page):
            # This handler will be triggered for any new page created after it's registered.
            print(f"[Tab Guard] Unexpected new page opened: {page.url}. Closing it.")
            await asyncio.sleep(1)  # Give it a moment to settle
            try:
                await page.close()
            except Exception as e:
                print(f"[Tab Guard] Failed to close page: {e}")
        
        context.on("page", handle_new_page)
        # --- End of Tab Guard ---

        # Get the initial page, or create one if none exist.
        page = context.pages[0] if context.pages else await context.new_page()
        # IMPORTANT: Remove the handler for the main page so it doesn't close itself.
        context.remove_listener("page", handle_new_page)
        
        await page.goto(PROFILE_URL)

        if "/login" in page.url:
            print("Please log in manually in the opened browser window.")
            # Re-register the handler after successful login
            context.on("page", handle_new_page)
            await page.wait_for_url(PROFILE_URL, timeout=300000)
            print("Login successful.")

        user_id = urlparse(PROFILE_URL).path.strip("/").split("/")[-1]
        csv_filename = f"{user_id}_groups_and_organizers.csv"

        # --- Start all tasks ---
        producer_task = asyncio.create_task(group_producer(page))
        
        global worker_tasks
        worker_tasks = [
            asyncio.create_task(organizer_worker(context, i + 1))
            for i in range(MAX_WORKERS)
        ]
        
        writer_task = asyncio.create_task(results_writer(csv_filename))

        # --- Wait for tasks to complete ---
        await asyncio.gather(producer_task)
        print("Group producer has finished. Waiting for workers to process remaining items.")
        
        await groups_queue.join() # Wait for all groups to be processed by workers
        print("All groups have been processed. Waiting for writer to finish.")
        
        # The writer will exit by timeout once all work is done
        await writer_task

        print(f"Finished processing. All data saved to {csv_filename}")
        print("Scraping complete. The browser will remain open for inspection.")
        print("Press Ctrl+C in this terminal to close the browser and exit.")

        try:
            # This will wait indefinitely until interrupted
            await asyncio.Event().wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("\nExiting... Closing browser.")
        finally:
            await context.close()

if __name__ == "__main__":
    # This global is needed for the writer to check worker status
    worker_tasks = []
    asyncio.run(main())