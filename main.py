import asyncio
import csv
from urllib.parse import urlparse
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

USER_DATA_DIR = "./user_data"
PROFILE_URL = "https://www.meetup.com/ru-RU/members/398792140/"


async def scrape_groups(page):
    groups = []

    while True:
        await page.wait_for_selector('div[data-testid="groups-container"]')
        group_divs = await page.query_selector_all('div[data-testid="groups-container"] > div')

        # Сохраняем уже найденные группы, чтобы избежать дубликатов
        existing_urls = set(g["group_url"] for g in groups)

        for group_div in group_divs:
            link_el = await group_div.query_selector('a.ds-font-body-medium')
            if not link_el:
                continue
            group_name = (await link_el.inner_text()).strip()
            group_url = await link_el.get_attribute('href')
            if group_url and not group_url.startswith('http'):
                group_url = "https://www.meetup.com" + group_url
            if group_url not in existing_urls:
                groups.append({"group_name": group_name, "group_url": group_url})
                existing_urls.add(group_url)

        # Проверяем есть ли кнопка "Show more groups"
        show_more_buttons = await page.query_selector_all('[data-testid="show-more-groups"]')
        if show_more_buttons:
            show_more_btn = show_more_buttons[-1]  # last button
            is_disabled = await show_more_btn.get_property('disabled')
            if is_disabled and await is_disabled.json_value():
                break
            try:
                await show_more_btn.click()
                await page.wait_for_timeout(1500)  # wait for new groups to load
            except Exception as e:
                print(f"Error clicking show more button: {e}")
                break
        else:
            break


    return groups


async def scrape_organizers(page, group_url):
    # Go to group page
    await page.goto(group_url)
    # Wait for the organizer photo link
    try:
        await page.wait_for_selector('a#organizer-photo-photo', timeout=10000)
    except PlaywrightTimeoutError:
        print(f"⚠️ Organizer link not found on {group_url}")
        return []

    # Click on the organizer photo link to open the organizers list
    await page.click('a#organizer-photo-photo')

    # Wait for the organizers list to load
    try:
        await page.wait_for_selector('ul.flex.w-full.flex-col.space-y-5', timeout=10000)
    except PlaywrightTimeoutError:
        print(f"⚠️ Organizers list not found on {group_url}")
        return []

    # Query all <li> elements inside the organizers list
    organizers_li = await page.query_selector_all('ul.flex.w-full.flex-col.space-y-5 > li')
    organizers = []

    for li in organizers_li:
        # Find anchor with organizer name and profile link
        a = await li.query_selector('a.select-none.font-medium')
        if not a:
            continue
        name = (await a.inner_text()).strip()
        profile_url = await a.get_attribute('href')
        # Make absolute if relative
        if profile_url and not profile_url.startswith('http'):
            profile_url = "https://www.meetup.com" + profile_url
        organizers.append({"name": name, "profile_url": profile_url})

    return organizers

async def main():
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            USER_DATA_DIR,
            headless=False,
            channel="chrome",
            viewport={"width": 1280, "height": 800},
        )
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(PROFILE_URL)

        if "/login" in page.url:
            print("Please log in manually in the opened browser window.")
            await page.wait_for_url(PROFILE_URL, timeout=300000)
            print("Login successful.")

        groups = await scrape_groups(page)

        user_id = urlparse(PROFILE_URL).path.strip("/").split("/")[-1]
        csv_filename = f"{user_id}_groups_and_organizers.csv"

        # Open CSV for writing
        with open(csv_filename, "w", newline="", encoding="utf-8") as f:
            fieldnames = ["group_name", "group_url", "organizer_name", "organizer_profile_url"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for group in groups:
                print(f"Processing group: {group['group_name']} - {group['group_url']}")
                organizers = await scrape_organizers(page, group['group_url'])

                if not organizers:
                    # Write group with no organizers found
                    writer.writerow({
                        "group_name": group["group_name"],
                        "group_url": group["group_url"],
                        "organizer_name": "",
                        "organizer_profile_url": ""
                    })
                else:
                    for org in organizers:
                        writer.writerow({
                            "group_name": group["group_name"],
                            "group_url": group["group_url"],
                            "organizer_name": org["name"],
                            "organizer_profile_url": org["profile_url"]
                        })

        print(f"Saved data to {csv_filename}")

        print("Press Ctrl+C to exit and close browser...")
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass

        await context.close()

if __name__ == "__main__":
    asyncio.run(main())
