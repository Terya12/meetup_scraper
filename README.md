# Meetup Group Scraper

This script is a Python-based tool for scraping group and organizer information from Meetup.com user profiles.

## What it does

The script automates the process of:
1.  Navigating to a specified Meetup user's profile page.
2.  Finding all the groups the user is a member of.
3.  Extracting the names and profile URLs of the organizers for each of those groups.
4.  Saving the collected data into a CSV file.

## Technologies Used

*   **Python**: The core language for the script.
*   **Playwright**: For browser automation and web scraping.
*   **asyncio**: To handle asynchronous operations, making the scraping process efficient.
*   **tqdm**: To display progress bars for a better user experience.

## Quick Start

1.  **Prerequisites**: Make sure you have Python 3.8+ installed.
2.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd meetup_scraper
    ```
3.  **Install dependencies**:
    ```bash
    python -m pip install -r requirements.txt
    ```
4.  **Configure the script**:
    - Open `main.py` and change the `PROFILE_URL` variable to the Meetup user profile you want to scrape.
5.  **Run the script**:
    ```bash
    python main.py
    ```


