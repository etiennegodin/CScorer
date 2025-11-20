from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

# URL to scrape
url = "https://www.inaturalist.org/taxa/48098-Acer-rubrum#similar-tab"

# Setup Chrome options
chrome_options = Options()
# Uncomment the line below to run in headless mode (no visible browser)
# chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

driver = None
try:
    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    # Wait for the SimilarTab container to load
    print("Waiting for page to load...")
    wait = WebDriverWait(driver, 15)
    similar_tab = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".SimilarTab.container"))
    )
    
    # Give extra time for dynamic content to fully load
    time.sleep(2)
    
    # Get the HTML content
    html_content = similar_tab.get_attribute('outerHTML')
    
    print("Found SimilarTab container!")
    print("\n" + "="*50)
    
    # Parse with BeautifulSoup for cleaner output
    soup = BeautifulSoup(html_content, 'html.parser')
    print(soup.prettify())
    print("="*50)
    
    # Extract specific information (example)
    links = soup.find_all('a')
    print(f"\nFound {len(links)} links in the container")
    
    # Example: extract similar species names
    similar_species = soup.find_all(class_='taxon')
    if similar_species:
        print(f"\nSimilar species found: {len(similar_species)}")
        for species in similar_species[:5]:  # Show first 5
            name = species.get_text(strip=True)
            if name:
                print(f"  - {name}")

except Exception as e:
    print(f"Error occurred: {e}")
    
finally:
    # Always close the driver
    if driver:
        driver.quit()
        print("\nBrowser closed successfully")