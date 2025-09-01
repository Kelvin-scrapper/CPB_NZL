import undetected_chromedriver as uc
import time
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def setup_chrome_driver():
    """
    Initialize Chrome driver with download preferences
    """
    options = uc.ChromeOptions()
    
    # Set download directory
    download_dir = os.path.join(os.getcwd(), "downloads")
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    # Chrome preferences for downloads
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Optional: Run in headless mode (uncomment if needed)
    # options.add_argument("--headless")
    
    return uc.Chrome(options=options)

def find_and_click_latest_mps(driver, wait):
    """
    Find and click on the latest Monetary Policy Statement
    """
    try:
        print("Looking for the latest Monetary Policy Statement...")
        
        # Wait longer for the dynamic content to load
        print("Waiting for search results to load...")
        time.sleep(5)
        
        # Wait for the Coveo search results to be populated
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "CoveoResult")))
        
        # Additional wait to ensure all results are loaded
        time.sleep(3)
        
        # Try multiple selectors to find MPS links
        mps_links = []
        
        # Method 1: Look for links containing "Monetary Policy Statement" in the heading text
        try:
            mps_links = driver.find_elements(By.XPATH, 
                "//span[contains(@class, 'listing-card__heading-text') and contains(text(), 'Monetary Policy Statement')]")
            print(f"Method 1: Found {len(mps_links)} MPS links")
        except:
            pass
        
        # Method 2: If method 1 fails, try broader search
        if not mps_links:
            try:
                mps_links = driver.find_elements(By.XPATH, 
                    "//h4[contains(@class, 'listing-card__heading')]//span[contains(text(), 'Monetary Policy Statement')]")
                print(f"Method 2: Found {len(mps_links)} MPS links")
            except:
                pass
        
        # Method 3: Even broader search
        if not mps_links:
            try:
                all_results = driver.find_elements(By.CLASS_NAME, "CoveoResult")
                print(f"Method 3: Found {len(all_results)} total results")
                
                for result in all_results:
                    text_content = result.text.lower()
                    if "monetary policy statement" in text_content:
                        heading_element = result.find_element(By.CLASS_NAME, "listing-card__heading-text")
                        mps_links.append(heading_element)
                        
                print(f"Method 3: Found {len(mps_links)} MPS links")
            except Exception as e:
                print(f"Method 3 failed: {e}")
        
        if not mps_links:
            print("No Monetary Policy Statements found!")
            # Debug: Print page source snippet to see what's actually loaded
            print("Page title:", driver.title)
            print("Checking if results are loaded...")
            
            results = driver.find_elements(By.CLASS_NAME, "CoveoResult")
            print(f"Total results found: {len(results)}")
            
            if results:
                print("First result text:", results[0].text[:200])
            
            return False
        
        # Get the parent link element of the first (latest) MPS
        latest_mps_element = mps_links[0]
        latest_mps_link = latest_mps_element.find_element(By.XPATH, "./ancestor::a")
        latest_mps_title = latest_mps_element.text
        
        print(f"Found latest MPS: {latest_mps_title}")
        print(f"URL: {latest_mps_link.get_attribute('href')}")
        
        # Scroll to the element to make sure it's visible
        driver.execute_script("arguments[0].scrollIntoView(true);", latest_mps_link)
        time.sleep(2)
        
        # Use JavaScript click to avoid potential interception issues
        driver.execute_script("arguments[0].click();", latest_mps_link)
        print("Clicked on the latest Monetary Policy Statement")
        
        # Wait for navigation to complete
        time.sleep(3)
        return True
        
    except Exception as e:
        print(f"Error finding/clicking latest MPS: {e}")
        import traceback
        traceback.print_exc()
        return False

def scroll_to_decision_documents(driver, wait):
    """
    Scroll down to find the Decision documents section
    """
    try:
        print("Scrolling to find Decision documents section...")
        
        # Wait for page to load
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        # Look for the "Decision documents" heading
        decision_docs_heading = wait.until(
            EC.presence_of_element_located((By.XPATH, "//h2[contains(text(), 'Decision documents')]"))
        )
        
        # Scroll to the Decision documents section
        driver.execute_script("arguments[0].scrollIntoView(true);", decision_docs_heading)
        time.sleep(1)
        
        print("Found Decision documents section")
        return True
        
    except TimeoutException:
        print("Decision documents section not found, trying alternative scroll method...")
        
        # Alternative: scroll down the page gradually looking for download links
        for _ in range(5):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(1)
            
            # Check if we can find any download links
            download_links = driver.find_elements(By.CLASS_NAME, "download-card__link")
            if download_links:
                print("Found download section")
                return True
                
        return False

def download_excel_file(driver, wait):
    """
    Find and download the Excel file from the Decision documents section
    """
    try:
        print("Looking for Excel file download link...")
        
        # Look for Excel download link specifically
        # The structure shows: download-card--xlsx class for Excel files
        excel_download_link = wait.until(
            EC.element_to_be_clickable((By.XPATH, 
                "//div[contains(@class, 'download-card--xlsx')]//a[contains(@class, 'download-card__link')]"))
        )
        
        # Get file name from the link
        file_name = excel_download_link.get_attribute('data-ga4-interaction-value') or "Excel file"
        print(f"Found Excel file: {file_name}")
        
        # Scroll to the download link
        driver.execute_script("arguments[0].scrollIntoView(true);", excel_download_link)
        time.sleep(1)
        
        # Click the download link
        excel_download_link.click()
        print("Clicked Excel download link")
        
        # Wait a bit for download to start
        time.sleep(3)
        return True
        
    except TimeoutException:
        print("Excel download link not found. Looking for alternative...")
        
        # Alternative: look for any download link containing "xlsx" or "data"
        try:
            alternative_links = driver.find_elements(By.XPATH, 
                "//a[contains(@href, '.xlsx') or contains(text(), 'data') or contains(text(), 'XLSX')]")
            
            if alternative_links:
                link = alternative_links[0]
                print(f"Found alternative Excel link: {link.get_attribute('href')}")
                link.click()
                time.sleep(3)
                return True
        except:
            pass
            
        print("No Excel file found to download")
        return False

def check_download_completion(download_dir, timeout=30):
    """
    Check if download has completed by monitoring the download directory
    """
    print("Checking download completion...")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        # List files in download directory
        files = os.listdir(download_dir)
        
        # Look for .xlsx files and exclude temporary files
        xlsx_files = [f for f in files if f.endswith('.xlsx') and not f.endswith('.crdownload')]
        
        if xlsx_files:
            print(f"Download completed! Files downloaded: {xlsx_files}")
            return True
            
        time.sleep(1)
    
    print("Download timeout reached")
    return False

def main():
    """
    Main function to orchestrate the entire process
    """
    driver = None
    
    try:
        # Initialize driver
        print("Initializing Chrome driver...")
        driver = setup_chrome_driver()
        wait = WebDriverWait(driver, 30)  # Increased timeout
        
        # Navigate to RBNZ publications page
        url = "https://www.rbnz.govt.nz/research-and-publications/publications/publications-library#sort=%40computedsortdate%20descending&f:@hierarchicalz95xsz120xacontenttypetagnames=[Publication,Monetary%20Policy%20Statement]"
        print(f"Opening website: {url}")
        driver.get(url)
        
        # Wait for page to load completely
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print("Initial page loaded, waiting for search interface...")
        
        # Wait for Coveo search interface to initialize
        try:
            wait.until(EC.presence_of_element_located((By.ID, "coveo-result-list1")))
            print("Coveo search interface detected")
        except TimeoutException:
            print("Coveo interface not detected, continuing anyway...")
        
        # Additional wait for dynamic content
        time.sleep(5)
        print("Page loaded successfully")
        
        # Step 1: Find and click on the latest MPS
        if not find_and_click_latest_mps(driver, wait):
            print("Failed to find and click latest MPS")
            
            # Debug: Take a screenshot if possible
            try:
                driver.save_screenshot("debug_page.png")
                print("Screenshot saved as debug_page.png")
            except:
                pass
                
            return
            
        # Step 2: Scroll to Decision documents section
        if not scroll_to_decision_documents(driver, wait):
            print("Failed to find Decision documents section")
            return
            
        # Step 3: Download Excel file
        if not download_excel_file(driver, wait):
            print("Failed to download Excel file")
            return
            
        # Step 4: Check download completion
        download_dir = os.path.join(os.getcwd(), "downloads")
        if check_download_completion(download_dir):
            print("Process completed successfully!")
        else:
            print("Download may not have completed properly")
            
    except Exception as e:
        print(f"An error occurred in main: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if driver:
            driver.quit()
            print("Browser closed")

if __name__ == "__main__":
    main()