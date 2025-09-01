#!/usr/bin/env python3
"""
RBNZ CBP Data Pipeline Orchestrator
This script coordinates the complete process of:
1. Scraping RBNZ website for latest Monetary Policy Statement Excel files
2. Processing and mapping the data to CBP format
3. Generating both QA and final output files

Usage:
    python orchestrator.py
"""

import os
import sys
import logging
import time
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('orchestrator.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_directories():
    """
    Ensure required directories exist
    """
    directories = ['downloads', 'mapped_output', 'logs']
    
    for directory in directories:
        path = Path(directory)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        else:
            logger.info(f"Directory exists: {directory}")

def run_scraper():
    """
    Execute the main.py scraper script
    """
    logger.info("=" * 60)
    logger.info("STEP 1: Starting RBNZ Website Scraper")
    logger.info("=" * 60)
    
    try:
        # Import and run the scraper
        from main import main as scraper_main
        
        logger.info("Launching Chrome scraper...")
        scraper_main()
        
        # Check if files were downloaded
        downloads_dir = Path("downloads")
        excel_files = list(downloads_dir.glob("*.xlsx"))
        
        if excel_files:
            logger.info(f"Scraper completed successfully. Found {len(excel_files)} Excel file(s):")
            for file in excel_files:
                logger.info(f"  - {file.name} ({file.stat().st_size} bytes)")
            return True
        else:
            logger.error("Scraper completed but no Excel files found in downloads directory")
            return False
            
    except Exception as e:
        logger.error(f"Error running scraper: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_mapper():
    """
    Execute the mapping.py processing script
    """
    logger.info("=" * 60)
    logger.info("STEP 2: Starting CBP Data Mapping")
    logger.info("=" * 60)
    
    try:
        # Import and run the mapper
        from mapping import RBNZMapper
        
        downloads_dir = "./downloads"
        output_dir = "./mapped_output"
        
        logger.info(f"Initializing RBNZ Mapper...")
        logger.info(f"  - Downloads directory: {downloads_dir}")
        logger.info(f"  - Output directory: {output_dir}")
        
        mapper = RBNZMapper(downloads_dir, output_dir)
        qa_result, final_result = mapper.run()
        
        if qa_result and final_result:
            logger.info("Mapping completed successfully!")
            logger.info(f"QA Output: {qa_result}")
            logger.info(f"Final Output: {final_result}")
            return True, qa_result, final_result
        elif qa_result or final_result:
            logger.warning("Mapping partially successful:")
            if qa_result:
                logger.info(f"QA Output: {qa_result}")
            if final_result:
                logger.info(f"Final Output: {final_result}")
            return True, qa_result, final_result
        else:
            logger.error("Mapping failed - no outputs created")
            return False, None, None
            
    except Exception as e:
        logger.error(f"Error running mapper: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None

def cleanup_old_files(days_old=7):
    """
    Clean up old files to prevent disk space issues
    """
    try:
        logger.info(f"Cleaning up files older than {days_old} days...")
        
        current_time = time.time()
        cutoff_time = current_time - (days_old * 24 * 60 * 60)
        
        # Clean downloads
        downloads_dir = Path("downloads")
        if downloads_dir.exists():
            for file in downloads_dir.iterdir():
                if file.is_file() and file.stat().st_mtime < cutoff_time:
                    file.unlink()
                    logger.info(f"Deleted old download: {file.name}")
        
        # Clean old outputs (keep recent ones)
        output_dir = Path("mapped_output")
        if output_dir.exists():
            for file in output_dir.iterdir():
                if file.is_file() and file.stat().st_mtime < cutoff_time:
                    file.unlink()
                    logger.info(f"Deleted old output: {file.name}")
                    
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")

def generate_summary_report(qa_path, final_path):
    """
    Generate a summary report of the pipeline execution
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        report_path = Path("pipeline_summary.txt")
        
        with open(report_path, "w") as f:
            f.write("RBNZ CBP Data Pipeline Execution Summary\n")
            f.write("=" * 50 + "\n")
            f.write(f"Execution Time: {timestamp}\n")
            f.write(f"Pipeline Status: SUCCESS\n\n")
            
            f.write("Output Files Created:\n")
            f.write("-" * 25 + "\n")
            if qa_path:
                f.write(f"QA Output: {qa_path}\n")
                if Path(qa_path).exists():
                    size = Path(qa_path).stat().st_size
                    f.write(f"  Size: {size:,} bytes\n")
            
            if final_path:
                f.write(f"Final Output: {final_path}\n")
                if Path(final_path).exists():
                    size = Path(final_path).stat().st_size
                    f.write(f"  Size: {size:,} bytes\n")
            
            # Check downloads
            downloads_dir = Path("downloads")
            if downloads_dir.exists():
                excel_files = list(downloads_dir.glob("*.xlsx"))
                f.write(f"\nSource Files:\n")
                f.write("-" * 15 + "\n")
                for file in excel_files:
                    f.write(f"  {file.name} ({file.stat().st_size:,} bytes)\n")
        
        logger.info(f"Summary report created: {report_path}")
        return str(report_path)
        
    except Exception as e:
        logger.warning(f"Error creating summary report: {e}")
        return None

def main():
    """
    Main orchestrator function
    """
    start_time = time.time()
    
    logger.info("🚀 Starting RBNZ CBP Data Pipeline Orchestrator")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Step 0: Setup
        logger.info("Setting up directories...")
        setup_directories()
        
        # Step 1: Scrape RBNZ website
        scraper_success = run_scraper()
        
        if not scraper_success:
            logger.error("❌ Pipeline failed at scraping stage")
            sys.exit(1)
        
        # Small delay to ensure files are fully written
        logger.info("Waiting for file system sync...")
        time.sleep(2)
        
        # Step 2: Process and map data
        mapper_success, qa_path, final_path = run_mapper()
        
        if not mapper_success:
            logger.error("❌ Pipeline failed at mapping stage")
            sys.exit(1)
        
        # Step 3: Generate summary
        summary_path = generate_summary_report(qa_path, final_path)
        
        # Step 4: Cleanup
        cleanup_old_files()
        
        # Success summary
        execution_time = time.time() - start_time
        logger.info("=" * 60)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if qa_path:
            logger.info(f"✅ QA Output: {qa_path}")
        if final_path:
            logger.info(f"✅ Final Output: {final_path}")
        if summary_path:
            logger.info(f"📋 Summary Report: {summary_path}")
        
        logger.info("\nPipeline execution completed. Check the output files above.")
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Fatal error in orchestrator: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()