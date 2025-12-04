#!/usr/bin/env python3

import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path
import logging

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S2SForecastRunner:
    def __init__(self, experiment_name):
        self.experiment_name = experiment_name
        
        # Base configuration
        self.base_path = Path("/home/gmaofcst/geos-s2s-3")
        self.experiment_dir = self.base_path / experiment_name
        self.script_dir = Path("/home/gmaofcst/ODAS/OBS/V3/D_BOSS/")
        self.forecast_dates_file = self.script_dir / "forecast_dates.txt"
        self.check_script = self.script_dir / "s2s_check.py"
        
        # Experiment-specific files
        self.job_script = self.experiment_dir / "gcm_run.j"
        
        # Timing configuration
        self.check_interval = 900  # 15 minutes
        self.max_wait_time = 7200  # 2 hours timeout
        
        # Email configuration - could be experiment-specific if needed
        self.email_list = [
            "wesley.j.davis@nasa.gov"
        ]
        
        logger.info(f"Initialized for experiment: {experiment_name}")
        logger.info(f"Experiment directory: {self.experiment_dir}")
    
    def validate_experiment(self):
        """Check if experiment directory and required files exist"""
        if not self.experiment_dir.exists():
            logger.error(f"Experiment directory does not exist: {self.experiment_dir}")
            return False
        
        required_files = [self.forecast_dates_file, self.check_script, self.job_script]
        missing_files = []
        
        for file_path in required_files:
            if not file_path.exists():
                missing_files.append(str(file_path))
        
        if missing_files:
            logger.error(f"Missing required files: {missing_files}")
            return False
        
        logger.info("Experiment validation passed")
        return True
        
    def check_forecast_date(self, input_date):
            """Check if date matches the 5-day pattern starting from Jan 1"""
            forecast_date = datetime.strptime(input_date, '%Y-%m-%d')
            jan_1 = datetime(forecast_date.year, 1, 1)
            days_since_jan_1 = (forecast_date - jan_1).days
    
            # Every 5 days starting from Jan 1
            return days_since_jan_1 % 5 == 0
            
    def wait_for_files(self, forecast_date):
        """Wait for required files using the existing check script"""
        dt = datetime.strptime(forecast_date, '%Y-%m-%d')
        year, month, day = dt.year, dt.month, dt.day
        
        check_cmd = [
            str(self.check_script),
            "--verbose",
            "--year", str(year),
            "--month", str(month),
            "--day", str(day)
        ]
        
        logger.info("Waiting for required files...")
        start_time = time.time()
        
        while time.time() - start_time < self.max_wait_time:
            try:
                result = subprocess.run(check_cmd, capture_output=True, text=True, timeout=300)
                if result.returncode == 0:
                    logger.info("All required files are available")
                    return True
                else:
                    logger.info(f"Files not ready yet, waiting {self.check_interval} seconds...")
                    time.sleep(self.check_interval)
            except subprocess.TimeoutExpired:
                logger.warning("File check script timed out")
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error running file check: {e}")
                time.sleep(self.check_interval)
        
        logger.error("Timeout waiting for files")
        return False
    
    def submit_job(self):
        """Submit the SLURM job"""
        logger.info(f"Submitting job for experiment {self.experiment_name}...")
        
        try:
            # Clean up any previous check files
            (self.experiment_dir / "ODAS_Check.txt").unlink(missing_ok=True)
            
            # Submit job from the experiment directory
            result = subprocess.run(
                ["/usr/bin/sbatch", str(self.job_script)],
                cwd=self.experiment_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                job_id = result.stdout.strip()
                logger.info(f"Job submitted successfully: {job_id}")
                return True, job_id
            else:
                logger.error(f"Job submission failed: {result.stderr}")
                return False, None
                
        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            return False, None
    
    def send_notification(self, success=True, message="", job_id=None):
        """Send email notification"""
        if success:
            subject = f"V2 ODAS Run Submitted - {self.experiment_name}"
            body = f"V2 ODAS Run Submitted for experiment: {self.experiment_name}"
            if job_id:
                body += f"\nJob ID: {job_id}"
        else:
            subject = f"V2 ODAS Run Failed - {self.experiment_name}"
            body = f"V2 ODAS Run Failed for experiment: {self.experiment_name}\nError: {message}"
        
        logger.info(f"Sending notification: {subject}")
        
        # Simple notification - could enhance later
        try:
            # For now, just log. Could implement actual email sending if needed
            logger.info(f"Would send email to {len(self.email_list)} recipients")
            logger.info(f"Subject: {subject}")
            logger.info(f"Body: {body}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    
    def run(self, forecast_date):
        """Main execution logic"""
        logger.info(f"Starting S2S forecast check for {self.experiment_name} on {forecast_date}")
        
        # Validate experiment setup
        if not self.validate_experiment():
            self.send_notification(success=False, message="Experiment validation failed")
            return 1
        
        # Check if forecast should run
        if not self.check_forecast_date(forecast_date):
            logger.info("Forecast not scheduled for this date")
            return 0
        
        # Wait for files
        if not self.wait_for_files(forecast_date):
            self.send_notification(success=False, message="Timeout waiting for input files")
            return 1
        
        # Submit job
        success, job_id = self.submit_job()
        if not success:
            self.send_notification(success=False, message="Job submission failed")
            return 1
        
        # Success notification
        self.send_notification(success=True, job_id=job_id)
        logger.info(f"S2S forecast process completed successfully for {self.experiment_name}")
        return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python s2s_forecast.py <experiment_name> <YYYY-MM-DD>")
        print("Example: python s2s_forecast.py S2S-2_1_ANA_002 2024-11-23")
        sys.exit(1)
    
    experiment_name = sys.argv[1]
    forecast_date = sys.argv[2]
    
    runner = S2SForecastRunner(experiment_name)
    exit_code = runner.run(forecast_date)
    sys.exit(exit_code)
