import subprocess
import os
import sys
from datetime import datetime

# ==========================================
# CONFIG
# ==========================================

PYTHON_PATH = sys.executable

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_FILE = os.path.join(BASE_DIR, "daily_sync_log.txt")

scripts = [
    "Get_Purchase_Invoice.py",
    "Get_Purchase_Invoice_Detail.py",
    "Get_Sales_Invoice.py",
    "Get_Sales_Invoice_Detail.py"
]

# ==========================================
# LOG FUNCTION
# ==========================================

def write_log(message):

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log_message = f"[{timestamp}] {message}"

    print(log_message)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_message + "\n")

# ==========================================
# START
# ==========================================

write_log("=" * 70)
write_log("DAILY SYNC STARTED")

# Ensure working directory is script folder
os.chdir(BASE_DIR)

# ==========================================
# RUN SCRIPTS
# ==========================================

for script in scripts:

    start_time = datetime.now()

    write_log("")
    write_log(f"STARTING SCRIPT: {script}")
    write_log("-" * 70)

    try:

        process = subprocess.Popen(
            [PYTHON_PATH, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        # ==========================================
        # REAL-TIME CONSOLE ONLY
        # ==========================================

        for line in process.stdout:

            line = line.rstrip()

            if line:
                print(line)

        process.wait()

        end_time = datetime.now()

        duration = end_time - start_time

        if process.returncode == 0:

            write_log(f"{script} COMPLETED SUCCESSFULLY")
            write_log(f"Duration: {duration}")

        else:

            write_log(f"{script} FAILED")
            write_log(f"Duration: {duration}")
            write_log(f"Return Code: {process.returncode}")

    except Exception as e:

        write_log(f"{script} CRASHED")
        write_log(str(e))

# ==========================================
# END
# ==========================================

write_log("")
write_log("ALL SYNC JOBS FINISHED")
write_log("=" * 70)
write_log("")