#!/usr/bin/env python3
"""
Standalone Hardware Thermal Gatekeeper
Monitors CPU temperature via acpi/thermal_zone and dynamically calculates
the maximum allowed concurrent long-running tasks to prevent hardware throttling.
"""

import os
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

THERMAL_ZONE_PATH = "/sys/class/thermal/thermal_zone0/temp"
TEMP_EMERGENCY = 95  # Kill ALL workloads, cooldown required
TEMP_HIGH = 85       # Heavy throttling: Max 1 concurrent heavy task
TEMP_MEDIUM = 75     # Moderate throttling: Max 2 concurrent heavy tasks
TEMP_LOW = 70        # Normal operations: Max 3 concurrent heavy tasks
MAX_LONG_RUNNING_TASKS = 3

def get_cpu_temp() -> int:
    """Reads system CPU temperature in Celsius."""
    try:
        if os.path.exists(THERMAL_ZONE_PATH):
            with open(THERMAL_ZONE_PATH, 'r') as f:
                return int(f.read().strip()) // 1000
    except Exception as e:
        logging.warning(f"Primary thermal zone unreadable: {e}")
        
    # Fallback routine iterating through available thermal zones
    for i in range(10):
        try:
            path = f"/sys/class/thermal/thermal_zone{i}/temp"
            if os.path.exists(path):
                with open(path, 'r') as f:
                    temp = int(f.read().strip()) // 1000
                    if temp > 20: 
                        return temp
        except Exception:
            continue
            
    logging.error("All thermal zones failed. Defaulting to safe fallback.")
    return 50 # Safe fallback

def calculate_capacity(current_temp: int) -> int:
    """Calculates maximum allowed concurrent I/O or CPU bound tasks based on thermal state."""
    if current_temp >= TEMP_EMERGENCY:
        logging.critical(f"EMERGENCY state reached ({current_temp}°C). Initiating emergency halt.")
        return 0
    elif current_temp >= TEMP_HIGH:
        logging.warning(f"HIGH thermal state ({current_temp}°C). Throttling heavily.")
        return 1
    elif current_temp >= TEMP_MEDIUM:
        logging.info(f"MEDIUM thermal state ({current_temp}°C). Throttling moderately.")
        return 2
    else:
        return MAX_LONG_RUNNING_TASKS

def evaluate_system_health() -> Tuple[int, int]:
    """Returns current temperature and calculated task capacity."""
    temp = get_cpu_temp()
    capacity = calculate_capacity(temp)
    return temp, capacity

if __name__ == "__main__":
    current_temp, allowed_tasks = evaluate_system_health()
    logging.info(f"System Check: Temp={current_temp}°C | Allowed Concurrent Heavy Tasks: {allowed_tasks}/{MAX_LONG_RUNNING_TASKS}")
