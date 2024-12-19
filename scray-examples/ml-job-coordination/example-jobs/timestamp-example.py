import torch
from datetime import datetime as dt
import time

def get_device_info():
    """Check if CUDA is available and return the device."""
    if torch.cuda.is_available():
        n_gpus = torch.cuda.device_count()
        device = torch.device('cuda')
        print(f"Using {n_gpus} GPU(s)")
    else:
        device = torch.device('cpu')
        print("No GPU found, using CPU")
    return device

def print_timestamps(count=10):
    """Print timestamps at regular intervals."""
    for i in range(count):
        timestamp = dt.now().strftime("%H:%M:%S")
        print(f"Timestamp {i+1}/{count}: [{timestamp}]")
        time.sleep(2)

if __name__ == "__main__":

    device = get_device_info()
    print_timestamps()
