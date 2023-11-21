import torch
import logging

def train():
    print("test")
    if torch.cuda.is_available():
        logging.info("GPU detected")