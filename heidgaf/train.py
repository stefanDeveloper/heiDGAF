import torch
from torch.utils.data import DataLoader

import logging
from heidgaf.dataset.majestic import MajesticMillionDataset
# from heidgaf.metrics

def train():
    # setting device on GPU if available, else CPU
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    logging.info(f'Using device: {device}')
    if torch.cuda.is_available():
        logging.info("GPU detected")
        logging.info(f"\t{torch.cuda.get_device_name(0)}")
        
    if device.type == 'cuda':
        logging.info("Memory Usage:")
        logging.info(f"\tAllocated: {round(torch.cuda.memory_allocated(0)/1024**3,1)} GB")
        logging.info(f"\tCached:    {round(torch.cuda.memory_reserved(0)/1024**3,1)} GB")
    
    logging.info(f'Loading data sets')
    majestic_dataset = MajesticMillionDataset("./data/majestic_million.csv")
    train_dataloader = DataLoader(majestic_dataset, batch_size=64, shuffle=True)
    test_dataloader = DataLoader(majestic_dataset, batch_size=64, shuffle=True)
    train_features, train_labels = next(iter(train_dataloader))

    