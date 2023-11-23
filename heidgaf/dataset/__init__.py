from torch.utils.data.dataset import Dataset
import torch
import string
import pandas as pd


class DomainDataset(Dataset):
    def __init__(self, csv_path, train=True):
        """
        Args:
            csv_path (string): path to csv file
            train (string): flag train or test mode i.e. labeled or not
        """
        self.data_df = pd.read_csv(csv_path, header=None)
        self.all_chars = self.__build__chars__()
        self.inputs = self.data_df.iloc[:, 0]
        self.train = train
        self.data_len = len(self.data_df.index)
        if train:
            self.labels = self.data_df.iloc[:, 1]

    def __build__chars__(self):
        """Build dictionary of chars."""
        all_letters = string.ascii_letters + string.digits + " .'-"
        return {all_letters[i]: i+1 for i in range(0, len(all_letters))}

    def char_to_ix(self, char):
        """Character to index lookup."""
        return self.all_chars[char]

    def ix_to_char(self, char):
        """Index to character lookup."""
        for i, val in self.all_chars.items():
            if val == char:
                return i

    def domain_to_ix(self, domain):
        """Domain to sequence of indexes."""
        return torch.LongTensor([self.char_to_ix(i) for i in domain])

    def __getitem__(self, index):
        domain = self.domain_to_ix(self.inputs[index])
        if self.train:
            target = torch.Tensor([self.labels[index]])
            return domain, target
        return domain

    def __len__(self):
        return self.data_len