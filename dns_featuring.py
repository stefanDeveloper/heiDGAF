import tldextract
import re
import numpy as np
from scipy.stats import entropy

def encode_domain(df):
    """
    Encode domain to string
    """
    domain = str(df["Domain"].decode('latin-1').encode("utf-8"))
    df["Domain"] = domain
    return df

def fqdn_entropy(df):
    if not df["Domain"]:
        return df
    
    domain = df["Domain"]
    
    pk = [domain.count(chr(x)) / len(domain) for x in range(256)]
    pk = np.array(pk)

    df["Entropy"] = entropy(pk, base=2)

    
def count(df):
    """
    fqdn_count, upper_count, lower_count, numeric_count, special_count
    """
    if not df["Domain"]:
        return df

    domain = df["Domain"]

    df["FQDN_full_count"] = len(domain)
    df["FQDN_upper_count"] = sum(1 for c in domain if c.isupper())
    df["FQDN_lower_count"] = sum(1 for c in domain if c.islower())
    df["FQDN_numeric_count"] = sum(1 for c in domain if c.isdigit())
    df["FQDN_special_count"] = len(df["Domain"]) - len(re.findall("[\w]", domain))

    return df


def subdomain(df):
    """
    subdomain_length, sld, subdomain
    """
    if not df["Domain"]:
        return df

    domain = df["Domain"]

    parsec_domain = tldextract.extract(domain)

    df["Subdomain_length"] = len(parsec_domain.subdomain)

    return df


def labels(df):
    """
    labels, labels_max, labels_average, longest_word
    """
    if not df["Domain"]:
        return df

    labels = df["Domain"].split(".")
    df["Labels_length"] = len(labels)
    df["Labels_max"] = len(max(labels, key=len))
    df["Labels_average"] = sum(len(c) for c in labels)

    return df
