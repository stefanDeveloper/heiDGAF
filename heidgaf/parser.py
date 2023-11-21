from tldextract import extract

class FQDN():
    def __init__(self, fqdn: str) -> None:
        self.extraced_fqdn = extract(fqdn)
