from tldextract import extract

class Word():
    def __init__(self) -> None:
        pass

class Domain():
    def __init__(self, fqdn: str) -> None:
        self.extraced_fqdn = extract(fqdn)
