import datetime
import ipaddress
import random


def generate_dns_log_line():
    timestamp = datetime.datetime.now().replace(hour=random.randint(0, 23),
                                                minute=random.randint(0, 59),
                                                second=random.randint(0, 59),
                                                microsecond=random.randint(0, 999999)
                                                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    statuses = ["NOERROR"] * 4 + ["NXDOMAIN"]
    status = random.choice(statuses)
    client_ip = f'192.168.0.{random.randint(1, 255)}'
    domain_suffixes = ["com", "org", "de"]
    domain_names = ['example', 'testsite', 'mydomain', 'google', 'website',
                    'uni-heidelberg', 'hei', 'urz', 'mathematikon', 'bioquant',
                    'klinikum-heidelberg', 'hochschulsport', 'studentenwerk-heidelberg',
                    'heidelberg-research', 'campuslife-hei', 'philfak-uni-hd',
                    'heidelberg-alumni', 'studium-generale-hei', 'hei-bibliothek',
                    'physik-uni-heidelberg', 'chemie-heidelberg', 'historischesmuseum-hd',
                    'heidelberg-sportclub', 'uni-hd-jura', 'medizin-forschung-hei',
                    'heidelberg-studentenleben', 'altstadt-heidelberg', 'biofak-hei',
                    'physiklabor-uni-hd', 'philosophie-heidelberg', 'heidelberger-schloss',
                    'uni-hd-musik', 'mathematik-hei', 'heidelberg-botanik',
                    'altphilologie-hei', 'neurowissenschaft-hd', 'hei-campusradio',
                    'medieninformatik-uni-hd', 'heidelberg-geowissenschaften',
                    'studienberatung-hei', 'heidelberg-sprachenzentrum', 'uni-hd-vwl',
                    'heidelberger-theater', 'physiologie-uni-hd', 'ethnologie-heidelberg',
                    'biotech-hei', 'uni-hd-psychologie', 'heidelberg-stadtbibliothek',
                    'geographie-uni-heidelberg', 'soziologie-hei', 'heidelberg-mensa',
                    'uni-hd-philosophenweg', 'astronomie-heidelberg', 'heidelberger-kunst',
                    'uni-hd-bwl', 'altstadt-campus-hd', 'heidelberg-law', 'uni-hd-theologie',
                    'heidelberg-biologie', 'heimatmuseum-hd', 'uni-heidelberg-chemie',
                    'heidelberg-studentenrat', 'campus-kunst-heidelberg']

    hostname = f"www.{random.choice(domain_names)}.{random.choice(domain_suffixes)}"
    record_type = random.choice(["AAAA", "A"])
    response = str(ipaddress.IPv6Address(random.randint(0, 2 ** 128 - 1)))
    size = f"{random.randint(50, 150)}b"

    # ONLY FOR TESTING # TODO: Remove
    client_ip = '192.168.0.0_24'
    #################################

    return f"{timestamp} {status} {client_ip} 8.8.8.8 {hostname} {record_type} {response} {size}"


if __name__ == "__main__":
    dns_log_lines = [generate_dns_log_line() for _ in range(100000)]

    # Writing to a file
    with open("../sandbox/dns_log.txt", "w") as file:
        for line in dns_log_lines:
            file.write(line + "\n")
