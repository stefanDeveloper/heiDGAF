import datetime
import ipaddress
import random


def random_ipv4():
    return ipaddress.IPv4Address._string_from_ip_int(random.randint(0, MAX_IPV4))


def random_ipv6():
    return ipaddress.IPv6Address._string_from_ip_int(random.randint(0, MAX_IPV6))


# DNS_DOMAINS = pl.read_csv("./data/majestic_million.csv")["Domain"]
STATUSES = ["NOERROR", "NXDOMAIN"]
MAX_IPV4 = ipaddress.IPv4Address._ALL_ONES  # 2 ** 32 - 1
MAX_IPV6 = ipaddress.IPv6Address._ALL_ONES  # 2 ** 128 - 1
IP = [random_ipv4, random_ipv6]
RECORD_TYPES = ["AAAA", "A", "AAAA", "A", "AAAA", "A", "AAAA", "A", "PR", "CNAME"]


def generate_dns_log_line(domain: str):
    timestamp = (
        datetime.datetime.now() + datetime.timedelta(0, 0, random.randint(0, 900))
    ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    status = random.choice(STATUSES)
    src_ip = f"192.168.{random.randint(0, 3)}.{random.randint(1, 255)}"
    server_ip = f"10.10.0.{random.randint(1, 10)}"
    record_type = random.choice(RECORD_TYPES)
    response = IP[random.randint(0, 1)]()
    size = f"{random.randint(50, 255)}b"

    return f"{timestamp} {status} {src_ip} {server_ip} {domain} {record_type} {response} {size}"


if __name__ == "__main__":
    dns_log_lines = [generate_dns_log_line() for _ in range(10000)]
