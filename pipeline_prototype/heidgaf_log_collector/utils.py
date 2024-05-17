import ipaddress


def validate_host(host) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    try:
        host = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError(f"Invalid host: {host}")
    return host


def validate_port(port: int) -> int:
    if not isinstance(port, int):
        raise TypeError

    if not (1 <= port <= 65535):
        raise ValueError(f"Invalid port: {port}")
    return port
