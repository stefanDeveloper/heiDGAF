import re
import argparse
import sys
import copy
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

optional_packages = True
try:
    # optional, for upload progess updates
    from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
    from tqdm import tqdm
except ImportError:
    optional_packages = False


def extract_var(script_text, variable_name, default=None):
    if variable_name in script_text:
        # match: var_name: "value" or var_name: 'value' or var_name = "value" or var_name = 'value'
        pattern = re.compile(
            r'{}\s*[:=]\s*(["\'])(.*?)\1'.format(re.escape(variable_name))
        )
        match = pattern.search(script_text)
        if match:
            return match.group(2)
    return default


def extract_info_from_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    scripts = soup.find_all("script")
    token = parent_dir = repo_id = dir_name = None
    for script in scripts:
        token = extract_var(script.text, "token", token)
        parent_dir = extract_var(script.text, "path", parent_dir)
        repo_id = extract_var(script.text, "repoID", repo_id)
        dir_name = extract_var(script.text, "dirName", dir_name)
    return token, parent_dir, repo_id, dir_name


def get_html_content(url):
    response = requests.get(url)
    return response.text


def get_upload_url(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json().get("upload_link")
    return None


def get_upload_url2(api_url):
    headers = {"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json().get("url")
    return None


def upload_file(upload_url, file_path, fields):
    fields = copy.deepcopy(fields)
    path = Path(file_path)
    filename = path.name
    total_size = path.stat().st_size

    if not optional_packages:
        with open(file_path, "rb") as f:
            fields["file"] = (filename, f)
            response = requests.post(
                upload_url, files=fields, params={"ret-json": "true"}
            )
        return response

    # ref: https://stackoverflow.com/a/67726532/11854304
    with tqdm(
        desc=filename,
        total=total_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        with open(file_path, "rb") as f:
            fields["file"] = (filename, f)
            encoder = MultipartEncoder(fields=fields)
            monitor = MultipartEncoderMonitor(
                encoder, lambda monitor: bar.update(monitor.bytes_read - bar.n)
            )
            headers = {"Content-Type": monitor.content_type}
            response = requests.post(
                upload_url, headers=headers, data=monitor, params={"ret-json": "true"}
            )
    return response


def upload_seafile(upload_page_link, file_path_list, replace_file, verbose):
    parsed_results = urlparse(upload_page_link)
    base_url = f"{parsed_results.scheme}://{parsed_results.netloc}"
    if verbose:
        print(f"Input:")
        print(f" * Upload page url: {upload_page_link}")
        print(f" * Files to be uploaded: {file_path_list}")
        print(f" * Replace existing files: {replace_file}")
        print(f"Preparation:")
        print(f" * Base url: {base_url}")

    # get html content
    html_content = get_html_content(upload_page_link)

    # extract variables from html content
    token, parent_dir, repo_id, dir_name = extract_info_from_html(html_content)
    if not parent_dir:
        print(f"Cannot extract parent_dir from HTML content.", file=sys.stderr)
        return 1
    if verbose:
        print(f" * dir_name: {dir_name}")
        print(f" * parent_dir: {parent_dir}")

    # get upload url
    upload_url = None
    if token:
        # ref: https://github.com/haiwen/seafile-js/blob/master/src/seafile-api.js#L1164
        api_url = f"{base_url}/api/v2.1/upload-links/{token}/upload/"
        upload_url = get_upload_url(api_url)
    elif repo_id:
        # ref: https://stackoverflow.com/a/38743242/11854304
        api_url = (
            upload_page_link.replace("/u/d/", "/ajax/u/d/").rstrip("/")
            + f"/upload/?r={repo_id}"
        )
        upload_url = get_upload_url2(api_url)
    if not upload_url:
        print(f"Cannot get upload_url.", file=sys.stderr)
        return 1
    if verbose:
        print(f" * upload_url: {upload_url}")

    # prepare payload fields
    fields = {"parent_dir": parent_dir}
    # overwrite file if already present in the upload directory.
    # contributor: hmassias
    # ref: https://gist.github.com/hmassias/358895ef0b2ffaa9e708181b16b554cf
    if replace_file:
        fields["replace"] = "1"

    # upload each file
    print(f"Upload:")
    for idx, file_path in enumerate(file_path_list):
        print(f"({idx+1}) {file_path}")
        try:
            response = upload_file(upload_url, file_path, fields)
            if response.status_code == 200:
                print(f"({idx+1}) upload completed: {response.json()}")
            else:
                print(
                    f"({idx+1}) {file_path} ERROR: {response.status_code} {response.text}",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"({idx+1}) {file_path} EXCEPTION: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--link", required=True, help="upload page link (generated by seafile)"
    )
    parser.add_argument(
        "-f", "--file", required=True, nargs="+", help="file(s) to upload"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="show detailed output"
    )
    parser.add_argument("--replace", action="store_true", help="Replace existing files")
    args = parser.parse_args()
    sys.exit(upload_seafile(args.link, args.file, args.replace, args.verbose))
