from bs4 import BeautifulSoup
import requests
import pathlib
import time

REMOTE_URL = 'http://www.gentool.net/data/'
DATA_DIR = 'data/gentool'


def download_files(url, data_dir, verbose=1):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    links = soup.find_all('a')[5:]  # Ignore the top links as they are not files
    local_subdir = url[28:]  # removing "http://www.gentool.net/data/" from the url
    local_subdir = pathlib.Path(data_dir, local_subdir)
    local_subdir.mkdir(parents=True, exist_ok=True)
    # print(local_subdir)
    if verbose in (1, 2):
        print('Start downloading files from url', url)
    for i, link in enumerate(links):
        filename = link.get('href')
        full_url = str(url + '/' + filename)
        local_full_path = local_subdir.joinpath(filename)
        # print(local_full_path)
        r = requests.get(full_url)
        with open(local_full_path, 'wb') as file:
            file.write(r.content)
        if verbose == 2:
            if (i + 1) % 10 == 0:
                print('Download in progress... Saved', i + 1, 'files to disk')
    if verbose in (1, 2):
        print('Finished downloading files from url. Saved', len(links), 'files to disk', local_subdir.as_posix())
    return len(links)


def download_day(day_url, data_dir, zh_ccg='zh', network_online='online', sleep_secs=3):
    url = day_url + '/' + zh_ccg + '/' + network_online
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    links = soup.find_all('a')[5:]  # Ignore the top links as they are not files
    print('Start downloading all files from day dir', url)
    n_files_sum = 0
    for i, link in enumerate(links):
        print(i, 'of', len(links) - 1)
        dir_name = link.get('href')
        full_url = str(url + '/' + dir_name)
        n_files = download_files(full_url, data_dir, verbose=1)
        n_files_sum += n_files
        time.sleep(sleep_secs)
    print('Finished downloading', n_files_sum, 'files from day dir')
    return n_files_sum


# download_files('http://www.gentool.net/data/2020_12_December/19_Saturday/zh/network/Wad_Panda_JFORDJMEJFRA/',
#                DATA_DIR)

n_files_total = download_day('http://www.gentool.net/data/2020_12_December/01_Tuesday', DATA_DIR,
                             zh_ccg='zh', network_online='online', sleep_secs=3)
