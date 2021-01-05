from bs4 import BeautifulSoup
import requests
import pathlib
import time
import pandas as pd


class ReplayScraper:
    def __init__(self, gentool_data_url, local_dir, sleep_secs=3):
        self.gentool_data_url = gentool_data_url
        self.local_dir = local_dir
        self.sleep_secs = sleep_secs
        self.log = self.create_df_log()
        self.file_count = 0

    @staticmethod
    def create_df_log():
        log = pd.DataFrame(columns=['date',
                                    'user_dir',
                                    'user_dir_clean',
                                    'filename',
                                    'filename_clean',
                                    'zh_ccg',
                                    'online_network',
                                    'remote_url',
                                    'local_path',
                                    'unix_time_downloaded'])
        return log

    @staticmethod
    def __remove_illegal_chars(text):
        chars = '\\*?:"<>|'  # / is allowed because it is obviously reserved for dir divisions
        for c in chars:
            text = text.replace(c, '_')
        return text

    def download_files(self, url, verbose=1):
        url = url.rstrip('/')
        mid_dir = url.split('/')[-6:-1]
        user_dir = url.split('/')[-1]
        user_dir_clean = self.__remove_illegal_chars(user_dir)

        local_subdir = pathlib.Path(self.local_dir, '/'.join(mid_dir), user_dir_clean)
        local_subdir.mkdir(parents=True, exist_ok=True)

        month_date = '_'.join(mid_dir[:2])
        zh_ccg = mid_dir[2]
        online_network = mid_dir[3]
        if verbose in (1, 2):
            print('Start downloading files from url', url)

        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')
        links = soup.find_all('a')[5:]  # Ignore the top links as they are not files

        for i, link in enumerate(links):
            log = {}
            filename = link.get('href')
            full_url = str(url + '/' + filename)
            filename_local = self.__remove_illegal_chars(filename)
            local_full_path = local_subdir.joinpath(filename_local)
            log['local_path'] = local_full_path
            r = requests.get(full_url)
            with open(local_full_path, 'wb') as file:
                file.write(r.content)
            self.log.loc[str(self.file_count)] = [month_date,
                                                  user_dir,
                                                  user_dir_clean,
                                                  filename,
                                                  filename_local,
                                                  zh_ccg,
                                                  online_network,
                                                  full_url,
                                                  local_full_path.as_posix(),
                                                  time.time()]
            self.file_count += 1

            if verbose == 2:
                if (i + 1) % 10 == 0:
                    print('Download in progress... Saved', i + 1, 'files to disk')
        if verbose in (1, 2):
            print('Finished downloading files from url. Saved', len(links), 'files to disk', local_subdir.as_posix())
        return len(links)

    def download_day(self, day_url, zh_ccg='zh', network_online='online'):
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
            while True:
                try:
                    n_files = self.download_files(full_url, verbose=1)
                    break
                except Exception:
                    print('Problems when downloading files. Sleeping for 60 seconds...')
                    time.sleep(60)
            n_files_sum += n_files
            time.sleep(self.sleep_secs)
        print('Finished downloading', n_files_sum, 'files from day dir')

    def log_to_csv(self, filename):
        self.log.to_csv(self.local_dir + '/' + filename)
