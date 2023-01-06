import json
import os
import time
import requests
from multiprocessing import Process,Semaphore
import logging
log=None

class LogUtils:
    @classmethod
    def build_log(cls):
        global  log
        if not log:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s - %(thread)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            logger.addHandler(ch)
            log=logger
        return  log

class DownloadUtils:

    log = LogUtils.build_log()
    @classmethod
    def parse_heads_by_browser(cls,head_str):

        if head_str is None:
            head_str = '''
        Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
        Accept-Encoding: gzip, deflate, br
        Accept-Language: zh-CN,zh;q=0.9
        Cache-Control: no-cache
        Connection: keep-alive
        Host: nist-oar-cache.s3.amazonaws.com
        Pragma: no-cache
        Referer: https://data.nist.gov/
        User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36
            '''
        try:
            heads = {}
            hs = head_str.split("\n")
            hs = [h.strip() for h in hs if h.strip() != '']
            for h in hs:
                try:
                    parts = h.split(": ")
                    heads[parts[0].strip()] = parts[1].strip()
                except Exception as ee:
                    pass
            return heads
        except Exception as e:
            return {}

    @classmethod
    def download_file_or_get_bytes(cls,url, save_file=None, head=None, cookie=None, isJson=False, error_limit=5,
                                   isChunk=False, semaphore_lock=None):
        if semaphore_lock:
            semaphore_lock.acquire()
        try:
            time.sleep(0.2)
            log.info(f"Donloading-{url}-{save_file}")
            begin_time = time.time()
            res = None
            error = 0
            while error < error_limit and not res:
                try:
                    res = requests.get(url, timeout=60 * 100, headers=head, cookies=cookie, stream=isChunk)
                    if res.status_code == 401:
                        log.error(f"Donloading 401-{url}-{save_file}")
                        raise Exception(f"Donloading 401-{url}-{save_file}")
                    if res.status_code == 403:
                        time.sleep(10*1)
                        url=url.replace('/gen1/','/gen0/')
                        log.error(f"Donloading 403-{url}-{save_file}")
                        raise Exception(f"Donloading 403-{url}-{save_file}")
                    elif res.status_code == 200:
                        if isJson:
                            return json.loads(res.text)

                        if save_file:
                            if isChunk:
                                with open(save_file, 'bw') as f:
                                    block_size = 1024 * 10
                                    total_size = int(res.headers['content-length'])
                                    total_size_gb = round(total_size / 1024 / 1024 / 1024, 2)
                                    total_chunk = total_size / block_size
                                    downloaded_size = 0
                                    for chunkIdx, chunk in enumerate(res.iter_content(chunk_size=block_size)):
                                        if chunk:
                                            f.write(chunk)
                                            downloaded_size += block_size
                                            if (downloaded_size / 1024 / 1024) % 3 == 0:
                                                f.flush()
                                                log.info(
                                                    f"Donloading-({save_file})-Size={total_size_gb}G- Rate=({round((chunkIdx + 1) / total_chunk * 100, 2)}%) "
                                                    f"time={time.time() - begin_time}")
                                    f.flush()
                                    log.info(f"Donloading file-{save_file} time={time.time() - begin_time}")
                                    return save_file
                            else:
                                byte_list = res.content
                                with open(save_file, 'bw') as f:
                                    f.write(byte_list)
                                    f.flush()
                                log.info(f"Donloading file-{save_file} time={time.time() - begin_time}")
                                return byte_list
                    else:
                        log.error(f"Donloading-{res.status_code}-{url}-{save_file}")
                        raise Exception(f"Donloading-{res.status_code}-{url}-{save_file}")
                    res = None
                except Exception as e:
                    res = None
                    if error % int(error_limit / 3) == 0:
                        print(e)
                finally:
                    error += 1
        except Exception as ee:
            print(ee)
        finally:
            if semaphore_lock:
                try:
                    semaphore_lock.release()
                except Exception as se:
                    pass

class CdipDataSet:

    log = LogUtils.build_log()
    @classmethod
    def get_download_urls(cls,file_path):
        lines = []
        img_base_url = ''
        text_base_url = ''
        with open(file_path, 'r') as f:
            lines = [l.strip() for l in f.readlines() if l.strip() != '']
            img_base_url = lines[0].split('cdip-images')[0]
            text_base_url = lines[1].split('cdip-text')[0]
            lines = lines[2:]
            lines = [l.split('\t') for l in lines]

        cur_dir = ''
        file_downloads = []
        file_class_idx = -1
        for l in lines:
            if len(l) < 2:
                cur_dir = l[0]
                file_downloads.append([])
                file_class_idx += 1
            else:
                filename = l[0]
                file_downloads[file_class_idx].append(
                    (img_base_url if file_class_idx == 0 else text_base_url) + cur_dir + '/' + filename)
        return file_downloads

    @classmethod
    def download_dataset(cls,base_dir='',download_url_parse_file='',mut_download_num=3):
        file_downloads = cls.get_download_urls(os.path.join(base_dir, download_url_parse_file))
        process_list = []
        semaphore_lock = Semaphore(mut_download_num)
        for files in file_downloads:
            for file_url in files:
                save_dir = os.path.join(base_dir, os.path.dirname(
                    file_url.split('https://nist-oar-cache.s3.amazonaws.com/prd/gen1/mds2-2531/')[1]))
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)
                save_file_path = os.path.join(save_dir, file_url.split('/')[-1])
                p = Process(target=DownloadUtils.download_file_or_get_bytes, args=(
                file_url, save_file_path, DownloadUtils.parse_heads_by_browser(None), None, False, 50, True, semaphore_lock))
                process_list.append(p)
                if len(process_list)>=mut_download_num:
                    for p in process_list:
                        try:
                            p.start()
                        except Exception as pe:
                            print(pe)

                    for p in process_list:
                        try:
                            p.join()
                        except Exception as pe:
                            print(pe)
                    process_list=[]


if __name__ == '__main__':

    base_dir=os.path.abspath(r'./') #work directory
    download_url_parse_file='links.txt' # wait for the download link
    mut_download_num=3  #worker num

    CdipDataSet.download_dataset(base_dir=base_dir,download_url_parse_file=download_url_parse_file,mut_download_num=mut_download_num)


