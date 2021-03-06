import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os
import urllib.robotparser
from pymongo import MongoClient
from urllib.parse import urlparse, urljoin

client = MongoClient()
db = client['riw_db']
URL_COLL = db['urls']
URL_COLL.delete_many({})

working_folder = 'scraping-content'
# create working folder
if not os.path.exists(working_folder):
    os.mkdir(working_folder)
working_folder += '/'

USER_AGENT = 'RIWEB_CRAWLER'

url_queue = ['http://fanacmilan.com/', 'riweb.tibeica.com/crawl', 'http://riweb.tibeica.com/crawl/inst-makeinstall.html',
             'riweb.tibeica.com/crawl/inst-prerequisites.html', 'https://www.w3schools.com/php/php_syntax.asp',
             'www.w3schools.com']

limit = 100


def create_url_directory_structure(domain, local_path):
    # check if exists a directory for current domain
    if not os.path.exists(working_folder + domain):
        os.makedirs(working_folder + domain)

    # get directory structure for local url removing empty elements
    dir_struct = list(filter(None, local_path.split('/')))

    # check is last element from local url is '/file.html' or just 'file'
    if len(dir_struct) > 0 and len(dir_struct[-1].split('.')) > 1:
        is_end_file = True
        len_to_end_file = len(dir_struct) - 1
    else:
        is_end_file = False
        len_to_end_file = len(dir_struct)

    for i in range(len_to_end_file):
        current_path = working_folder + domain + '/' + '/'.join(dir_struct[:(i + 1)])
        if not os.path.exists(current_path):
            os.mkdir(current_path)

    # in case we access just domain like: 'www.w3school.com'
    if len(dir_struct) == 0:
        current_path = working_folder + domain

    if is_end_file:
        # 'index.asp' ---> 'index.html'
        file_path = current_path + '/' + '.'.join(dir_struct[-1].split('.')[:-1]) + '.html'
    elif len(dir_struct) > 0:
        file_path = current_path + '/' + dir_struct[-1] + '.html'
    else:
        file_path = current_path + '/' + 'index.html'

    return file_path


def write_data_into_file(file_path, data, domain, local_path):
    res = data.split('\r\n')
    header = res[:-1]
    html_content = res[-1]

    if '200' not in header[0]:
        return
    # 'w+'
    # Opens a file for writing only in binary format. Overwrites the file if the file exists.
    # If the file does not exist, creates a new file for writing.
    with open(file_path, 'w+') as file:
        file.write(html_content)

    # extract links
    get_links(file_path, domain, local_path)


def check_move_permanently(data):
    res = data.split('\r\n')
    header = res[:-1]
    if '301' in header[0]:
        for line in header:
            if 'Location:' in line:
                return ':'.join(line.split(':')[1:])


def get_links(html_file, domain, local_path):
    with open(html_file, 'r') as file:
        soup = BeautifulSoup(file, features="html.parser")

    meta_robots = soup.find("meta", attrs={"name": "robots"})
    # if robots meta exists, check if the following is allowed
    if meta_robots:
        meta_robots_content = meta_robots['content']
        if 'nofollow' in meta_robots_content.split(' '):
            return

    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'https' in href or 'http' in href and href not in url_queue:
            url_queue.append(href)
        else:
            if href == '':
                continue
            if href[0] == '/':
                new_url = 'http://' + domain + href
            else:
                new_url = 'http://' + domain + '/' + local_path + '/' + href
            if new_url not in url_queue:
                url_queue.append(new_url)


def http_client(domain, local_path):
    # check local dns for
    TCP_IP = DNS_CLIENT.check_cache(domain)
    # if dns cache expired or does not exist
    if not TCP_IP:
        TCP_IP = DNS_CLIENT.get_ip(domain)
    TCP_PORT = 80
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT))

    MESSAGE = 'GET /{0} HTTP/1.0\nHost: {1}\nUser-Agent: {2}\n\n'.format(local_path, domain, USER_AGENT)
    s.send(MESSAGE.encode())

    data = b''
    while True:
        try:
            buf = s.recv(1)
            data += buf
            if not buf:
                break
        except:
            pass

    s.close()
    return data.decode()


while limit > 0 and len(url_queue) > 0:
    # pop the url from queue
    current_url = url_queue.pop(0)

    if URL_COLL.find_one({'url': current_url}):
        print("URL-ul is already visited: {}".format(current_url))
        continue
    print('current url: {}'.format(current_url))
    current_url = urlparse(current_url)

    # get domain and local_path
    # if '//' in current_url:
    #     domain = current_url.split('//')[1].split('/')[0]
    #     local_path = '/'.join(current_url.split('//')[1].split('/')[1:])
    # else:
    #     domain = current_url.split('/')[0]
    #     local_path = '/'.join(current_url.split('/')[1:])


    # remove '/' from local_path
    # if local_path != '' and local_path[-1] == '/':
    #     local_path = local_path[:-1]
    # if local_path != '' and local_path[0] == '/':
    #     local_path = local_path[1:]

    # check url in robots
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(urljoin(current_url.scheme + current_url.netloc, '/robots.txt'))
        rp.read()
        if not rp.can_fetch(USER_AGENT, current_url):
            continue
    except:
        continue

    data = http_client(current_url)

    # check for 301 Moved Permanently
    new_location = check_move_permanently(data)
    if new_location and new_location != current_url:
        url_queue.insert(0, new_location)
        continue

    file_path = create_url_directory_structure(domain, local_path)
    write_data_into_file(file_path, data, domain, local_path)

    URL_COLL.insert_one({'url': current_url})

    limit -= 1
    print('limit: {}'.format(limit))
