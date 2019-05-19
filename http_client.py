import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os

working_folder = 'scraping-content/'

url_queue = ['riweb.tibeica.com/crawl/inst-prerequisites.html', 'https://www.w3schools.com/php/php_syntax.asp', 'stackabuse.com/python-check-if-a-file-or-directory-exists/', 'www.w3schools.com', 'riweb.tibeica.com/crawl']

limit = 100

while limit > 0 and len(url_queue) > 0:
    # pop the url from queue
    current_url = url_queue[0]
    url_queue.pop(0)

    # get domain and local_path
    if '//' in current_url:
        domain = current_url.split('//')[1].split('/')[0]
        local_path = '/'.join(current_url.split('//')[1].split('/')[1:])
    else:
        domain = current_url.split('/')[0]
        local_path = '/'.join(current_url.split('/')[1:])

    # print('domain: {}'.format(domain))
    # print('local path: {}'.format(local_path))

    # check local dns for
    TCP_IP = DNS_CLIENT.check_cache(domain)
    # if dns cache expired or does not exist
    if not TCP_IP:
        TCP_IP = DNS_CLIENT.get_ip(domain)

    TCP_PORT = 80
    # print('IP Address: {}'.format(TCP_IP))

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((TCP_IP, TCP_PORT))

    MESSAGE = 'GET /{0} HTTP/1.0\nHost: {1}\nUser-Agent: CLIENT RIW\n\n'.format(local_path, domain)
    # print(MESSAGE)
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

    data = data.decode()
    s.close()
    # print(data)

    # check if exists a directory for current domain
    if not os.path.exists(working_folder + domain):
        os.makedirs(working_folder + domain)

    # get directory structure for local url removing empty elements
    dir_struct = list(filter(None, local_path.split('/')))

    # check is last element from local url is '/file.html' or just 'file'
    if len(dir_struct) > 0 and len(dir_struct[-1].split('.')) > 1:
        is_end_file  = True
        len_to_end_file = len(dir_struct) - 1
    else:
        is_end_file = False
        len_to_end_file = len(dir_struct)

    for i in range(len_to_end_file):
        current_path = working_folder + domain + '/' + '/'.join(dir_struct[:(i+1)])
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

    print(file_path)
    # 'w+'
    # Opens a file for writing only in binary format. Overwrites the file if the file exists.
    # If the file does not exist, creates a new file for writing.
    with open(file_path, 'w+') as file:
        pass



    limit -= 1
