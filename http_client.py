import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os

working_folder = 'scraping-content/'

url_queue = ['riweb.tibeica.com/crawl']

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
    # if dns cache expired
    if not TCP_IP:
        print("cache expired! ")
        TCP_IP = DNS_CLIENT.get_ip(domain)
    else:
        print("ip from cache")
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


    limit -= 1
    if current_url.split('.')[-1] == 'html':
        file_name = current_url
    else:
        file_name = current_url + '.html'

    file_name = 'file.txt'
    with open(working_folder + '' + file_name, 'w+') as file:
        pass
    break
