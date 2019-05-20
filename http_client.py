import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os
import urllib.robotparser

working_folder = 'scraping-content/'
USER_AGENT = 'RIWEB_CRAWLER'

#create robots folder
if not os.path.exists('robots'):
    os.makedirs('robots')

url_queue = ['riweb.tibeica.com/crawl', 'http://fanacmilan.com/', 'http://riweb.tibeica.com/crawl/inst-makeinstall.html',
             'riweb.tibeica.com/crawl/inst-prerequisites.html', 'https://www.w3schools.com/php/php_syntax.asp',
             'www.w3schools.com']

limit = 100


def create_url_directory_structure(domain):
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


def write_data_into_file(file_path, data):
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


def get_robots(domain, ip):
    msg_robots = 'GET /{0} HTTP/1.0\nHost: {1}\nUser-Agent: {2}\n\n'.format('robots.txt', domain, USER_AGENT)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, 80))
    s.send(msg_robots.encode())
    robots = b''
    while True:
        try:
            buf = s.recv(1)
            robots += buf
            if not buf:
                break
        except:
            pass
    robots = robots.decode().split('\r\n')

    if len(robots) < 1:
        return
    file_path = 'robots/{}.robots.txt'.format(domain)
    with open(file_path, 'w+') as file:
        for line in robots:
            if 'User-agent' in line or 'Disallow' in line:
                file.write(line)
    # delete file if nothing is written
    if os.path.getsize(file_path) == 0:
        os.remove(file_path)


def check_url_in_robots(domain, local_path):
    file_path = 'robots/{}.robots.txt'.format(domain)
    if not os.path.exists(file_path):
        return
    local_path = '/'.join(filter(None, local_path.split('/')))
    with open(file_path, 'r') as file:
        for line in file:
            if line.split(':')[0] == 'User-agent' and line.split(':')[1].strip() == USER_AGENT:
                while True:
                    ln = file.readline()
                    if ln == '':
                        break
                    if ln == '\n':
                        continue
                    if ln.split(':')[0] == 'Disallow' and ln.split(':')[1].strip() == '':      # 'Disallow: '
                        return True
                    if ln.split(':')[0] == 'Disallow' and ln.split(':')[1].strip() == '/':     # 'Disallow: /'
                        return False
                    if ln.split(':')[0] == 'User-agent':
                        line = ln
                        break
            print(line)
            if line.split(':')[0] == 'User-agent' and line.split(':')[1].strip() == '*':
                while True:
                    ln = file.readline()
                    if ln == '':
                        break
                    if ln == '\n':
                        continue
                    # ln = '/'.join(filter(None, ln.split(':')[1].split('/')))
                    ln = ln.split(':')[1].split('/')
                    print(ln)
                    if len(ln.split(':')) > 1 and ln.split(':')[1].strip() == file_path:
                        return False
                    if ln.split(':')[0] == 'User-agent':
                        break
            return True
    return True


def check_move_permanently(data):
    res = data.split('\r\n')
    header = res[:-1]
    print(header)
    if '301' in header[0]:
        for line in header:
            if 'Location:' in line:
                return ':'.join(line.split(':')[1:])



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

    # check local dns for
    TCP_IP = DNS_CLIENT.check_cache(domain)
    # if dns cache expired or does not exist
    if not TCP_IP:
        TCP_IP = DNS_CLIENT.get_ip(domain)
    TCP_PORT = 80


    # if not os.path.isfile('robots/{}.robots.txt'.format(domain)):
    #     # request 'robots.txt'
    #     get_robots(domain, TCP_IP)
    #
    # # check url in robots
    # check_url_in_robots(domain, local_path)

    # check url in robots
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url('http://' + domain + '/robots.txt')
    rp.read()
    if not rp.can_fetch(USER_AGENT, current_url):
        continue



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

    data = data.decode()
    s.close()

    # check for 301 Moved Permanently
    new_location = check_move_permanently(data)
    if new_location and new_location != current_url:
        url_queue.insert(0, new_location)
        continue

    file_path = create_url_directory_structure(domain)
    write_data_into_file(file_path, data)

    limit -= 1
