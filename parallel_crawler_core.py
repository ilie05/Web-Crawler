from mpi4py import MPI
from mpi_master_slave import Master, Slave
import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os
import urllib.robotparser
from pymongo import MongoClient


class Utils(object):
    def __init__(self):
        self.working_folder = 'scraping-content'
        # create working folder
        if not os.path.exists(self.working_folder):
            os.mkdir(self.working_folder)
        self.working_folder += '/'
        self.USER_AGENT = 'RIWEB_CRAWLER'
        self.WEB_FILES_FORMAT = ['.html', '.asp', '.jsp', '.php']


class MyApp(object):
    """
    This is my application that has a lot of work to do so it gives work to do
    to its slaves until all the work is done
    """

    def __init__(self, slaves):

        # when creating the Master we tell it what slaves it can handle
        self.master = Master(slaves)

        self.url_queue = ['riweb.tibeica.com/crawl', 'http://fanacmilan.com/']

        client = MongoClient()
        db = client['riw_db']
        self.URL_COLL = db['urls']
        self.URL_COLL.delete_many({})

        self.UTILS = Utils()

        self.limit = 200

    def terminate_slaves(self):
        """
        Call this to make all slaves exit their run loop
        """
        self.master.terminate_slaves()

    def run(self):
        """
        This is the core of my application, keep starting slaves
        as long as there is work to do
        """

        #
        # while we have work to do and not all slaves completed
        #
        while self.url_queue or not self.master.done():

            #
            # give work to do to each idle slave
            #
            for slave in self.master.get_ready_slaves():

                if not self.url_queue:
                    break
                current_url = self.url_queue.pop(0)  # get next url in the queue

                if self.URL_COLL.find_one({'url': current_url}):
                    print("URL-ul is already visited: {}".format(current_url))
                    continue

                print('Slave {0} is going to process url {1}'.format(slave, current_url))

                # get domain and local_path
                if '//' in current_url:
                    domain = current_url.split('//')[1].split('/')[0]
                    local_path = '/'.join(current_url.split('//')[1].split('/')[1:])
                else:
                    domain = current_url.split('/')[0]
                    local_path = '/'.join(current_url.split('/')[1:])

                # remove '/' from local_path
                if local_path != '' and local_path[-1] == '/':
                    local_path = local_path[:-1]
                if local_path != '' and local_path[0] == '/':
                    local_path = local_path[1:]

                # check url in robots
                rp = urllib.robotparser.RobotFileParser()
                try:
                    rp.set_url('http://' + domain + '/robots.txt')
                    rp.read()
                    if not rp.can_fetch(self.UTILS.USER_AGENT, current_url):
                        continue
                except:
                    continue

                self.master.run(slave, data=(domain, local_path, current_url))


            #
            # reclaim slaves that have finished working
            # so that we can assign them more work
            #
            for slave in self.master.get_completed_slaves():
                done, code, new_location = self.master.get_data(slave)
                if done:
                    if code == 301:
                        if new_location not in self.url_queue:
                            self.url_queue.insert(0, new_location)
                    elif code == 200:
                        self.url_queue.extend(new_location)
                else:
                    print('Master: slave {0} failed to accomplish his task'.format(slave))

                self.limit -= 1
                print('Limit: {}'.format(self.limit))
                if self.limit < 0:
                    print('Done with this shit!!!')
                    self.terminate_slaves()
                    exit()


class MySlave(Slave):
    """
    A slave process extends Slave class, overrides the 'do_work' method
    and calls 'Slave.run'. The Master will do the rest
    """

    def __init__(self):
        super(MySlave, self).__init__()
        self.UTILS = Utils()

    def do_work(self, data):
        domain, local_path, current_url = data

        data = self.http_client(domain, local_path)

        # check for 301 Moved Permanently
        new_location = self.check_move_permanently(data)
        if new_location and new_location != current_url:
            return True, 301, new_location

        file_path = self.create_url_directory_structure(domain, local_path)

        links = self.write_data_into_file(file_path, data, domain, local_path)

        if links:
            return True, 200, links
        else:
            return False, 400, []

    def http_client(self, domain, local_path):
        # check local dns for
        TCP_IP = DNS_CLIENT.check_cache(domain)
        # if dns cache expired or does not exist
        if not TCP_IP:
            TCP_IP = DNS_CLIENT.get_ip(domain)
        TCP_PORT = 80
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((TCP_IP, TCP_PORT))

        MESSAGE = 'GET /{0} HTTP/1.0\nHost: {1}\nUser-Agent: {2}\n\n'.format(local_path, domain, self.UTILS.USER_AGENT)
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

    def check_move_permanently(self, data):
        res = data.split('\r\n')
        header = res[:-1]
        if '301' in header[0]:
            for line in header:
                if 'Location:' in line:
                    return ':'.join(line.split(':')[1:])

    def create_url_directory_structure(self, domain, local_path):
        # check if exists a directory for current domain
        if not os.path.exists(self.UTILS.working_folder + domain):
            os.makedirs(self.UTILS.working_folder + domain)

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
            current_path = self.UTILS.working_folder + domain + '/' + '/'.join(dir_struct[:(i + 1)])
            if not os.path.exists(current_path):
                os.mkdir(current_path)

        # in case we access just domain like: 'www.w3school.com'
        if len(dir_struct) == 0:
            current_path = self.UTILS.working_folder + domain

        if is_end_file:
            # 'index.asp' ---> 'index.html'
            file_path = current_path + '/' + '.'.join(dir_struct[-1].split('.')[:-1]) + '.html'
        elif len(dir_struct) > 0:
            file_path = current_path + '/' + dir_struct[-1] + '.html'
        else:
            file_path = current_path + '/' + 'index.html'

        return file_path

    def write_data_into_file(self, file_path, data, domain, local_path):
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
        return self.get_links(file_path, domain, local_path)

    def get_links(self, html_file, domain, local_path):
        with open(html_file, 'r') as file:
            soup = BeautifulSoup(file, features="html.parser")

        meta_robots = soup.find("meta", attrs={"name": "robots"})
        # if robots meta exists, check if the following is allowed
        if meta_robots:
            meta_robots_content = meta_robots['content']
            if 'nofollow' in meta_robots_content.split(' '):
                return

        local_path = local_path.split('/')
        if len(local_path) > 1 and local_path[-1].split('.')[-1] in self.UTILS.WEB_FILES_FORMAT:
            local_path = '/'.join(local_path[:-1])
            print('\n\n new local path {}\n\n'.format(local_path))
        else:
            local_path = '/'.join(local_path)

        url_queue = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href == '' or href[0] == '#':
                continue

            if 'https' in href or 'http' in href and href not in url_queue:
                url_queue.append(href)
            else:
                if href[0] == '/':
                    new_url = 'http://' + domain + href
                else:
                    new_url = 'http://' + domain + '/' + local_path + '/' + href

                if new_url not in url_queue:
                    url_queue.append(new_url)

        return url_queue

def main():
    name = MPI.Get_processor_name()
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    print('I am  %s rank %d (total %d)' % (name, rank, size))

    if rank == 0:  # Master

        app = MyApp(slaves=range(1, size))
        app.run()
        app.terminate_slaves()

    else:  # Any slave

        MySlave().run()

    print('Task completed (rank %d)' % (rank))


if __name__ == "__main__":
    main()
