from mpi4py import MPI
from mpi_master_slave import Master, Slave
import socket
from bs4 import BeautifulSoup
from dns_client import DNS_CLIENT
import os
import urllib.robotparser
from urllib.parse import urlparse, urljoin
from timeout import timeout


class Utils(object):
    def __init__(self):
        self.working_folder = 'scraping-content'
        # create working folder
        if not os.path.exists(self.working_folder):
            os.mkdir(self.working_folder)

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

        self.url_queue = ['http://riweb.tibeica.com/crawl/'] #  'http://riweb.tibeica.com/crawl/'
        self.visited = {}

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
                if current_url in self.visited:
                    continue

                print('Slave {0} is going to process url {1}'.format(slave, current_url))

                # check url in robots
                rp = urllib.robotparser.RobotFileParser()
                try:
                    url = urlparse(current_url)
                    rp.set_url(url.scheme + '://' + url.netloc + '/robots.txt')
                    self.read_robots(rp)
                    if not rp.can_fetch(self.UTILS.USER_AGENT, current_url):
                        continue
                except:
                    continue

                # set to visited current url
                self.visited[url.scheme + '://' + url.netloc + url.path] = True
                self.master.run(slave, data=current_url)
                self.limit -= 1
                print('Limit: {}'.format(self.limit))

            #
            # reclaim slaves that have finished working
            # so that we can assign them more work
            #
            for slave in self.master.get_completed_slaves():
                done, code, file_path, url = self.master.get_data(slave)
                if done:
                    if code == 200:
                        self.get_links(file_path, url)
                    else:
                        new_location = file_path
                        if code == 301:
                            try:
                                self.visited.pop(url.geturl(), None)
                            except:
                                pass
                        if new_location not in self.url_queue and new_location not in self.visited:
                            self.url_queue.insert(0, new_location)
                else:
                    print('Failed to process the url: {0} --- Response code: {1}'
                          .format(url.geturl(), code))

                if self.limit <= 0:
                    self.terminate_slaves()

    def get_links(self, html_file, url):
        with open(html_file, 'r') as file:
            soup = BeautifulSoup(file, features="html.parser")

        meta_robots = soup.find("meta", attrs={"name": "robots"})
        # if robots meta exists, check if the following is allowed
        if meta_robots:
            meta_robots_content = meta_robots['content']
            if 'nofollow' in meta_robots_content:
                return

        for a in soup.find_all('a', href=True):
            href = a['href'].strip()
            if href == '' or href[0] == '#':
                continue

            if '#' in href:
                href = ''.join(href.split('#')[:-1])

            if 'https' in href or 'http' in href and href not in self.visited:
                self.url_queue.append(href)
            else:
                new_url = urljoin(url.geturl(), href)
                if new_url not in self.visited and new_url not in self.url_queue:
                    self.url_queue.append(new_url)

    @timeout(0.7)
    def read_robots(self, rp):
        rp.read()


class MySlave(Slave):
    """
    A slave process extends Slave class, overrides the 'do_work' method
    and calls 'Slave.run'. The Master will do the rest
    """

    def __init__(self):
        super(MySlave, self).__init__()
        self.UTILS = Utils()

    def do_work(self, data):
        current_url = data
        url = urlparse(current_url)
        domain = url.netloc
        local_path = url.path

        data = self.http_client(domain, local_path)

        file_path = self.create_url_directory_structure(domain, local_path)

        code = self.write_data_into_file(file_path, data, url)

        if code == 200:
            return True, code, file_path, url
        elif code >= 300 and code < 400:
            return True, code, self.check_move_permanently(data), url
        else:
            return False, code, None, url

    def http_client(self, domain, local_path):
        # check local dns for
        TCP_IP = DNS_CLIENT.check_cache(domain)
        # if dns cache expired or does not exist
        if not TCP_IP:
            TCP_IP = DNS_CLIENT.get_ip(domain)
        TCP_PORT = 80
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((TCP_IP, TCP_PORT))

        if len(local_path) > 1:
            local_path = local_path[1:]

        MESSAGE = 'GET /{0} HTTP/1.1\nHost: {1}\nConnection: close\nUser-Agent: {2}\n\n'.format(local_path, domain, self.UTILS.USER_AGENT)
        s.send(MESSAGE.encode())

        data = ''
        while True:
            try:
                buf = s.recv(1)
                data += buf.decode()
                if not buf:
                    break
            except:
                pass

        s.close()
        return data

    def check_move_permanently(self, data):
        res = data.split('\r\n')
        header = res[:-1]
        if '301' in header[0]:
            for line in header:
                if 'Location:' in line:
                    return ':'.join(line.split(':')[1:])

    def create_url_directory_structure(self, domain, local_path):
        cwd = self.UTILS.working_folder

        url = domain + local_path

        fullname = os.path.join(cwd, url)
        path, basename = os.path.split(fullname)
        if not os.path.exists(path):
            os.makedirs(path)

        if basename == '':
            return path + '/' + path.split('/')[-1] + '.html'
        else:
            return path + '/' + basename

    def write_data_into_file(self, file_path, data, url):
        res = data.split('\r\n')

        header = res[:-1]
        html_content = res[-1]

        code = int(header[0].split(' ')[1])

        if code != 200:
            return code
        # 'w+'
        # Opens a file for writing only in binary format. Overwrites the file if the file exists.
        # If the file does not exist, creates a new file for writing.
        with open(file_path, 'w+') as file:
            file.write(html_content)

        return code


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

    print('Task completed (rank {})'.format(rank))


if __name__ == "__main__":
    main()
