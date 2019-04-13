from mpi4py import MPI
from mpi_master_slave import Master, Slave
from mpi_master_slave import WorkQueue
import time
import os
from text_processor import Utils
from pymongo import MongoClient


client = MongoClient()
db = client['riw_db']


class MyApp(object):
    """
    This is my application that has a lot of work to do so it gives work to do
    to its slaves until all the work is done
    """

    def __init__(self, slaves):
        # when creating the Master we tell it what slaves it can handle
        self.master = Master(slaves)
        # WorkQueue is a convenient class that run slaves on a tasks queue
        self.work_queue = WorkQueue(self.master)

    def terminate_slaves(self):
        """
        Call this to make all slaves exit their run loop
        """
        self.master.terminate_slaves()

    def run(self, root_dir):
        """
        This is the core of my application, keep starting slaves
        as long as there is work to do
        # """

        #
        # let's prepare our work queue. This can be built at initialization time
        # but it can also be added later as more work become available
        #
        for dirName, subdirList, fileList in os.walk(root_dir):
            for fname in fileList:
                self.work_queue.add_work(data=os.path.join(dirName, fname))

        #
        # Keeep starting slaves as long as there is work to do
        #
        while not self.work_queue.done():
            #
            # give more work to do to each idle slave (if any)
            #
            self.work_queue.do_work()

            #
            # reclaim returned data from completed slaves
            #
            for slave_return_data in self.work_queue.get_completed_work():
                done, message = slave_return_data
                if not done:
                    print(message)


class MySlave(Slave):
    """
    A slave process extends Slave class, overrides the 'do_work' method
    and calls 'Slave.run'. The Master will do the rest
    """
    def __init__(self):
        super(MySlave, self).__init__()

    def do_work(self, data):
        file_name = data

        words = {}
        with open(file_name, "r", errors='ignore') as file:
            word = []
            letter = file.read(1)

            while letter:
                if letter not in Utils.separators:
                    word.append(letter)
                else:
                    word = "".join(word)  # transforms list of characters into a string
                    word = Utils.PorterStemmer(word)  # stemming the word
                    if len(word):
                        # add the current word into the dictionary
                        if word not in Utils.exception and word not in Utils.stop_words:
                            if word in words:
                                words[word] += 1
                            else:
                                words[word] = 1
                        elif word in Utils.stop_words and word in Utils.exception:
                            if word in words:
                                words[word] += 1
                            else:
                                words[word] = 1

                    # prepare the list for creating a new word
                    word = []
                letter = file.read(1)

        # write to collection
        direct_index_coll = db["direct_index"]
        coll_array = []
        for key, value in words.items():
            coll_array.append({"term": key, "freq": value})
        direct_index_coll.insert_one({"file": file_name, "terms": coll_array})

        # db.foo.update({'title.de': {$exists : false}}, {$set: {'title.de': ''}})
        return True, 'I completed reading  %s file' % file_name


def create_invers_index():
    direct_index_coll = db["direct_index"]
    invers_index_coll = db["invers_index"]


    invers_index = {}
    for document in direct_index_coll.find():
        for item in document["terms"]:
            if item["term"] not in invers_index:
                invers_index[item["term"]] = []
                invers_index[item["term"]].append({"file": document["file"], "freq": item["freq"]})
            else:
                invers_index[item["term"]].append({"file": document["file"], "freq": item["freq"]})

    for key, value in invers_index.items():
        invers_index_coll.insert_one({"term": key, "docs": value})


def main():
    rank = MPI.COMM_WORLD.Get_rank()
    size = MPI.COMM_WORLD.Get_size()

    if rank == 0:  # Master
        # creates a 'master' with size-1 'slaves'
        app = MyApp(slaves=range(1, size))

        start_time = time.time()
        app.run(root_dir="input_files")
        mapping_time = time.time() - start_time
        print("Direct index time execution: {} seconds\n".format(mapping_time))

        app.terminate_slaves()

        start_time = time.time()
        create_invers_index()
        mapping_time = time.time() - start_time
        print("Invers index time execution: {} seconds\n".format(mapping_time))
    else:  # Any slave
        MySlave().run()


if __name__ == "__main__":
    main()
