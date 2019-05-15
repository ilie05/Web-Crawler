You must have installed Python 3.6.7.

1. Install all required packages: pip3 install -r requirements.txt

2. Create a fodler 'input_files' and place in it text files and folders.

3. Run script that builds Direct Index and Invers Index and insterts them into mongodb collections: 
    mpiexec -n 4 python3 mpi_indexing.py 

4. Run script that build Vector representation of each document from Direct Index and inserts it into mongodb collection: 
    python3 text_processor.py

5. Search an somthing: python3 search.py

***Reference to mpi_master_slave package: https://github.com/luca-s/mpi-master-slave

