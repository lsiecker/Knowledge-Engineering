# When this file is executed, it will clean the data in the data folder using the other files in the cleaner folder.

import os
import threading

# Run the data_wrapper_test.py file 
os.system('python src/cleaner/data_wrapper_test.py')


# Run the following files in parrallel using multithreading

# Define the files to run
files = ['data_extension_person.py', 'data_extension_budget.py']

# Define a function to run the files
def run_file(file):
    os.system('python src/cleaner/' + file)

# Create a list to store the threads
threads = []

# Iterate over the files and run them in parallel
for file in files:
    thread = threading.Thread(target=run_file, args=[file])
    thread.start()
    threads.append(thread)

# Wait for the threads to finish
for thread in threads:
    thread.join()
