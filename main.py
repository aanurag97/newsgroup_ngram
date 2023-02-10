#This code is to find Salience Score of n-gram in 20_Newsgroups Dataset
#submitted as part of Assignement-1 of course on Big Data Processing,
#IIT-KGP, Spring 2023 by Anurag Shukla (PGDBA-08)

#Importing Libraries
import os
import re
import sys
from threading import Thread
from datetime import datetime

#Function for unique n-gram
def gen_unique(s):
    #constructing Set for unique
    seen = set()
    for sub_l in s:
        if sub_l[0] not in seen:
            seen.add(sub_l[0])
            yield sub_l

#n-gram function
def ngram_func(file_path,n,countlist,id):
    #Dictionary for counting
    count = dict()
    for file in file_path:
        fp = open(file, encoding='latin1', mode = 'r')
        #Splitting at all non-aplhanumeric characters
        words = re.split('[^A-Za-z0-9]+', fp.read())
        fp.close()
        #Lower-case string
        words = [x.lower() for x in words]
        #Generating n-grams
        ngrams = []
        for i in range(len(words)-n+1):
            ngrams.append(" ".join(words[i:i+n]))
        del words
        #Counting n-gram frequency
        for i in ngrams:
            if i in count:
                count[i] += 1
            else:
                count[i] = 1
    countlist[id] = count

#Inputs w.r.t location, threads, n-gram and k
path = sys.argv[1]
threads = int(sys.argv[2])
n = int(sys.argv[3])
k = int(sys.argv[4])

#Execution Start Time
start = datetime.now()

#Main code
out = []
for category in os.listdir(path):
    #Scanning for classes
    category_path = os.path.join(path, category)
    files = os.listdir(category_path)
    #work distribution to Threads
    work = [[] for _ in range(threads)]
    nof = len(files)
    for i in range(nof):
        work[i%threads].append(os.path.join(category_path,files[i]))
    del files
    #Threads initialization
    threads_out = [[] for _ in range(threads)]
    countlist = [[] for _ in range(threads)]
    for i in range(threads):
        threads_out[i] = Thread(target=ngram_func, args=(work[i],n,countlist,i))
        threads_out[i].start()
    for i in range(threads):
        threads_out[i].join()
    #Merging Threads output
    class_dict = {}
    for i in countlist:
        for key in i:
            if key in class_dict:
                class_dict[key] += i[key]
            else:
                class_dict[key] = i[key]
    del countlist
    #ngram Score calculation
    class_dict = {key: val / nof for key, val in class_dict.items()}
    class_list = list(class_dict.items())
    del class_dict
    #Sorting the n-grams on score
    class_list.sort(reverse=True, key=lambda x: x[1])
    out += class_list[:k]
    del class_list

#Sorting unique among classes
out.sort(reverse=True, key = lambda x: x[1])
unique_out = list(gen_unique(out))
del out

#Print top K n-grams
for i in range(k):
    print('Rank ' + str(i+1) + ' ', unique_out[i])

#Printing start and end time
endt = datetime.now()
start_time = start.strftime("%H:%M:%S")
endt_time = endt.strftime("%H:%M:%S")
print("Start Time =", start_time)
print("End Time =", endt_time)