#Sunny Kalsi
#this mapper tells the average number of miles driven by each driver ID
from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self, _, line): 
        (ID, weeks, hours, miles) = line.split(',') 

        yield ID, float(miles) 

    def reducer(self, ID, miles):
        total = 0 
        numElements = 0  
        for x in miles: 
            total += x 
            numElements += 1 
       
        yield ID, total / numElements 


if __name__ == '__main__':
    MRFriendsByAge.run()