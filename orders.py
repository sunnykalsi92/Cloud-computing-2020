#Sunny Kalsi
#This map reduce program uses two seperate mappers to first sum up the amount total by itemID
#then sorts the amounts and prints it out
from mrjob.job import MRJob
from mrjob.step import MRStep
#need mrsteps to use multisteps
class MRCustomerOrders(MRJob):

    MRJob.SORT_VALUES = True
    #define the steps the program needs to take, in this case we use two different map reducers to solver problem

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                   reducer=self.reducer_totals_by_item),
            MRStep(mapper=self.mapper_make_amounts_key,
                   reducer=self.reducer_output_results)
        ]
    #extract only itemID and order total from data
    def mapper_get_orders(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield itemID, float(orderAmount)
    #sum up the order totals based on itemID
    def reducer_totals_by_item(self, itemID, orders):
        yield itemID, sum(orders)
    #keys are the values of the previous mapper
    #fix formating
    def mapper_make_amounts_key(self, itemID, orderTotal):
        yield None, ("%07.02f"%float(orderTotal), itemID)
    #output results
    def reducer_output_results(self, n, orderTotalitemIDs):
        for c in orderTotalitemIDs:
            yield c[1], c[0]

if __name__ == '__main__':
    MRCustomerOrders.run()