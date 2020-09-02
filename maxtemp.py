from mrjob.job import MRJob
#Sunny Kalsi
#this mapreduce program takes the input from a file that contains locations and the max and 
#min temperatures in celsius, converts the temp to fahrenheit and returns the max temp and location

class MRMaxTemp(MRJob):
#take the temperature in celsius and convert it to fahrenheit and return it
    def MakeFahrenheit(self, tenthsOfCelsius): 
        celsius = float(tenthsOfCelsius) 
        fahrenheit = celsius * 1.8 + 32.0 
        return fahrenheit 
#tell the program what the columns of data are, and that we only want the max temp.
    def mapper(self, _, line):
        (location, date, type, data, x, y, z, w) = line.split(',') 
        if(type == 'TMAX'):
            temperature = self.MakeFahrenheit(data)
            yield location, temperature
    #pass the locations and temps(in fahrenheit) to the reducer and it'll give the max temp and the location.
    def reducer(self, location, temps):
        yield location, max(temps)
if __name__ == '__main__':
    MRMaxTemp.run()
    