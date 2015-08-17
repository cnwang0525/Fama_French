import os
import csv
import sys
import datetime
import zipfile
import numpy
import mysql.connector as ffsql
import urllib.request as urllib2
from scipy.stats import t


class MyCUR(object):
    def __init__(self, url, usr, pwd, mydb):
        """Input database address, username, password, and database name in string
        """
        try:
            self.con = ffsql.connect(host=url, user=usr, password=pwd, db=mydb)
            self.cur = self.con.cursor()
            self.stmCnt = 0
            print("Connected to database " + mydb)
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when connecting to database")

    def exe_stm(self, statement):
        """Input SQL statement in string
        """
        try:
            self.cur.execute(statement)
            self.stmCnt += 1
            cmd = statement.upper().partition(" ")[0]
            if cmd == "INSERT" or cmd == "UPDATE":
                self.con.commit()
            elif cmd == "SELECT":
                query = self.cur.fetchall()
                return query
            else:
                pass
            print("Statement executed :" + statement.partition("\n")[0])
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when executing statement :" +
                  statement.partition("\n")[0])

    def __del__(self):
        try:
            self.cur.close()
            self.con.close()
            print("Con & cur successfully closed")
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when closing con & cur")


class MyCSV(object):
    def __init__(self, filename):
        """Input CSV filename in string
        """
        try:
            self.name = filename
            self.data = open(filename, 'r', newline='')
            self.reader = csv.reader(self.data)
            self.rowCnt = 0
            for row in self.reader:
                self.rowCnt += 1
            self.data.seek(0)   # rewind
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when opening CSV file")

            #    def sort(self, sortName):
            #        """Input sorted file path with name
            #        """
            #        try:
            #            sortedFile = sorted(self.reader, key = operator.itemgetter(0), reverse = True)
            #            print('done')
            #            with open(sortName, 'w', newline = '') as f:
            #                fileWriter = csv.writer(f, delimiter = ',')
            #                for row in sortedFile:
            #                    self.writer.writerow(row)
            #        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when sorting the file")

    def __next__(self):
        try:
            return next(self.reader)
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when reading next line")

    def __del__(self):
        try:
            self.data.close()
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when closing file")


class MyCOM(object):
    def __init__(self):
        self.it_cnt = 0
        self.cm_return = None
        self.betas = None
        self.p_values = None

    def gather(self, cm_symbol, first_day, last_day):
        try:
            f_year = str(first_day)[0:4]
            f_mon = str(int(str(first_day)[4:6]) - 1)
            f_day = str(first_day)[6:8]
            l_year = str(last_day)[0:4]
            l_mon = str(int(str(last_day)[4:6]) - 1)
            l_day = str(last_day)[6:8]
            tmp_url = 'http://ichart.yahoo.com/table.csv?s=' + cm_symbol + '&a=' + f_mon + '&b=' + f_day + '&c=' + \
                      f_year + '&d=' + l_mon + '&e=' + l_day + '&f=' + l_year + '&g=d'
            file_path = './Data/Returns/tmp_' + cm_symbol + '.csv'
            down_file(tmp_url, file_path)
            tmp_data = MyCSV(file_path)
            tmp_data.data.seek(0)
            next(tmp_data)
            p_close = list()
            p_open = list()
            for row in tmp_data.reader:
                p_close = numpy.append(p_close, float(row[4]))
                p_open = numpy.append(p_open, float(row[1]))
            self.cm_return = (p_close / p_open) - 1
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when closing file")

    def ols_reg(self, rf, x_iv):
        try:
            self.betas, self.p_values = reg(x_iv, self.cm_return - rf)
            return self.betas, self.p_values
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when conducting OLS regression")


def csv2sql(csv_obj, cur, tab, col, apd=None):
    """Input CSV Object, Cursor Object, Table Name, and Column as list
    """
    csv_obj.data.seek(0)  # rewind
    trc_row = 0
    next(csv_obj)  # skip header
    for tmp0 in csv_obj.reader:
        try:
            tmp1 = ""
            for i in col:
                if i != col[-1]:
                    tmp1 += """'%s',""" % tmp0[i]
                else:
                    tmp1 += """'%s'""" % tmp0[i]
            for obs in apd:
                tmp1 += """,'%s'""" % obs
            stm = """INSERT INTO %s VALUES(%s)""" % (tab, tmp1)
            cur.cur.execute(stm)
            trc_row += 1
        except:
            print("Error: " + str(sys.exc_info()[1]) + " occurs when inserting row " + str(trc_row) + " from file " +
                  str(csv_obj.name))
    cur.con.commit()
    print(str(csv_obj.name) + " inserted")


def down_file(url, filename):
    """Input url of file need to download and the filename in string
    """
    try:
        urllib2.urlretrieve(url, filename)
        print("File " + filename + " downloaded")
    except:
        print("Error: " + str(sys.exc_info()[1]) + " occurs when downloading file")


def ext_file(zip_name, path=None, namelist=None):
    """Input zip-file name with address, extract path, and destination names within a list
    and path has to be in relative address
    """
    try:
        my_zip = zipfile.ZipFile(zip_name)
        my_zip.extractall(path)
        file_list = my_zip.namelist()
        i = 0
        for name in file_list:
            try:
                if os.path.isfile(path + namelist[i]):
                    del_id = input("Same file name already exisits, type 1 for delete the old one : ")
                    if int(del_id) == 1:
                        os.remove(path + namelist[i])
                        os.rename(path + name, path + namelist[i])
                        print("File " + name + " extracted, overwritten the old one")
                    else:
                        pass
                else:
                    os.rename(path + name, path + namelist[i])
                    print("File " + name + " extracted")
            except:
                print('Error' + str(sys.exc_info()[1]) + " occurs when renaming")
    except:
        print("Error: " + str(sys.exc_info()[1]) + " occurs when extracting files")


def fama_fa(filename):
    """Import daily fama_french 3 factors within one year and sort the data by date in descending order
        Returns fama_french factors matrix, risk_free rate matrix, last day and first day
    """
    try:
        tmp = numpy.loadtxt(filename)
        tmp = - tmp
        tmp = - tmp[tmp[:,0].argsort()]
        last_day = tmp[0][0]
        first_day = last_day - 10000
        first_wd = datetime.date(int(str(first_day)[0:4]), int(str(first_day)[4:6]), int(str(first_day)[6:8])).weekday()
        if first_wd > 5:
            print(str(first_day) + " is not a business day and the starting date will be set to the last Friday of it")
            first_day = first_day - first_wd + 4
        else:
            pass
        ind = numpy.where(tmp == first_day)[0][0]
        fama = tmp[:ind + 1, 1:4] / 100
        rf = tmp[:ind + 1, 4] / 100
        return fama, rf, last_day, first_day
    except:
        print("Error " + str(sys.exc_info()[1]) + " occurs when import fama factors")


def reg(x, y):
    """Conduct OLS regression on y = beta * x
        Return betas and p_values from t-test of betas
    """
    try:
        dim_x = x.shape
        constant = numpy.ones(dim_x[0])
        x = numpy.append(constant.T, x.T)
        x = x.reshape(dim_x[1] + 1, dim_x[0]).T
        beta = numpy.dot(numpy.dot(numpy.linalg.inv(numpy.dot(x.T, x)),x.T),y)
        epsilon = y - numpy.dot(x, beta.T)
        var_cov = numpy.dot(numpy.dot(epsilon.T, epsilon),numpy.linalg.inv(numpy.dot(x.T, x))) / \
                  (dim_x[0] - dim_x[1])
        std_err = numpy.diagonal(var_cov) ** 0.5
        t_stat = beta / std_err
        p_values = list()
        for i in t_stat:
            if  t.cdf(i,dim_x[0]-dim_x[1]-1)>0.5:
                p_values.append(2*(1-t.cdf(i,dim_x[0]-dim_x[1]-1)))
            else:
                p_values.append(2*t.cdf(i,dim_x[0]-dim_x[1]-1))
    except:
        dim_x = x.shape
        beta = numpy.zeros(dim_x[1] + 1)
        p_values = numpy.zeros(dim_x[1] + 1)
        print("Error: " + str(sys.exc_info()[1]) + " occurs when conducting OLS regression")
    return beta, p_values
