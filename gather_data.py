import fama
import numpy

famaCur = fama.MyCUR('localhost', 'root', 'root', 'fama')
stm = """SELECT comSymbol FROM com_beta"""
dataSymbol = famaCur.exe_stm(stm)

ind_var, rf_rate, lst_d, fst_d = fama.fama_fa('./Data/fama.txt')

for i in range(len(dataSymbol)):
    ticker = dataSymbol[i][0]
    try:
        tmp_com = fama.MyCOM()
        tmp_com.gather(ticker, fst_d, lst_d)
        tmp_result = tmp_com.ols_reg(rf_rate, ind_var)
        if numpy.count_nonzero(tmp_result) == 0:
            pass
        else:
            f_str = list()
            for row in tmp_result:
                for ele in row:
                    f_str.append(ele)
            f_str.append(ticker)
            print(numpy.count_nonzero(tmp_result))
            stm = """UPDATE com_beta SET alpha = %s, betaMarket = %s, betaSMB = %s, betaHML = %s, p_alpha = %s, p_Market = %s,
        p_SMB = %s, p_HML = %s WHERE comSymbol = '%s'""" % tuple(f_str)
            famaCur.exe_stm(stm)
    except:
        print("Error: " + str(sys.exc_info()[1]) + " occurs when processing: " + ticker)
        pass
