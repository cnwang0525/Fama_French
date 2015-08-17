import os
import fama

ffCur = fama.MyCUR('127.0.0.1', 'root', 'root', 'fama')
print("=============================================================================")

# ################################## Clearing  Tables #################################
ff_Stm = list()
ff_Stm.append("""DROP TABLE IF EXISTS fama.com_beta""")
ff_Stm.append("""CREATE TABLE IF NOT EXISTS fama.com_beta (
  comSymbol VARCHAR(10) NOT NULL,
  comName VARCHAR(255),
  comSector VARCHAR(255),
  comIndustry VARCHAR(255),
  comEx VARCHAR(10),
  alpha FLOAT,
  p_alpha FLOAT,
  betaMarket FLOAT,
  p_Market FLOAT,
  betaSMB FLOAT,
  p_SMB FLOAT,
  betaHML FLOAT,
  p_HML FLOAT,
  peRatio FLOAT,
  PRIMARY KEY (comSymbol),
  UNIQUE INDEX comSymbol_UNIQUE (comSymbol ASC))""")

for stm in ff_Stm:
    ffCur.exe_stm(stm)
print("=============================================================================")

# ################################### Inputting Data ##################################
fileNames = os.listdir('./Data/list/')
col = [0, 1, 5, 6]
for name in fileNames:
    if name.endswith(".csv"):
        tmpName = fama.MyCSV('./Data/list/' + name)
        fama.csv2sql(tmpName, ffCur, 'com_beta(comSymbol, comName, comSector, comIndustry, comEx)',
                     col, [name.upper().partition('.')[0]])
    else:
        pass

    
print("=============================================================================")
