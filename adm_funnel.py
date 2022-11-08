from pathlib import Path
import csv
import sys
import os
import datetime
import pandas as pd
import subprocess

# Future Improvements: 
# 1) convert any csv files into UTF-8

## Parameters
############################################################################################################################
CSV_File = sys.argv[1]           # Banner_BOB_STUDENTS.csv
Types_File = sys.argv[2]         # Banner_BOB_STUDENTS-Data_Types.csv
DIR_TOP_LEVEL = sys.argv[3]      # Top Level directory (F:\DataWarehouse)
CSV_Schema = sys.argv[4]         # CSV Schema Name
CSV_Log_Table_Name = sys.argv[5] # CSV Log table name on the database
SQL_TS_ind  = sys.argv[6]        # Timestamp indicator (True/False)

try:
    SQL_TS_Numb = sys.argv[7]    # Timestamp Record (Optional)
except:
    SQL_TS_Numb = None

try:
    SQL_TS_Desc = sys.argv[8]    # Timestamp Record Description (optional)
except:
    SQL_TS_Desc = None

## Variables
############################################################################################################################
SQL_Table    = CSV_File[CSV_File.find('_')+1:CSV_File.find('.csv')]
SQL_Schema   = CSV_File[:CSV_File.find('_')]
SQL_records = 5                                                             # Number of insert records to send per statement.
SQL_database = "GSONDBTEST"                                                 # Name of the database
DIR_ARCHIVE = DIR_TOP_LEVEL + "\\Transform_" + SQL_Schema + "_" + SQL_Table # (F:\Datawarehouse\BOB_STUDENTS)
SQL_load_file = DIR_ARCHIVE + "\\LOAD_" + SQL_Schema + "_" + SQL_Table + ".SQL" # Load file that is created to run on the database repository server.
log_file = DIR_ARCHIVE + "\\Transform_Data_" + SQL_Schema + "_" + SQL_Table + ".log" # Python log file
time = datetime.datetime.now().strftime("%m-%d-%Y %H:%M")                  # Current Time
SQL_CSV_Load_Log_File = DIR_ARCHIVE + "\\LOAD_" + CSV_Schema + "_" + CSV_Log_Table_Name + "_" + SQL_Schema + "_" + SQL_Table + ".SQL"
SQL_CSV_Load_Types_File = CSV_Schema + "_" + CSV_Log_Table_Name + '_Data_Types.csv'

# HISTORICAL PURPOSES ONLY Register CSV Dialects
#######################
#csv.register_dialect('Banner', delimiter='|', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)

# Writes a new line to a file.
def write_line(p_file, p_line):
    p_file.write(p_line + '\n')

# Create the archive directory if it doesn't exist.
Path(DIR_ARCHIVE).mkdir(parents=True, exist_ok=True)

# Data Repository Log File
f_log = open(log_file,"w+")

write_line(f_log, 'Date & Time: %s\n' % time)
write_line(f_log,'log_file:                 %s' % log_file)

# Log the python parameters
write_line(f_log,'Python Parameters:')
write_line(f_log,'---------------------------------------------')
write_line(f_log,'CSV_File:            %s' % CSV_File)
write_line(f_log,'Types_File:          %s' % Types_File)
write_line(f_log,'DIR_TOP_LEVEL:       %s' % DIR_TOP_LEVEL)
write_line(f_log,'SQL_TS_ind:          %s' % SQL_TS_ind)
write_line(f_log,'SQL_TS_Numb:         %s' % SQL_TS_Numb)
write_line(f_log,'SQL_TS_Desc:         %s' % SQL_TS_Desc)
write_line(f_log,'CSV_Schema:          %s' % CSV_Schema)
write_line(f_log,'CSV_Log_Table_Name:  %s' % CSV_Log_Table_Name)

# Log the python variables
write_line(f_log,'Python Variables:')
write_line(f_log,'-------------------------------------------------------')
write_line(f_log,'SQL_records:              %s' % SQL_records)
write_line(f_log,'SQL_database:             %s' % SQL_database)
write_line(f_log,'SQL_Schema:               %s' % SQL_Schema)
write_line(f_log,'SQL_Table:                %s' % SQL_Table)
write_line(f_log,'SQL_TS_ind:               %s' % SQL_TS_ind)
write_line(f_log,'SQL_TS_Numb:              %s' % SQL_TS_Numb)
write_line(f_log,'CSV_File:                 %s' % CSV_File)
write_line(f_log,'DIR_ARCHIVE:              %s' % DIR_ARCHIVE)
write_line(f_log,'DIR_TOP_LEVEL:            %s' % DIR_TOP_LEVEL)
write_line(f_log,'SQL_load_file:            %s' % SQL_load_file)
write_line(f_log,'SQL_CSV_Load_Log_File:    %s' % SQL_CSV_Load_Log_File)
write_line(f_log,'SQL_CSV_Load_Types_File:  %s' % SQL_CSV_Load_Types_File)

# Data Repository Load Script
write_line(f_log,'Creating the SQL Load file: %s' % SQL_load_file)
f = open(SQL_load_file,"w")
write_line(f_log,'Created the SQL Load file.\n')

### Grab the file Headers
write_line(f_log,'Opening the source file: %s and parsing the headers.' % CSV_File)
delimiter = None
dialect = None

# Determine dialect and delimiter from csv file
with open(CSV_File, 'r') as delimiterFile:
    dialect = csv.Sniffer().sniff(delimiterFile.read(1024))
    delimiter = dialect.delimiter
write_line(f_log,"Delimiter: %s" %(delimiter))

# Grab headers
with open(CSV_File, 'r') as headerFile:
    headers_csv = headerFile.readline().strip().split(delimiter)
write_line(f_log,'Finished parsing the headers.')

# Remove the Double Quotes from the file headers
headers_csv = [h.strip('\"') for h in headers_csv]
write_line(f_log,'Removed the double quotes from the file headers and saved it as a list.')

# Insert TS_ID column into headers array
if (SQL_TS_ind == 'False'):
    headers_sql = list(headers_csv)
else:
    headers_sql = list(headers_csv)
    headers_sql.insert(0,'TS_ID')
write_line(f_log, 'List of headers %s\n' % headers_sql)

# SQL insert header statement 
SQL_InsertHeader = 'INSERT INTO [%s].[%s].[%s] \r\n(' % (SQL_database, SQL_Schema, SQL_Table)
SQL_InsertHeader += ','.join('[' + h + ']' for h in headers_sql)
SQL_InsertHeader += ') \r\n VALUES \r\n'

# Checks the type formatting and converts the data into the propper formats.
def typeFormatting(j, p_row, p_header):
    data_type = df.at[j,'Column Type']
    
    # Prints for troubleshooting
    #print("TypeFormatting:")
    #print("data_type: %s" % data_type)
    #print("j: %s" % j)
    #print("Value: %s" % p_row[p_header])
    
    # Null Type
    if p_row[p_header] is None or p_row[p_header] == '':
        p_row[p_header] = 'NULL'
        
    # Integer
    elif data_type == 'int':
        p_row[p_header] = str(int(p_row[p_header]))
    
    #Date
    elif data_type == 'date':
        p_row[p_header] = "'"+str(p_row[p_header])+"'"
    
    #String
    elif data_type == 'varchar':
        p_row[p_header] = "'"+str(p_row[p_header])+"'"
    
    #Float
    elif data_type == 'decimal':
        None

# Replace characters with escape characters when found
def escapeCharacters(p_row, p_header):
    record = p_row[p_header]
    
    if isinstance(record,str) and "'" in record:
        p_row[p_header] = record.replace("'","''")

# Pandas Processing
####################
write_line(f_log,'Loading the data types into the Pandas Python Module.')
# Read csv file into a dataframe, forcing strings
df = pd.read_csv(DIR_TOP_LEVEL+'\\Existing_Tables\\'+Types_File, dtype=str, sep=',') 

# Processes the csv file using the dialect
#######################################################
write_line(f_log,'Opening csv file with the csv dialect.\nDialect:\n %s' %dialect)
with open(CSV_File) as csvfile:
    i=0
    reader = csv.DictReader(csvfile, dialect=dialect)
    
    # Go through every record in the csv file.
    for row in reader:
        #print('Row: %s' % row)
        if(i==0):
            f.write(SQL_InsertHeader)
        elif (i%SQL_records == 0 ):
            f.write(');\r\n')
            f.write('\r\n')
            f.write(SQL_InsertHeader)
        else:
            f.write('),\r\n')
        f.write('(')
        
        # Write the data values:
        if SQL_TS_ind == 'True':
            j=1
        else:
            j=0
        for h in headers_csv:
            
            # escape any characters needed
            escapeCharacters(row,h)
            
            # Convert to the correct formatting used by MSSQL Database based on the column types
            typeFormatting(j, row, h)
            j+=1
            
        p = ','.join(str(row[h]) for h in headers_csv)
        if(SQL_TS_ind == 'True'):
            f.write(str(SQL_TS_Numb)+ ',' + p)
        else:
            f.write(p)
        i+=1
f.write(');\r\n')
f.write('\r\n')
f.close()

write_line(f_log,'Created SQL Load file: %s.\n' % SQL_load_file)

# SQL CSV Log File
###########################
# Pandas Processing
####################
write_line(f_log,'Loading the CSV Log data types into the Pandas Python Module.')
df = pd.read_csv(DIR_TOP_LEVEL+'\\Existing_Tables\\'+SQL_CSV_Load_Types_File, dtype=str, sep=',')

# SQL insert header statement 
SQL_InsertHeader = 'INSERT INTO [%s].[%s].[%s] \r\n(' % (SQL_database, CSV_Schema, CSV_Log_Table_Name)
SQL_InsertHeader += ','.join('[' + i + ']' for i in df['Column Name'])
SQL_InsertHeader += ') \r\n VALUES (\r\n'

f = open(SQL_CSV_Load_Log_File,"w")
f.write(SQL_InsertHeader)
f.write("'" + time + "','" + str(CSV_File) + "','" + str(SQL_Schema) + "','" + str(SQL_Table) + "','" + str(SQL_TS_Numb) + "','" + str(SQL_TS_Desc) + "'")
f.write(');\r\n')
f.write('\r\n')
f.close()
write_line(f_log,'Created CSV Log SQL Load file: %s.\n' % SQL_CSV_Load_Types_File)