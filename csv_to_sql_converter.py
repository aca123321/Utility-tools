import os
import mysql.connector as conn
import sqlalchemy

def create_sql_table(s,cols,filename,data_types):
	cols_len = len(cols)
	s = ""
	count = 0
	for i in cols:
		s += i + " " + data_types[count]
		if(count != len(cols)-1):
			s += ","
		count += 1
	st = "CREATE TABLE " + filename + " ( " + s + " );"
	return (st)

def feed_sql(content,filename,col_names):
	ret = []
	for i in range(len(content)):
		con = ""
		st = ""
		for j in content[i]:
			con += " \'" + j + "\',"
		con = con[:-1]
		st = "INSERT INTO " + filename + " (" + col_names + ") VALUES (" + con + ");"
		ret.append(st)
	return (ret)

if(__name__ == "__main__"):

	un = str(input("Enter your sql username"))
	pw = str(input("Enter your sql password"))

	db = conn.connect(
	  host="localhost",
	  user=un, ##insert your mysql username and password 
	  passwd=pw
	)

	engine = sqlalchemy.create_engine('mysql+pymysql://' + un + ':' + pw + '@localhost:3306/csv_to_sql')
	cur = db.cursor()
	cur.execute("use csv_to_sql;")	

	filename = str(input("Enter the full csv file name or path without extension(eg.C2ImportUsersSample):\n"))

	'''
	filename = "C2ImportUsersSample"
	'''

	f = open(filename+".csv",'rt')
	s = f.readline()
	col_names = s[:-1]
	cols = col_names.split(",")
	col_names = ""

	data_types = []
	for i in range(len(cols)):

		a = str(input("Enter SQL data type for the column " + cols[i] + ":\n"))

		cols[i] = cols[i].rstrip(" ")
		cols[i] = cols[i].lstrip(" ")
		cols[i] = cols[i].replace(" ", "_")
		cols[i] += "_"
		col_names += cols[i]
		if(i != len(cols)-1):
			col_names += " ,"
		'''
		a = " varchar(20)" XXXXW
		'''
		data_types.append(a)



	content = []
	for s in f:
		t = ""
		i=0
		while(i<len(s)):
			if(s[i] == "\""):
				i += 1
				inquote = ""
				while(s[i] != "\""):
					inquote += s[i]
					i += 1
				inquote = inquote.replace(",", " ")
				t += inquote

			else:
				t += s[i]
			i += 1

		a = t.split(",")
		a[-1] = a[-1].replace("\n","")
		content.append(a)

	st = create_sql_table(s,cols,filename,data_types)
	engine.execute(st)
	ret = feed_sql(content,filename,col_names)

	for i in ret:
		engine.execute(i)
