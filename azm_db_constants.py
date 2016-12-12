'''
Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.
'''

 
# note: source contents must never have , or | in col contents - must replace before insert on android

#the PARAMs for other programs like sql server bulk insert format file so the '\n' must be '\\n' so it finally shows up as '\n' etc
  
BULK_INSERT_LINE_SEPARATOR_VALUE = "|\n"
BULK_INSERT_LINE_SEPARATOR_PARAM = "|\\n" # like c_bitwise_or, also called pipe character

BULK_INSERT_COL_SEPARATOR_VALUE = "\t"
BULK_INSERT_COL_SEPARATOR_PARAM =  "\\t"
