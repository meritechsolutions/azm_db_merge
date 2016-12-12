'''
Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.
Author: Kasidit Yusuf <kasidit@azenqos.com>, <ykasidit@gmail.com>
'''


debug = 0

def dprint(s):
    global debug
    if debug != 0:
        print(s)
