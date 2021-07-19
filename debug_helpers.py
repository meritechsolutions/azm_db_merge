'''
Copyright: Copyright (C) 2016 Freewill FX Co., Ltd. All rights reserved.
'''


debug = 0

def set_debug(d):
    global debug
    debug = d

def dprint(*s): # https://stackoverflow.com/questions/919680/can-a-variable-number-of-arguments-be-passed-to-a-function
    global debug
    if debug != 0:
        print(("dprint: "+str(s)))

