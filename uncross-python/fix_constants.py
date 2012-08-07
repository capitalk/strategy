from enum import enum


"""Transaction type for execution report FIX tag 20.
  New = status of order changed 
  Cancel = Cancel previous execution report
  Correct = Update value in previous execution report
  Status = No new information 
""" 
EXEC_TRANS_TYPE = enum(
  NEW = '0', 
  CANCEL = '1',
  CORRECT = '2', 
  STATUS = '3'
).map(ord)
  
""" Order status - FIX tag 39"""
ORDER_STATUS = enum(
  NEW  = '0', 
  PARTIAL_FILL = '1', 
  FILL = '2',
  DONE_FOR_DAY = '3',
  CANCELLED  = '4',
  REPLACE  = '5',
  PENDING_CANCEL  = '6',
  STOPPED  = '7',
  REJECTED  = '8',
  SUSPENDED  = '9',
  PENDING_NEW  = 'A',
  CALCULATED  = 'B',
  EXPIRED  = 'C',
  RESTATED  = 'D',
  PENDING_REPLACE  = 'E',
).map(ord)

"""Execution Report Type, fix tag 150. 
  Often, but not always, the same as order status. 
  Read the FIX protocol to learn more about this terrible scheme!
"""
EXEC_TYPE = enum(
  NEW  = '0',
  PARTIAL_FILL  = '1',
  FILL  = '2',
  DONE_FOR_DAY  = '3',
  CANCELLED  = '4',
  REPLACE  = '5',
  PENDING_CANCEL  = '6',
  STOPPED  = '7',
  REJECTED  = '8',
  SUSPENDED  = '9',
  PENDING_NEW  = 'A',
  CALCULATED  = 'B',
  EXPIRED  = 'C',
  RESTATED  = 'D',
  PENDING_REPLACE  = 'E',
).map(ord)

"""Order Types - FIX tag 40"""
ORDER_TYPE = enum(
  MARKET = '1',
  LIMIT = '2',
  STOP = '3',
  STOP_LIMIT = '4',
  MARKET_ON_CLOSE = '5',
  WITH_OR_WITHOUT = '6',
  LIMIT_OR_BETTER = '7',
  LIMIT_WITH_OR_WITHOUT = '8',
  ON_BASIS = '9',
  ON_CLOSE = 'A',
  LIMIT_ON_CLOSE = 'B',
  FOREX_MARKET = 'C',
  PREVIOUSLY_QUOTED = 'D',
  PREVIOUSLY_INDICATED = 'E',
  FOREX_LIMIT = 'F',
  FOREX_SWAP = 'G',
  FOREX_PREVIOUSLY_QUOTED = 'H',
  FUNARI = 'I', # limit day order with executed portion handled as Market On Close e.g. Japan...
  PEGGED = 'P',
).map(ord)


"""Exec instruction - FIX tag 18"""
EXEC_INSTRUCTION = enum(
  STAY_ON_OFFERSIDE  = '0',
  NOT_HELD  = '1',
  WORK  = '2',
  GO_ALONG  = '3',
  OVER_THE_DAY  = '4',
  HELD  = '5',
  PARTICIPATE_DONT_INITIATE  = '6',
  STRICT_SCALE  = '7',
  TRY_TO_SCALE = '8',
  STAY_ON_BIDSIDE  = '9',
  NO_CROSS  = 'A',
  OK_TO_CROSS  = 'B',
  CALL_FIRST  = 'C',
  PERCENT_OF_VOLUME  = 'D',
  DO_NOT_INCREASE  = 'E',
  DO_NOT_REDUCE  = 'F',
  ALL_OR_NONE  = 'G',
  INSTITUTIONS_ONLY  = 'I',
  LAST_PEG  = 'L',
  MIDPRICE_PEG  = 'M', #  midprice of inside quote
  NON_NEGOTIABLE  = 'N',
  OPENING_PEG  = 'O',
  MARKET_PEG  = 'P',
  PRIMARY_PEG  = 'R', # peg to primary market - buy at bid/sell at offer
  SUSPEND  = 'S',
  FIXED_PEG  = 'T', # peg to local best bid or offer at time of order
  CUSTOMER_DISPLAY_INSTRUCTION  = 'U',
  NETTING  = 'V',
  PEG_TO_VWAP  = 'W',
).map(ord)



"""OrdRejReason - order reject reason - FIX tag 103"""
REJECT = enum(
  BROKER_OPTION = 0, 
  UNKNOWN_SYMBOL  = 1,
  EXCHANGE_CLOSED  = 2,
  ORDER_EXCEEDS_LIMIT  = 3,
  TOO_LATE_TO_ENTER  = 4,
  UNKNOWN_ORDER  = 5,
  DUPLICATE_ORDER  = 6,
  DUPLICATE_VERBAL_ORDER  = 7,
  STALE_ORDER  = 8,
)
  
HANDLING_INSTRUCTION = enum(
  AUTOMATED_NO_INTERVENTION = '1',
  AUTOMATED_INTERVENTION_OK = '2',
  MANUAL = '3', 
).map(ord)
  
#  Time in Force - FIX tag 59"
TIME_IN_FORCE = enum(
  DAY = '0',
  GOOD_TIL_CANCEL =  '1',
  AT_THE_OPENING = '2',
  IMMEDIATE_OR_CANCEL = '3',
  FILL_OR_KILL = '4',
  GOOD_TIL_CROSSING = '5',
  GOOD_TIL_DATE = '6',
).map(ord)
