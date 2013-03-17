
import string
import traceback
import sys


def sysTrace():
    if sys.exc_info()[0] != None:
        exc_type = str(sys.exc_info()[0]).split('.', 1)[1]
        return "%s%s: %s\n"%(' '.join(traceback.format_tb(sys.exc_info()[2])),
                             exc_type, sys.exc_info()[1] )
    else:
        return "None"

def traceBack():
   """Returns a string containing the 
      current exception being handled. An 
      empty string is returned if 
      no exception is available.""" 

   if sys.exc_info()[0] != None:
      exc_type = string.split( str(sys.exc_info()[0]), '.', 1)[1]
      #exc_type = string.join( string.split( str(sys.exc_info()[0]), '.' )[1:] )
      return "%s%s: %s\n"%( string.join( traceback.format_tb( sys.exc_info()[2] ) ), exc_type, sys.exc_info()[1] )
      #return "%s%s: %s\n"%( list_to_string( traceback.format_tb( sys.exc_info()[2] ) ) , exc_type, sys.exc_info()[1] ) 
      #return "%s%s: %s\n"%( stackTrace()  , exc_type, sys.exc_info()[1] ) 
   else: 
      return "(no exceptions)"


def stackTrace():
   """Returns a string containing a trace
      of the current execution stack. """

   return list_to_string( traceback.format_list( traceback.extract_stack()[:-1] ) )



def getSourceFileName( steps=0 ):
   """Convenience function for obtaining
      the source file name of the instruction on
      the selected stack step. """

   steps = steps + 3 
   return get_stack_tuple( steps )[0] 
      

def getLineNumber( steps=0 ):
   """Convenience method for obtaining 
      line number of the instruction on the
      selected stack step.""" 

   steps = steps + 3 
   return get_stack_tuple( steps )[1]


def getInstruction( steps=0 ):
   """Convenience method for obtaining 
      line number of the instruction on the
      selected stack step.""" 

   steps = steps + 3 
   return get_stack_tuple( steps )[3]


def getFunctionName( steps=0 ):
   """Convenience function for obtaining 
      the function name of the instruction on the
      selected stack step. """

   steps = steps + 3 
   func_name = get_stack_tuple( steps )[2]

   if func_name == '?':
      func_name = '__main__'

   return func_name



def get_stack_tuple( steps ):
   """ Internal function for obtaining an 
       adjusted stack trace. """
 
   stack_tuple = traceback.extract_stack()
   if steps > len( stack_tuple ) or steps < 0:
      steps = 0 

   steps = -1 * steps
   return stack_tuple[steps] 



def list_to_string( l ):
   """Internal function for converting
      a list of string values into a single string. """

   result = ''
   if l != []:
     for line in l:
        result = string.join( [result, line] )

   return result


##########################
# Test suite begins here #
##########################


def test_function_1():

   print "\nGetting line number of test_finction_1() with getLineNumber()..."
   print "getLineNumber() -> %s"%( getLineNumber() )

   print "\nGetting current function name of test_function_1() with getFunctionName()..."
   print "getFunctionName() -> %s"%( getFunctionName() )

   print "\nGetting current source file of test_function_name_1() name with getSourceFileName()..."
   print "getSourceFileName() -> %s"%( getSourceFileName() )

   print "\nGetting line number of source that invoked test_finction_1() with getLineNumber()..."
   print "getLineNumber(1) -> %s"%( getLineNumber(1) )

   print "\nGetting instruction that invoked test_finction_1() with getInstruction()..."
   print "getInstruction(1) -> %s"%( getInstruction(1) )

   print "\nInvoking test_function_2()..."
   test_function_2()

def test_function_2():

   print "\nGetting line number of test_finction_2() with getLineNumber()..."
   print "getLineNumber() -> %s"%( getLineNumber() )

   print "\nGetting current function name of test_function_2() with getFunctionName()..."
   print "getFunctionName() -> %s"%( getFunctionName() )

   print "\nGetting current source file of test_function_name_2() name with getSourceFileName()..."
   print "getSourceFileName() -> %s"%( getSourceFileName() )

   print "\nGetting line number of source that invoked test_finction_2() with getLineNumber()..."
   print "getLineNumber(1) -> %s"%( getLineNumber(1) )

   print "\nGetting function name that invoked test_function_2() with getFunctionName()..."
   print "getFunctionName(1) -> %s"%( getFunctionName(1) )
 
   print "\nGetting function name 2 levels deep from curren location with getFunctionName()..."
   print "getFunctionName(2) -> %s"%( getFunctionName(2) )


if __name__ == '__main__':

   print "\n***** Starting wplTrace Test *****"

   print "\nGetting stack trace with stackTrace()..."
   st = stackTrace()
   print "stackTrace() ->\n%s"%( st )


   print "\nGetting line number with getLineNumber()..."
   print "getLineNumber() -> %s"%( getLineNumber() )

   print "\nGetting current function with getFunctionName()..."
   print "getFunctionName() -> %s"%( getFunctionName() )

   print "\nGetting current source file name with getSourceFileName()..."
   print "getSourceFileName() -> %s"%( getSourceFileName() )


   print "\nInvoking test_function_1()..."
   test_function_1()

   print "\nGetting trace back BEFORE exception call with traceBack()..."
   tb = traceBack()
   print "traceBack() ->\n%s"%( tb )   


   print "\nRaising TypeError exception..."
   try:
      raise TypeError( "an error" )
   except:
      print "\nGetting trace back AFTER exception is raised with traceBack()..."
      tb = traceBack()
      print "traceBack() ->\n%s"%( tb ) 


   

   print "\n***** End of wplTrace tests *****\n"
