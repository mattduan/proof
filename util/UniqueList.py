"""
List with unique entries. UniqueList does not allow null nor duplicates.
"""

__version__= '$Revision: 11 $'[11:-2]
__author__ = "Duan Guoqiang (mattgduan@gmail.com)"


class UniqueList(list):

    def __init__(self, initlist=[]):
        # call super class
        list.__init__(self)
        # add initlist
        if initlist:
            self.extend(initlist)

    def __getslice__(self, i, j):
        # return a UniqueList object
        i = max(i, 0); j = max(j, 0)
        return self.__class__(list.__getslice__(self, i, j))

    def __setslice__(self, i, j, other):
        i = max(i, 0); j = max(j, 0)

        # remove duplicates
        uniques = []
        try:
            for o in other:
                if o not in self:
                    uniques.append(o)
        except TypeError:
            raise TypeError( "UniqueList.__setslice__() argument %s must be iterable" % (other) )

        # call super class
        list.__setslice__(self, i, j, uniques)
     
    def __add(self, l, flag=None):
        """ A convenient method for all add call.
        """
        if type(l) == type([]) or \
               isinstance(l, UniqueList):
            if flag == "r":
                new_list = UniqueList()
                new_list.extend(l)
                new_list.extend(self)
                return new_list
            elif flag == "i":
                self.extend(l)
                return self
            else:
                new_list = UniqueList()
                new_list.extend(self)
                new_list.extend(l)
                return new_list
        else:
            raise TypeError( """can only concatenate list/List/UniqueList (not "%s")""" % \
                             type(l) )

    def __add__(self, l):
        return self.__add(l)

    def __radd__(self, l):
        return self.__add(l, "r")

    def __iadd__(self, l):
        return self.__add(l, "i")

    def __mul__(self, n):
        return self
    __rmul__ = __mul__
    __imul__ = __mul__
 
    def append(self, item):
        """ Append an Item to the list.
    
            @param item the Item to append
        """
        if item != None and item not in self:
            list.append(self, item)

    def insert(self, i, item):
        """ Insert an item to the list.

            @param i the index to insert
            @param item the item to insert
        """
        if item != None and item not in self:
            list.insert(self, i, item)

    def extend(self, l):
        """ Extend another list into this list.
    
            @param l the list to extend
        """
        try:
            for i in l:
                self.append(i)
        except TypeError, msg:
            raise TypeError("UniqueList.extend() argument must be iterable")

    def clear(self):
        """ Remove all items in the list.
        """
        list.__init__(self, [])


# only used for test
if __name__ == '__main__':

    print
    print "UniqueList Test"
    print

    print "testing constructor"
    ul1 = UniqueList()
    print "ul1 (UniqueList()) => %s" % (ul1)
    ul2 = UniqueList('123')
    print "ul2 (UniqueList('123')) => %s" % (ul2)
    ul3 = UniqueList([1,1,2,3])
    print "ul3 (UniqueList([1,1,2,3])) => %s" % (ul3)
    print
    
    print 'testing type'
    print "ul1 type => %s" % (type(ul1))
    print "ul1 is subclass list => %s" % (issubclass(ul1.__class__, list))
    
    print "testing append"
    ul1.append(2)
    print "ul1.append(2) => %s" % (ul1)
    ul1.append(2)
    print "ul1.append(2) => %s" % (ul1)
    ul2.append(2)
    print "ul2.append(2) => %s" % (ul2)
    ul3.append(2)
    print "ul3.append(2) => %s" % (ul3)
    print

    print "testing insert"
    ul1.insert(1, 1)
    print "ul1.insert(1, 1) => %s" % (ul1)
    ul1.insert(1, 1)
    print "ul1.insert(1, 1) => %s" % (ul1)
    ul3.insert(3, 3)
    print "ul3.insert(3, 3) => %s" % (ul3)
    print

    print "testing extend"
    ul1.extend('123')
    print "ul1.extend('123') => %s" % (ul1)
    ul1.extend([1,2,3])
    print "ul1.extend([1,2,3]) => %s" % (ul1)
    print

    print "testing +"
    print "ul1 = %s" % (ul1)
    print "ul2 = %s" % (ul2)
    print "ul3 = %s" % (ul3)
    ul4 = ul1 + ul2 + ul3
    print "ul1 + ul2 + ul3 => %s" % (ul4)
    print "type(ul1 + ul2 + ul3) => %s" % (type(ul4))
    print "ul1 + [2,4,5] => %s" % (ul1 + [2,4,5])
    print "type(ul1 + [2,4,5]) => %s" % (type(ul1 + [2,4,5]))
    print

    print "testing slice"
    print "ul1[2:5] => %s" % (ul1[2:5])
    ul1[2:5] = [1,2,3]
    print "ul1[2:5] = [1,2,3]"
    print "ul1 => %s" % (ul1)
    print "type(ul1) => %s" % (type(ul1))
    print
    
    print "testing mul"
    print "ul1 * 3 => %s" % (ul1*3)
    print
    
    print "testing clear"
    ul1.clear()
    print "ul1.clear() => %s" % (ul1)
    print
    
    print "done."
    print
    
