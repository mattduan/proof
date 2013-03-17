"""
A PROOF code integration utility script.

Integration from a generated code base to production code base:

1. backup production file in _backup/datetime_xxx directory.
2. replace the customized blocks in production code to generated code.
3. svn move all generated files into production directory and svndelete 
   all redundant files.

"""

import string
import re
import os, os.path
import sys
import getopt
import datetime


# regex patterns for customized contents
re_imports  = "(?si)^.*(\n[#]+ START OF CUSTOMIZED IMPORTS [#]+.*[#]+ END OF CUSTOMIZED IMPORTS [#]+\n).*$"
sub_imports = "(?si)(\n[#]+ START OF CUSTOMIZED IMPORTS [#]+.*[#]+ END OF CUSTOMIZED IMPORTS [#]+\n)"
re_code     = "(?si)^.*(    [#]+ START OF CUSTOMIZED CODE [#]+.*[#]+ END OF CUSTOMIZED CODE [#]+\n).*$" 
sub_code    = "(?si)(    [#]+ START OF CUSTOMIZED CODE [#]+.*[#]+ END OF CUSTOMIZED CODE [#]+\n)" 

# excluded files or dirs in production code base
EXCLUDED_LIST = ['.', '.svn', '__init__.py', '_gen', '_backup']

class CodeIntegrator:

    def __init__(self, pro_base, gen_base):
        """ Constructor.

            @param pro_base <string> The production base directory.
            @param gen_base <string> The generated base directory.
        """
        assert os.path.isdir(pro_base)
        assert os.path.isdir(gen_base)
        self.pro_base = pro_base
        self.gen_base = gen_base

    def process(self):
        self.backup()
        self.integrate()
        self.migrate()
    
    def backup(self):
        cmd = "cp %s/*.py %s/" % (self.pro_base,
                               self.__backup_path())
        if os.system(cmd) != 0:
            raise Exception("Error: can't backup production code base!")

    def integrate(self):
        """ Copy customized code to new generated code base.
        """
        filenames = os.listdir(self.gen_base)
        for filename in filenames:
            if filename[-3:] == '.py':
                pro_file = os.path.join(self.pro_base, filename)
                if os.path.isfile(pro_file):
                    gen_file = os.path.join(self.gen_base, filename)
                    self.__move_customized_code(pro_file, gen_file)

    def migrate(self):
        """ Use svn command to override the existing production code from
            generated code and delete redundancies.
        """
        old_pro_filenames = os.listdir(self.pro_base)
        gen_filenames = os.listdir(self.gen_base)
        for gen_filename in gen_filenames:
            gen_file = os.path.join(self.gen_base, gen_filename)
            os.system("cp %s %s/"%(gen_file, self.pro_base))
            if gen_filename in old_pro_filenames:
                old_pro_filenames.remove(gen_filename)
            else:
                pro_file = os.path.join(self.pro_base, gen_filename)
                if pro_file[-3:] == '.py':
                    os.system("svn add %s"%(pro_file))

        for pro_filename in old_pro_filenames:
            if pro_filename in EXCLUDED_LIST:
                continue
            pro_file = os.path.join(self.pro_base, pro_filename)
            if pro_file[-3:] == '.py':
                os.system("svn del %s"%(pro_file))

    def __backup_path(self):
        # make sure the basic path exists
        backup_dir = os.path.join( self.pro_base,
                                   '_backup' )
        if not os.access(backup_dir, os.F_OK):
            os.makedirs(backup_dir)

        date = datetime.date.today().strftime('%Y%m%d')

        i = 0
        full_path = None
        while not full_path or os.access(full_path, os.F_OK):
            i += 1
            full_path = os.path.join( backup_dir,
                                      "%s_%s" % (date, string.zfill(`i`, 3)) )
        os.mkdir(full_path)
        
        return full_path

    def __move_customized_code(self, pro_file, gen_file):
        pro_code = open(pro_file).read()
        gen_code = open(gen_file).read()
        m = re.match(re_imports, pro_code)
        if m:
            cust_imports = m.group(1)
            print cust_imports
            gen_code = re.sub(sub_imports, cust_imports, gen_code)
        
        m = re.match(re_code, pro_code)
        if m:
            cust_code    = m.group(1)
            print cust_code
            gen_code = re.sub(sub_code, cust_code, gen_code)
        
        open(gen_file, 'w').write(gen_code)


def usage(msg=''):

    print """USAGE:
%s [-h] -b dir -s dir

Generate PROOF Code for database schemas configured in the given 
configuration file.

Options:
    -h, --help   -- print this message
    -b, --base   -- the base path of production code needed to be
                    integrated.
    -s, --src    -- the base path of newly generated source code.

"""%( sys.argv[0] )
    if msg:
        print >> sys.stderr, msg

    sys.exit(1)
    
if __name__ == '__main__':

    # options:
    # for simplity, we use config file for now
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hb:s:', 
                                   ['help', 'base=', 'src='])
    except getopt.error, msg:
        usage()

    pro_base = None
    gen_base = None
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()

        elif opt in ('-b', '--base'):
            pro_base = arg

        elif opt in ('-s', '--src'):
            gen_base = arg

    if not pro_base:
        usage('Please specify the base path of production code.')

    if not gen_base:
        usage('Please specify the base path of newly generated code.')


    ci = CodeIntegrator(pro_base, gen_base)
    ci.process()

