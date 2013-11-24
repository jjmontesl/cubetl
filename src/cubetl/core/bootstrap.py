
from cubetl.core.context import Context
from springpython.config import XMLConfig
from springpython.context import ApplicationContext
import cubetl
import getopt
import logging
import os
import sys
from compiler.ast import Raise
import traceback



# Get an instance of a logger
logger = logging.getLogger(__name__)

class Bootstrap:
    
    def configure_logging(self, ctx):
        
        # In absence of file config
        defaul_level = logging.INFO if ctx.debug == False else logging.DEBUG  
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=defaul_level)
        #logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        
        springpython_logger = logging.getLogger("springpython")
        if (not ctx.debug2):
            springpython_logger.setLevel(logging.INFO)
        
        #logging.basicConfig(
        #    level=logging.DEBUG,
        #    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        #    datefmt="%H:%M:%S", stream=sys.stdout)
        
        # With file config:
        #logging.config.fileConfig('logging.conf')

    def usage(self):
        print "cubetl [-d] [-f config.xml] <start-process>"
        print ""
        print "    -f   include configuration file"
        print "    -d   debug mode (can be used up to 2 times for more debug)"
        

    def parse_args(self, ctx):
        
        try:
            opts, arguments = getopt.gnu_getopt(ctx.argv, "f:d", [ ])
        except getopt.GetoptError as err:
            print str(err) 
            self.usage()
            sys.exit(2)
            
        for o,a  in opts:
            if o == "-f":
                ctx.configfiles.append(a)
            elif o == "-d":
                if (ctx.debug):
                    ctx.debug2 = True
                else:
                    ctx.debug = True

        if (len(arguments) < 1):
            print ("A start process can be specified.")
            self.usage()
            sys.exit(2)

        if (len(arguments) != 1):
            print ("Only one start process can be specified.")
            self.usage()
            sys.exit(2)                        
        
        ctx.startprocess = arguments[0]
    
    def init_container(self, ctx):
        
        try:
            configs = [XMLConfig(os.path.dirname(os.path.realpath(__file__)) + "/../cubetl-context.xml")]
            configs.extend ([XMLConfig(config_file) for config_file in ctx.configfiles])

            cubetl.container = ApplicationContext( configs )
            
        except Exception, e:
            logger.error ("Could not load config: %s" % e)
            if (ctx.debug): 
                raise e
            else:
                sys.exit(3)
    
    def start(self, argv):

        # Set up context
        ctx = Context()
        ctx.argv = argv 
        
        # Parse arguments
        self.parse_args(ctx)
        
        # Init logging
        self.configure_logging(ctx)
        logger = logging.getLogger(__name__)
        logger.info ("Starting CubETL")
        logger.debug ("Debug logging level enabled")
        
        # TODO: Character encoding considerations? warnings?
        
        
        # Init container
        self.init_container(ctx)

        # Launch process
        try:
            process = cubetl.container.get_object(ctx.startprocess)
        except KeyError, e:
            logger.error ("Start process '%s' not found in configuration" % ctx.startprocess)
            sys.exit(1)
            

        count = 0
        source = { }
        
        # Launch process and consume messages
        try:
            logger.info ("Initializing components")
            process.signal(ctx, "initialize")
            
            logger.info ("Processing %s" % ctx.startprocess)
            msgs = process.process(ctx, source)
            for m in msgs:
                count = count + 1
            
            logger.debug ("%s messages resulted from the process" % count)
            
            logger.debug ("Finalizing components")
            process.signal(ctx, "finalize")
            
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if (ctx.debug):
                traceback.print_exception(exc_type, exc_value, exc_traceback)

            logger.fatal("Error during process: %s" % (traceback.format_exception_only(exc_type, exc_value)))
            
