###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.

# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in

class DiscoveryAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        PUB_REGISTER=2,
        SUB_REGISTER=3,
        PUB_ISREADY=4,
        SUB_LOOKUP=5


    def __init__(self,logger):
        self.state = self.State.INITIALIZE # state that are we in
        self.pubnum=0
        self.subnum=0
        self.mw_obj = None # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements

    def configure(self,args):
        try:
            self.logger.info ("DiscoveryAppln::configure")
            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE
            # initialize our variables

            # Now, get the configuration object
            self.logger.debug ("DiscoveryAppln::configure - parsing config.ini")
            config = configparser.ConfigParser ()
            config.read (args.config)

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug ("DiscoveryAppln::configure - initialize the middleware object")
            self.mw_obj = DiscoveryMW (self.logger)
            self.mw_obj.configure (args) # pass remainder of the args to the m/w object

            self.logger.info ("DiscoveryAppln::configure - configuration complete")
      
        except Exception as e:
            raise e
        

    def driver (self):
        ''' Driver program '''

        try:
            self.logger.info ("DiscoveryAppln::driver")

            self.dump ()

            self.logger.debug ("DiscoveryAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle (self)

            self.state = self.State.CHECKING

            self.mw_obj.event_loop (timeout=0)  # start the event loop

            self.logger.info ("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e
        
    def invoke_operation (self):
        ''' Invoke operating depending on state  '''

        try:
            self.logger.info ("DiscoveryAppln::invoke_operation")

            if (self.state == self.State.CHECKING):
                # send a register msg to discovery service
                self.logger.debug ("DiscoveryAppln::invoke_operation - waiting for pub and sub ")
                self.mw_obj.register (self.name)

                return None

            elif (self.state == self.State.EVENTLOOP):

                self.logger.debug ("DiscoveryAppln::invoke_operation - look up from discovery about publishers") 
                self.mw_obj.lookup_publisher(self.name, self.topiclist) #send look up request

                return None
            
            elif (self.state == self.State.DATARECEIVE):
                
                self.logger.debug ("DiscoveryAppln::invoke_operation - connect to publisher and reveive data")    
                #recevie the message
                for pubaddr in self.pubinfos.values():
                    received_data = self.mw_obj.receive_data (self.name,pubaddr)
                    strs=received_data.split(':')
                    #print data we received
                    self.print_data(strs[0],strs[1])


                self.logger.debug ("DiscoveryAppln::invoke_operation - date receive completed")
    
                # we are done. And continue to receive publishers
                self.state = self.State.LOOKUP
    
                # go to event loop waiting the reply
                return None

            # elif (self.state == self.State.WAITING):
            #     #we received some publishers to show up
            #     self.state = self.State.LOOKUP
            #     return 0

            else:
                raise ValueError ("Undefined state of the appln object")

        except Exception as e:
            raise e
        
    def dump (self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("THIS IS DISCOVERY :D")
            self.logger.info ("**********************************")

        except Exception as e:
            raise e