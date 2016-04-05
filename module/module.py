#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.


# This Class is an example of an Scheduler module
# Here for the configuration phase AND running one

try:
    import memcache
except ImportError:
    memcache = None

import cPickle

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['scheduler'],
    'type': 'repcache_retention',
    'external': False,
    }


def get_instance(modconf):
    """
    Called by the plugin manager to get a broker
    """
    logger.debug("Get a memcache retention scheduler module for plugin %s" % modconf.get_name())
    if not memcache:
        raise Exception('Missing module python-memcache. Please install it.')
    instance = Repcache_retention_scheduler(modconf)
    return instance


class Repcache_retention_scheduler(BaseModule):
    def __init__(self, mod_conf):
        BaseModule.__init__(self, mod_conf)
        self.servers = []
        for server in mod_conf.servers.split(','):
            self.servers.append(server.strip())

    def init(self):
        """
        Called by Scheduler to say 'let's prepare yourself guy'
        """
        logger.debug("Initialization of the repcache module")

    def get_memcache_client(self):
        logger.info("Finding available repcache server")
        for server in self.servers:
            mc = memcache.Client([server], debug=0)
            if mc.servers[0].connect():
                logger.info("Found server: {}".format(server))
                return mc
            logger.warning("server {} is unavailable".format(server))
        logger.error("No repcache available")


    def normalize_key(self, key):
        """
        Prepare key to be correct for memcache
        """
        # space are not allowed in memcache key.. so change it by SPACE token
        return key.replace(' ', 'SPACE').encode('utf8', 'ignore')

    def hook_save_retention(self, daemon):
        """
        main function that is called in the retention creation pass
        """
        logger.debug("[RepcacheRetention] asking me to update the retention objects")

        all_data = daemon.get_retention_data()

        hosts = all_data['hosts']
        services = all_data['services']
        mc = self.get_memcache_client()

        # Now the flat file method
        for h_name in hosts:
            try:
                h = hosts[h_name]
                key = self.normalize_key("HOST-%s" % h_name)
                val = cPickle.dumps(h)
                mc.set(key, val)
            except:
                logger.error("[RepcacheRetention] error while saving host %s" % key)

        for (h_name, s_desc) in services:
            try:
                key = self.normalize_key("SERVICE-%s,%s" % (h_name, s_desc))
                s = services[(h_name, s_desc)]
                val = cPickle.dumps(s)
                mc.set(key, val)
            except:
                logger.error("[RepcacheRetention] error while saving service %s" % key)

        mc.disconnect_all()
        logger.info("Retention information updated in Memcache")

    # Should return if it succeed in the retention load or not
    def hook_load_retention(self, daemon):
        logger.debug("[RepcacheRetention] asking me to load the retention objects")
        mc = self.get_memcache_client()

        # We got list of loaded data from retention server
        ret_hosts = {}
        ret_services = {}

        # Now the flat file method
        for h in daemon.hosts:
            key = ""
            try:
                key = self.normalize_key("HOST-%s" % h.host_name)
                val = mc.get(key)
                if val is not None:
                    val = cPickle.loads(val)
                    ret_hosts[h.host_name] = val
            except:
                logger.error("[RepcacheRetention] error while loading host %s" % key)

        for s in daemon.services:
            key = ""
            try:
                key = self.normalize_key("SERVICE-%s,%s" % (s.host.host_name, s.service_description))
                val = mc.get(key)
                if val is not None:
                    val = cPickle.loads(val)
                    ret_services[(s.host.host_name, s.service_description)] = val
            except:
                logger.error("[RepcacheRetention] error while loading service %s" % key)

        mc.disconnect_all()

        all_data = {'hosts': ret_hosts, 'services': ret_services}

        # Ok, now comme load them scheduler :)
        daemon.restore_retention_data(all_data)

        logger.info("[RepcacheRetention] Retention objects loaded successfully.")

        return True
