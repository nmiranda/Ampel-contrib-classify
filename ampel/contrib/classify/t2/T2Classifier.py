#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/examples/t2/T2ExamplePolyFit.py
# License           : BSD-3-Clause
# Author            : nmiranda <nicolas.miranda@hu-berlin.de>
# Date              : 16.04.2020
# Last Modified Date: 16.04.2020
# Last Modified By  : nmiranda <nicolas.miranda@hu-berlin.de>

from ampel.base.abstract.AbsT2Unit import AbsT2Unit
from ampel.contrib.hu.t2.T2SNCosmo import mag_to_flux

import zerorpc
import sfdmap
import numpy as np
import backoff

_CLIENTS = {}

FILTERS = {1: 'g', 2: 'r', 3: 'i'}

def get_client(address):
    """
    Get a zerorpc client for the given address.
    :param address: a ZeroMQ address
    :returns: a zerorpc.Client instance
    """
    if address in _CLIENTS:
        pass
    elif address.startswith('tcp://'):
        _CLIENTS[address] = zerorpc.Client(address)
    else:
        raise ValueError("{} is not a ZeroMQ address")
    return _CLIENTS[address]

class T2Classifier(AbsT2Unit):
    """
    Source classification
    """

    version = 0.1
    author = "nicolas.miranda@hu-berlin.de"

    def __init__(self, logger, base_config=dict()):
		"""
		:param logger: instance of logging.Logger (std python module 'logging')
		:param base_config: optional dict with keys given by the `resources` property of the class
		"""

		# Set sfdmap environment variable
		# os.env[‘SFD_DIR’] = base_config[‘dust_map_path’]

		# Save the logger as instance variable
		self.logger = logger
        self.base_config = base_config
		self.sfd_map = sfdmap.SFDMap()

		# TODO: check client connection
		self.client = get_client(base_config['classify.default'])

		# retry on with exponential backoff on failed reconnects
        self.run = backoff.on_exception(backoff.expo,
            (zerorpc.exceptions.LostRemote),
            logger=self.logger,
            max_time=300,
        )(self.run)

	def run(self, light_curve, run_config):
		"""
		:param light_curve: instance of ampel.base.LightCurve. See LightCurve docstring for more info.
		:param run_config: dict instance containing run parameters defined in ampel config section:
		    t2_run_config->POLYFIT_[run_config_id]->runConfig
			whereby the run_config_id value is defined in the associated t2 document.
			In the case of POLYFIT, run_config_id would be either 'default' or 'advanced'.
			A given channel (say HU_SN_IA) could use the runConfig 'default' whereas
			another channel (say OKC_SNIIP) could use the runConfig 'advanced'
		:returns: either:
			* A dict instance containing the values to be saved into the DB
				-> IMPORTANT: the dict *must* be BSON serializable, that is:
					import bson
					bson.BSON.encode(<dict instance to be returned>)
				must not throw a InvalidDocument Exception
			* One of these T2RunStates flag member:
				MISSING_INFO:  reserved for a future ampel extension where
							   T2s results could depend on each other
				BAD_CONFIG:	   Typically when run_config is not set properly
				ERROR:		   Generic error
				EXCEPTION:     An exception occured
		"""
        
        # Transform jd to mjd
        mjd = [Time(vals, format='jd').mjd for vals in light_curve.get_values('jd')]

        # Transform mag (magpsf, magzpsci, sigmapsf) to flux (flux, fluxerr)
        magpsf = light_curve.get_values('magpsf')
        mapzpsci = light_curve.get_values('magzpsci')
        sigmapsf = light_curve.get_values('sigmapsf')
        flux, fluxerr = mag_to_flux(magpsf, magerr=sigmapsf, units='zp', zp=mapzpsci)

		ra = np.median(lc.get_values('ra'))
		dec = np.median(lc.get_values('dec'))

		objid = lc.id

		redshift = None

		mwebv = sfd_map.ebv(ra, dec)

        light_curve_info1 = (mjd, flux, fluxerr, passband, ra, dec, objid, redshift, mwebv)
		light_curve_list = [light_curve_info1,]

		return self.client.classify(light_curve_list)