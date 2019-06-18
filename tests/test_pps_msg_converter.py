#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2014 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c14526.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit testing the pps-msg format conversion tools
"""

from mesan_compositer.pps_msg_conversions import (ctype_procflags2pps,
                                                  ctth_procflags2pps,
                                                  get_bit_from_flags,
                                                  bits2value,
                                                  value2bits)
import unittest
import numpy as np

BITS1 = [0, 1, 0, 1]
BITS2 = [0, 0, 1, 1, 1, 0, 0, 1]
DEC1 = 10
DEC2 = 156
PFLAGS = [10, 156]
CTYPE_MSG_PROCFLAGS = np.array(PFLAGS)


class TestFlagConversions(unittest.TestCase):

    """Unit testing the functions to convert msg flags to pps (old) flags"""

    def setUp(self):
        """Set up"""
        return

    def test_bit_conversions(self):
        """Test converting from binary to decimal and back"""

        res = value2bits(DEC1)
        # Python 2.7:
        # self.assertSequenceEqual(res, BITS1, seq_type=list)
        self.assertEqual(res, BITS1)
        res = value2bits(DEC2)
        # self.assertSequenceEqual(res, BITS2, seq_type=list)
        self.assertEqual(res, BITS2)
        res = bits2value(BITS1)
        self.assertEqual(res, DEC1)
        res = bits2value(BITS2)
        self.assertEqual(res, DEC2)

    def test_ctype_procflags2pps(self):
        """Test convert msg cloudtype quality flags to pps (<v2014) processing
        flags"""

        # msg illumination bit 0,1,2 (undefined,night,twilight,day,sunglint) maps
        # to pps bits 2, 3 and 4:
        msgbits = [1, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 7)

        # Example: Day and no Sunglint - 3-bit value = 3
        msgbits = [1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 3)
        res = ctype_procflags2pps(np.array([3], 'int32'))
        self.assertEqual(res[0], 0)

        # Example: Day and Sunglint - 3-bit value = 4
        msgbits = [0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 4)
        res = ctype_procflags2pps(np.array([4], 'int32'))
        self.assertEqual(res[0], 16)

        # Example: Night - 3-bit value = 1
        res = ctype_procflags2pps(np.array([1], 'int32'))
        self.assertEqual(res[0], 4)

        # Example: Twilight - 3-bit value = 2
        res = ctype_procflags2pps(np.array([2], 'int32'))
        self.assertEqual(res[0], 8)

        # msg nwp-input bit 3 (nwp present?) maps to pps bit 7:
        # msg nwp-input bit 4 (low level inversion?) maps to pps bit 6:

        # Example: All NWP parameters available (no low level inversion) -
        # 2-bit value = 1
        msgbits = [0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 8)
        # PPS bit 7 should be set
        res = ctype_procflags2pps(np.array([8], 'int32'))
        self.assertEqual(res[0], 128)

        # Example: All NWP parameters available (low level inversion) -
        # 2-bit value = 2
        msgbits = [0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 16)
        # PPS bits 6 and 7 should be set
        res = ctype_procflags2pps(np.array([16], 'int32'))
        self.assertEqual(res[0], 192)

        # Example: At least one NWP parameter missing - 2-bit value = 3
        msgbits = [0, 0, 0, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 24)
        # PPS bits 6 and 7 should be set
        res = ctype_procflags2pps(np.array([24], 'int32'))
        self.assertEqual(res[0], 0)

        # msg seviri-input bits 5&6 maps to pps bit 8:

        # Example: All useful SEVIRI channels available - 2-bit value = 1
        msgbits = [0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 32)
        # PPS bit 8 should not be set!
        res = ctype_procflags2pps(np.array([32], 'int32'))
        self.assertEqual(res[0], 0)

        # Example: At least one useful SEVIRI channel missing - 2-bit value = 2
        msgbits = [0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 64)
        # PPS bit 8 should be set
        res = ctype_procflags2pps(np.array([64], 'int32'))
        self.assertEqual(res[0], 256)

        # Example: At least one mandatory SEVIRI channel missing - 2-bit value
        # = 3
        msgbits = [0, 0, 0, 0, 0, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 96)
        # PPS bit 8 should be set
        res = ctype_procflags2pps(np.array([96], 'int32'))
        self.assertEqual(res[0], 256)

        #
        # msg quality bits 7&8 maps to pps bit 9&10:

        # Example: Good quality (high confidence) - 2-bit value = 1
        msgbits = [0, 0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 128)
        # PPS bit 9 should not be set!
        res = ctype_procflags2pps(np.array([128], 'int32'))
        self.assertEqual(res[0], 0)

        # Example: Poor quality (low confidence) - 2-bit value = 2
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 256)
        # PPS bit 9 should be set
        res = ctype_procflags2pps(np.array([256], 'int32'))
        self.assertEqual(res[0], 512)

        # Example: Reclassified after spatial smoothing (very low confidence) -
        # 2-bit value = 3
        msgbits = [0, 0, 0, 0, 0, 0, 0, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 384)
        # PPS bit 9 should be set
        res = ctype_procflags2pps(np.array([384], 'int32'))
        self.assertEqual(res[0], 1024)

        #
        # msg bit 9 (stratiform-cumuliform distinction?) maps to pps bit 11:

        # Example: separation between cumuliform and stratiform clouds
        # performed
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 512)
        # PPS bit 11 should be set
        res = ctype_procflags2pps(np.array([512], 'int32'))
        self.assertEqual(res[0], 2048)

        # Example: Some combinations of the above
        # Twilight,
        # All NWP parameters available (no low level inversion),
        # All useful SEVIRI channels available
        # Good quality (high confidence)
        # Separation between cumuliform and stratiform clouds performed
        msgbits = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 682)
        # PPS bit 11 should be set
        ppsbits = [0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1]
        res = ctype_procflags2pps(np.array([682], 'int32'))
        self.assertEqual(ppsbits, value2bits(res[0]))

    def test_ctth_procflags2pps(self):
        """Test convert msg ctth quality flags (14 bits) to pps (<v2014)
        processing flags"""

        # 2 bits to define processing status (maps to pps bits 0 and 1:)
        # Non-processed?
        # If non-processed in msg (0) then set pps bit 0 and nothing else.
        # If non-processed in msg due to FOV is cloud free (1) then also only
        # set pps bit 0
        # If processed (because cloudy) with result in msg (3) then set
        # pps bit 1.
        # If processed (because cloudy) without result in msg (2) then set
        # pps bit 0 (non-processed) and pps bit 1.

        # Example: non-processed - 2-bit value = 0
        res = ctth_procflags2pps(np.array([0], 'int32'))
        ppsbits = [1, ]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: non-processed because FOV is cloud free - 2-bit value = 1
        msgbits = [1, ]
        res = bits2value(msgbits)
        self.assertEqual(res, 1)
        res = ctth_procflags2pps(np.array([1], 'int32'))
        ppsbits = [1, ]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: processed because cloudy, but without result - 2-bit value =
        # 2
        res = ctth_procflags2pps(np.array([2], 'int32'))
        ppsbits = [1, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: processed because cloudy, with result - 2-bit value =
        # 3
        res = ctth_procflags2pps(np.array([3], 'int32'))
        ppsbits = [0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        #
        # 1 bit to define if RTTOV-simulations are available?
        # (maps to pps bit 3:)
        msgbits = [0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 4)
        # PPS bit 3 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([4], 'int32'))
        ppsbits = [1, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        #
        # 3 bits to describe NWP input data (maps to pps bits 4&5:)

        # Example: All NWP parameters available, no thermal inversion - 3-bit
        # value = 1
        msgbits = [0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 8)
        # No PPS bits should be set (only pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([8], 'int32'))
        ppsbits = [1, ]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: All NWP parameters available, thermal inversion present - 3-bit
        # value = 2
        msgbits = [0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 16)
        # PPS bit 5 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([16], 'int32'))
        ppsbits = [1, 0, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: Some NWP pressure levels missing, no thermal inversion - 3-bit
        # value = 3
        msgbits = [0, 0, 0, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 24)
        # No PPS bits should be set (only pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([24], 'int32'))
        # ppsbits = [1, 0, 0, 0, 1] # This is how it has been up to now
        # (2014-10-24)
        ppsbits = [1, ]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: Some NWP pressure levels missing, thermal inversion present - 3-bit
        # value = 4
        msgbits = [0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 32)
        # PPS bit 5 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([32], 'int32'))
        # This is how it has been up to now (2014-10-24):
        # ppsbits = [1, 0, 0, 0, 1, 1]
        ppsbits = [1, 0, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: At least one mandatory NWP information is missing - 3-bit
        # value = 5
        msgbits = [0, 0, 0, 1, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 40)
        # PPS bit 4 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([40], 'int32'))
        ppsbits = [1, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        #
        # 2 bits to describe SEVIRI input data (maps to pps bits 6:)

        # Example: 2-bit value = 2/3: at least one SEVIRI useful channel
        # missing (value=2) or at least one SEVIRI mandatory channel is missing
        # (value 0 3):
        msgbits = [0, 0, 0, 0, 0, 0, 1, 1]  # value=3
        res = bits2value(msgbits)
        self.assertEqual(res, 192)
        # PPS bit 6 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([192], 'int32'))
        ppsbits = [1, 0, 0, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))
        msgbits = [0, 0, 0, 0, 0, 0, 0, 1]  # value=2
        res = bits2value(msgbits)
        self.assertEqual(res, 128)
        # PPS bit 6 should be set (and pps bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([128], 'int32'))
        ppsbits = [1, 0, 0, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # 4 bits to describe which method has been used (maps to pps bits 7&8
        # and bit 2):

        # Example: Opaque cloud, using rttov - 4-bit value = 1
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 1]  # value=1
        res = bits2value(msgbits)
        self.assertEqual(res, 256)
        # PPS bits 0, 2, 7 and 9 (spare) should be set:
        res = ctth_procflags2pps(np.array([256], 'int32'))
        ppsbits = [1, 0, 1, 1, 0, 0, 0, 1, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: Opaque cloud, using RTTOV, in case thermal inversion - 4-bit
        # value=13
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 3328)
        # PPS bits 9, 11 and 12 (direct mapping) + PPS bits 2 (opaque cloud)
        # and 3 (rttov available) and 5 (thermal inversion) and 7 (rttov
        # simulations applied):
        res = ctth_procflags2pps(np.array([3328], 'int32'))
        ppsbits = [1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        #
        # 2 bits to describe the quality of the processing itself
        # Example: Good quality (high confidence) - 2-bit value = 1
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 4096)
        # PPS bit 14 is set (and bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([4096], 'int32'))
        ppsbits = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

        # Example: Poor quality (low confidence) - 2-bit value = 2
        msgbits = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        res = bits2value(msgbits)
        self.assertEqual(res, 8192)
        # PPS bit 14 is set (and bit 0 for non-processed)
        res = ctth_procflags2pps(np.array([8192], 'int32'))
        ppsbits = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1]
        self.assertEqual(ppsbits, value2bits(res[0]))

    def tearDown(self):
        """Clean up"""
        return


def suite():
    """The suite for test_pps_msg_converter.
    """
    loader = unittest.TestLoader()
    mysuite = unittest.TestSuite()
    mysuite.addTest(loader.loadTestsFromTestCase(TestFlagConversions))

    return mysuite
