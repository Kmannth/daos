#!/usr/bin/python3
"""
  (C) Copyright 2018-2021 Intel Corporation.

  SPDX-License-Identifier: BSD-2-Clause-Patent
"""
from performance_test_base import PerformanceTestBase

class IorEasy(PerformanceTestBase):
    # pylint: disable=too-many-ancestors
    # pylint: disable=too-few-public-methods
    """Test class Description: Run IOR Easy

    Use Cases:
            Create a pool, container, and run IOR Easy.

    :avocado: recursive
    """

    def test_performance_ior_easy_dfs_sx(self):
        """Test Description: Run IOR Easy with DFS and SX.

        :avocado: tags=all,full_regression
        :avocado: tags=hw,large
        :avocado: tags=performance,ior,dfs
        :avocado: tags=performance_ior_easy,performance_ior_easy_dfs_sx
        """
        self.run_performance_ior(namespace="/run/ior_dfs_sx/*")

    def test_performance_ior_easy_dfs_ec_16p2gx(self):
        """Test Description: Run IOR Easy with DFS and EC_16P2GX.

        :avocado: tags=all,manual
        :avocado: tags=hw,large
        :avocado: tags=performance
        :avocado: tags=performance_ior_easy,performance_ior_easy_dfs_16p2gx
        """
        self.run_performance_ior(namespace="/run/ior_dfs_ec_16p2gx/*")

    def test_performance_ior_easy_dfuse_sx(self):
        """Test Description: Run IOR Easy with dfuse and SX.

        :avocado: tags=all,full_regression
        :avocado: tags=hw,large
        :avocado: tags=performance,ior,dfuse
        :avocado: tags=performance_ior_easy,performance_ior_easy_dfuse_sx
        """
        self.run_performance_ior(namespace="/run/ior_dfuse_sx/*")

    def test_performance_ior_easy_dfuse_ec_16p2gx(self):
        """Test Description: Run IOR Easy with dfuse and EC_16P2GX.

        :avocado: tags=all,manual
        :avocado: tags=hw,large
        :avocado: tags=performance
        :avocado: tags=performance_ior_easy,performance_ior_easy_dfuse_16p2gx
        """
        self.run_performance_ior(namespace="/run/ior_dfuse_ec_16p2gx/*")

    def test_performance_ior_easy_dfs_rp_2gx_fault(self):
        """Test Description: Run IOR Easy with DFS and SX and inject a failure.

        :avocado: tags=all,full_regression
        :avocado: tags=hw,large
        :avocado: tags=performance,ior,dfs
        :avocado: tags=performance_ior_easy,performance_ior_easy_dfs_rp_2gx_fault
        """
        self.run_performance_ior(namespace="/run/ior_dfs_rp_2gx/*", kill_delay_write=0.5)
