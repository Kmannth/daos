#!/usr/bin/python3
"""
  (C) Copyright 2018-2021 Intel Corporation.

  SPDX-License-Identifier: BSD-2-Clause-Patent
"""
from performance_test_base import PerformanceTestBase

class IorHard(PerformanceTestBase):
    # pylint: disable=too-many-ancestors
    # pylint: disable=too-few-public-methods
    """Test class Description: Run IOR Hard

    Use Cases:
            Create a pool, container, and run IOR Hard.

    :avocado: recursive
    """

    def test_performance_ior_hard_dfs_sx(self):
        """Test Description: Run IOR Hard with DFS and SX.

        :avocado: tags=all,full_regression
        :avocado: tags=hw,large
        :avocado: tags=performance,ior,dfs
        :avocado: tags=performance_ior_hard,performance_ior_hard_dfs_sx
        """
        self.run_performance_ior(namespace="/run/ior_dfs_sx/*")

    def test_performance_ior_hard_dfs_ec_16p2gx(self):
        """Test Description: Run IOR Hard with DFS and EC_16P2GX.

        :avocado: tags=all,manual
        :avocado: tags=hw,large
        :avocado: tags=performance
        :avocado: tags=performance_ior_hard,performance_ior_hard_dfs_16p2gx
        """
        self.run_performance_ior(namespace="/run/ior_dfs_ec_16p2gx/*")

    def test_performance_ior_hard_dfuse_sx(self):
        """Test Description: Run IOR Hard with dfuse and SX.

        :avocado: tags=all,full_regression
        :avocado: tags=hw,large
        :avocado: tags=performance,ior,dfuse
        :avocado: tags=performance_ior_hard,performance_ior_hard_dfuse_sx
        """
        self.run_performance_ior(namespace="/run/ior_dfuse_sx/*")

    def test_performance_ior_hard_dfuse_ec_16p2gx(self):
        """Test Description: Run IOR Hard with dfuse and EC_16P2GX.

        :avocado: tags=all,manual
        :avocado: tags=hw,large
        :avocado: tags=performance
        :avocado: tags=performance_ior_hard,performance_ior_hard_dfuse_16p2gx
        """
        self.run_performance_ior(namespace="/run/ior_dfuse_ec_16p2gx/*")
